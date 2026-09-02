# =============================================================================
# CATS — Security-Constrained Unit Commitment (N-1), single model instance
#
# A unit-commitment problem on the synthetic California Test System (CATS,
# ~8,870 buses) where the dispatch must respect line flows BOTH in the base case
# and under N-1 contingencies. The security behaviour comes entirely from the
# `Line` formulation `SecurityConstrainedStaticBranch` (base-case capacity +
# post-contingency capacity) backed by a MODF (multiple-outage distribution
# factors) that PowerOperationsModels derives from the PTDF's factorization core.
#
# This solves ONE `DecisionModel` instance. The psy5 original ran a multi-step
# rolling-horizon `Simulation`; see Archive/psi-simulation/cats_simulation.jl.
#
# Network reductions: we apply ALL PowerNetworkMatrices reductions
# (RadialReduction + DegreeTwoReduction). Radial feeders and degree-two
# pass-through buses are stripped, leaving the meshed EHV/HV backbone.
#
# Flow gates (N-1): the contingency sources are the NON-RADIAL 500 kV lines
# (lines that survive the reduction — a radial 500 kV stub is not a meaningful
# transmission contingency). Each contingency monitors every NON-RADIAL 230 kV
# line. We only ever pick lines that survive the reduction: declaring a reduced
# (radial) line as monitored/outaged would force its buses back into the network
# and silently undo the reduction. We request duals on both the base-case
# (`FlowRateConstraint`) and post-contingency (`PostContingencyFlowRateConstraint`)
# capacity limits and report which monitored lines bind.
#
# Power-flow evaluation: a `DCPowerFlow()` evaluator runs on the solved dispatch
# (lossless DC check of the PTDF flows).
#
# Solver: HiGHS, configured to use the parallel MIP/simplex solver
# (`parallel="on"`) and the HiPO interior-point solver (`solver="hipo"`) —
# confirmed working on HiGHS 1.15.1. Tune via CATS_SOLVER_PARALLEL /
# CATS_SOLVER_ALGORITHM.
#
# Run from the repository root (Sienna/ holds this model's environment; the case lives in
# ../CATS_openapi, built by build/build_CATS.jl):
#   julia --project=Sienna Sienna/cats_model.jl
# Tune the demo size without editing the file via env vars CATS_N_GATES,
# CATS_N_MONITORED, CATS_PTDF_TOL (configuration block).
# =============================================================================

using PowerOperationsModels
using InfrastructureOptimizationModels
using PowerSystems
using PowerNetworkMatrices
using PowerFlows
using DataFrames
using CSV
using Dates
using Logging
using JuMP
using HiGHS

const POM = PowerOperationsModels
const IOM = InfrastructureOptimizationModels
const PSY = PowerSystems
const PNM = PowerNetworkMatrices
const PFS = PowerFlows

# Solver backend: "highs" (default) or "cuopt" (NVIDIA GPU solver). cuOpt.jl doesn't
# support MOI.ConstraintDual, so SUPPORTS_DUALS gates the dual request/analysis below.
const SOLVER_BACKEND = get(ENV, "CATS_SOLVER_BACKEND", "highs")   # "highs" | "cuopt"
const SUPPORTS_DUALS = SOLVER_BACKEND != "cuopt"

# -----------------------------------------------------------------------------
# 1. Configuration.
# -----------------------------------------------------------------------------
const CASE_DIR = joinpath(@__DIR__, "..", "CATS_openapi")
const HORIZON = Hour(2)             # length of this instance's horizon
# Still required even though nothing steps: `auto_transform_time_series!` converts the
# SingleTimeSeries to a Deterministic forecast only when BOTH horizon and interval are
# set, and the model cannot build without that forecast. It does not advance anything here.
const INTERVAL = Hour(1)
const START_HOUR = 17               # begin at 5 PM (evening peak), not off-peak midnight
const MIP_GAP = 0.01                # relative MILP gap
const SOLVER_TIME_LIMIT = 600.0     # s — HiGHS wall-clock cap
const SOLVER_THREADS = 18           # HiGHS threads
const SOLVER_PARALLEL = get(ENV, "CATS_SOLVER_PARALLEL", "on")       # "choose" | "on" | "off"
const SOLVER_ALGORITHM = get(ENV, "CATS_SOLVER_ALGORITHM", "hipo")   # "choose" | "simplex" | "hipo" | "ipm"

# Sparsification tolerance for the PTDF rows (absolute cutoff on a
# distribution-factor coefficient). Smaller = more accurate, denser rows.
# The MODF is no longer separately tunable: POM derives it from this PTDF's
# factorization core, so it inherits this tolerance and this reduction.
const PTDF_TOL = parse(Float64, get(ENV, "CATS_PTDF_TOL", "0.01"))

# Voltage thresholds (kV).
const MODELED_KV = 230.0            # apply security constraints to lines with from-bus >= this
const CONTINGENCY_KV = 500.0        # N-1 contingency sources: non-radial lines at this level
const MONITOR_KV = 230.0            # monitored under contingency: non-radial lines at this level
# Problem-size caps. At tol=0.01 a 500 kV contingency's PTDF row is nearly global
# (~2,500 nonzeros on the 5,346-bus reduced network), so each post-contingency
# constraint is dense. The matrix nonzero count scales with N_FLOW_GATES *
# N_MONITORED, and the full non-radial sets (83 x 612) would need ~10 billion
# nonzeros — far beyond 64 GB. We therefore cap BOTH: take the highest-rated
# non-radial 500 kV lines as contingencies and the highest-rated non-radial 230 kV
# lines as the monitored set. Raise these (memory/time permitting) toward the full
# counts; the selection logic is unchanged, only truncated.
const N_FLOW_GATES = parse(Int, get(ENV, "CATS_N_GATES", "5"))      # # of 500 kV contingencies
const N_MONITORED = parse(Int, get(ENV, "CATS_N_MONITORED", "50"))  # # of 230 kV monitored lines
const DUAL_BINDING_TOL = 1e-4       # |dual| above this counts as a binding limit

const RESULTS_ROOT = joinpath(@__DIR__, "results")
const CSV_DIR = joinpath(@__DIR__, "csv_results")
mkpath(RESULTS_ROOT)
mkpath(CSV_DIR)

# -----------------------------------------------------------------------------
# 2. Load the system.
#
#    The OpenAPI bundle is the only psy6-readable form of this case; the legacy
#    CATS_Sienna.json at the repo root is a psy5-era serialization. The single
#    time series is NOT transformed here — `DecisionModel` calls
#    `auto_transform_time_series!` from the horizon/resolution settings below.
# -----------------------------------------------------------------------------
sys = PSY.from_file(PSY.System, CASE_DIR)

# Start at the first timestamp landing on START_HOUR (5 PM). Read the timestamps from
# the SingleTimeSeries itself, not from `get_forecast_initial_times`: no forecast exists
# yet, because the conversion happens inside `DecisionModel`. `Dates.hour` is qualified
# because TimeSeries and TimeZones also export `hour`.
function pick_initial_time(system, target_hour)
    owner = first(PSY.get_components(PSY.has_time_series, PSY.PowerLoad, system))
    stamps =
        PSY.get_time_series_timestamps(PSY.SingleTimeSeries, owner, "max_active_power")
    idx = findfirst(t -> Dates.hour(t) == target_hour, stamps)
    if isnothing(idx)
        return error(
            "No timestamp at hour $(target_hour); " *
            "available hours: $(sort(unique(Dates.hour.(stamps))))",
        )
    end
    return stamps[idx]
end

const INITIAL_DATE = pick_initial_time(sys, START_HOUR)

# -----------------------------------------------------------------------------
# 3. Network reductions + PTDF.
#
# Build the PTDF on the FULLY reduced network (radial + degree-two). The same
# reduced matrix is handed to the network model as a PrebuiltMatrixSource, so the
# model reuses this exact matrix — populated row cache included — and derives the
# MODF from its core. Building it here (rather than letting the model build it) is
# required: the flow-gate selection below needs `get_arc_axis` to know which arcs
# survive the reduction before the model exists.
# -----------------------------------------------------------------------------
const REDUCTIONS = [PNM.RadialReduction(), PNM.DegreeTwoReduction()]

ptdf = PNM.VirtualPTDF(sys; tol = PTDF_TOL, network_reductions = REDUCTIONS)

# Arcs that survive every reduction. A Line is "retained" (non-radial and not
# collapsed by the degree-two reduction) iff its arc appears here. Radial lines
# are exactly the ones missing from this set.
const SURVIVING_ARCS = Set(PNM.get_arc_axis(ptdf))

function line_arc(l)
    arc = PSY.get_arc(l)
    return (PSY.get_number(PSY.get_from(arc)), PSY.get_number(PSY.get_to(arc)))
end

function is_retained(l)
    (fb, tb) = line_arc(l)
    return (fb, tb) in SURVIVING_ARCS || (tb, fb) in SURVIVING_ARCS
end

# Base voltage is always kV — not a convertible per-unit field — so its getter
# takes no unit system, unlike `get_rating` below.
function from_bus_kv(l)
    return PSY.get_base_voltage(PSY.get_from(PSY.get_arc(l)))
end

# A line sits "at" a voltage level if its from-bus base voltage matches (5% tol).
function at_voltage(l, kv)
    return isapprox(from_bus_kv(l), kv; rtol = 0.05)
end

# -----------------------------------------------------------------------------
# 4. Flow-gate selection.
#
#    First classify which lines are non-radial (retained by the reduction), THEN
#    build the gates from that set only — never the other way around. Both sets
#    are truncated to the highest-rated lines (see the N_FLOW_GATES / N_MONITORED
#    caps above) to keep the security matrix within memory.
#      * contingencies = highest-rated NON-RADIAL 500 kV lines -> FixedForcedOutage
#      * monitored     = highest-rated NON-RADIAL 230 kV lines -> watched post-N-1
# -----------------------------------------------------------------------------
available_lines = [l for l in PSY.get_components(PSY.Line, sys) if PSY.get_available(l)]

contingency_candidates =
    [l for l in available_lines if at_voltage(l, CONTINGENCY_KV) && is_retained(l)]
monitored_candidates =
    [l for l in available_lines if at_voltage(l, MONITOR_KV) && is_retained(l)]

function by_descending_rating(l)
    return -PSY.get_rating(l, PSY.SU)
end

gate_lines = first(sort(contingency_candidates; by = by_descending_rating), N_FLOW_GATES)
monitored = first(sort(monitored_candidates; by = by_descending_rating), N_MONITORED)

@info "Flow gates (N-1)" n_contingencies = length(gate_lines) n_monitored =
    length(monitored) n_nonradial_500kV = length(contingency_candidates) n_nonradial_230kV =
    length(monitored_candidates) n_500kV_total =
    count(l -> at_voltage(l, CONTINGENCY_KV), available_lines) n_230kV_total =
    count(l -> at_voltage(l, MONITOR_KV), available_lines)
isempty(gate_lines) && error(
    "No non-radial $(CONTINGENCY_KV) kV lines found — check voltage levels / reduction.",
)
isempty(monitored) &&
    error("No non-radial $(MONITOR_KV) kV lines found to monitor.")

# -----------------------------------------------------------------------------
# 5. Attach one N-1 outage per gate line.
#
#    `outage_status = 1.0` -> the line is taken out for this contingency.
#    `monitored_components` -> the non-radial 230 kV lines watched under it.
#    POM registers these outages on the MODF it derives during
#    `instantiate_network_model!`; because every one of those lines is already
#    non-radial, the reduction is preserved.
# -----------------------------------------------------------------------------
# Map each outage's UUID back to its contingency line name. Every post-contingency
# dual series is labelled "<outage-uuid>__<monitored-line>", so this lets the
# export report the readable contingency line instead of a raw UUID.
outage_to_gate = Dict{String, String}()
for l in gate_lines
    outage =
        PSY.FixedForcedOutage(; outage_status = 1.0, monitored_components = monitored)
    PSY.add_supplemental_attribute!(sys, l, outage)
    outage_to_gate[string(IOM.IS.get_id(outage))] = PSY.get_name(l)
end

# -----------------------------------------------------------------------------
# 6. Network model: the prebuilt PTDF as the network source, with a DC
#    power-flow evaluator. The MODF is derived from the PTDF's factorization
#    core, so there is no separate MODF argument and no second reduction.
#    (If a prebuilt PTDF ever fails to yield a MODF, the fallback is
#    `PrebuiltCoreSource(core)` built from the same core.)
# -----------------------------------------------------------------------------
network_model = POM.NetworkModel(
    POM.PTDFNetworkModel;
    use_slacks = true,
    network_source = POM.PrebuiltMatrixSource(ptdf),
    evaluations = POM.power_flow_evaluations(PFS.DCPowerFlow()),
)

# -----------------------------------------------------------------------------
# 7. Template. The security behaviour is selected solely by the Line formulation
#    `SecurityConstrainedStaticBranch`. Constraints are applied only to lines at
#    or above MODELED_KV (the 230 kV + 500 kV backbone) via `filter_function`,
#    and duals are requested on the base-case + post-contingency capacity limits
#    (skipped for cuOpt, which doesn't support MOI.ConstraintDual).
# -----------------------------------------------------------------------------
function dual_request(supports_duals::Bool)
    if supports_duals
        return DataType[POM.FlowRateConstraint, POM.PostContingencyFlowRateConstraint]
    end
    return DataType[]
end

template = POM.PowerOperationsProblemTemplate(network_model)
POM.set_device_model!(template, PSY.ThermalStandard, POM.ThermalBasicUnitCommitment)
POM.set_device_model!(template, PSY.RenewableDispatch, POM.RenewableFullDispatch)
POM.set_device_model!(template, PSY.HydroDispatch, POM.HydroDispatchRunOfRiver)
POM.set_device_model!(template, PSY.PowerLoad, POM.StaticPowerLoad)
POM.set_device_model!(template, PSY.TwoWindingTransformer, POM.StaticBranchUnbounded)
POM.set_device_model!(
    template,
    POM.DeviceModel(
        PSY.Line,
        POM.SecurityConstrainedStaticBranch;
        use_slacks = true,
        duals = dual_request(SUPPORTS_DUALS),
        attributes = Dict(
            "filter_function" => x -> from_bus_kv(x) >= MODELED_KV,
        ),
    ),
)
POM.set_device_model!(
    template,
    POM.DeviceModel(
        PSY.EnergyReservoirStorage,
        POM.StorageDispatchWithReserves;
        attributes = Dict(
            "reservation" => false,
            "cycling_limits" => false,
            "energy_target" => false,
            "complete_coverage" => false,
            "regularization" => true,
        ),
    ),
)

# -----------------------------------------------------------------------------
# 8. Decision model -> build! -> solve!. Backend chosen via
#    CATS_SOLVER_BACKEND ("highs", 18 threads, or "cuopt", GPU).
# -----------------------------------------------------------------------------
function build_solver(backend::AbstractString)
    if backend == "highs"
        return JuMP.optimizer_with_attributes(
            HiGHS.Optimizer,
            "mip_rel_gap" => MIP_GAP,
            "time_limit" => SOLVER_TIME_LIMIT,
            "threads" => SOLVER_THREADS,
            "parallel" => SOLVER_PARALLEL,
            "solver" => SOLVER_ALGORITHM,
            # mip_lp_solver is distinct from `solver` — it picks the LP method used for
            # the root + node LP relaxations inside branch-and-bound, which is what
            # actually runs repeatedly for a MIP like this UC problem.
            "mip_lp_solver" => SOLVER_ALGORITHM,
            # HiGHS has no Gurobi-style MIPFocus. The intent of MIPFocus=1 (find feasible
            # integer incumbents quickly — the dual LP needs a feasible commitment) maps
            # to raising the heuristic effort (default 0.05).
            "mip_heuristic_effort" => 0.5,
        )
    end
    return error("Unknown CATS_SOLVER_BACKEND: $(backend). Expected \"highs\".")
end

solver = build_solver(SOLVER_BACKEND)

model = POM.DecisionModel(
    template,
    sys;
    optimizer = solver,
    initial_time = INITIAL_DATE,
    horizon = HORIZON,
    interval = INTERVAL,
    check_numerical_bounds = false,
    initialize_model = false,
    direct_mode_optimizer = true,
    optimizer_solve_log_print = get(ENV, "CATS_SOLVER_LOG", "0") == "1",
    calculate_conflict = false,
    name = "CATS_SCUC",
)

status = POM.build!(model; output_dir = RESULTS_ROOT, console_level = Logging.Info)
if status != IOM.ModelBuildStatus.BUILT
    error("build! returned $(status), expected ModelBuildStatus.BUILT")
end

POM.solve!(model)
outputs = IOM.OptimizationProblemOutputs(model)

# -----------------------------------------------------------------------------
# 9. Dual analysis + CSV export.
#
#    FlowRate / PostContingencyFlowRate are two-sided, so the store holds
#    `__lb` / `__ub` dual containers; we merge them by |dual| (the active side
#    is nonzero).
# -----------------------------------------------------------------------------
let stats = IOM.read_optimizer_stats(outputs)
    @info "Solver performance" backend = SOLVER_BACKEND solver = SOLVER_ALGORITHM parallel =
        SOLVER_PARALLEL threads = SOLVER_THREADS
    for r in eachrow(stats)
        gap = hasproperty(stats, :relative_gap) ? r.relative_gap : missing
        nodes = hasproperty(stats, :node_count) ? r.node_count : missing
        @info "  solve" solve_time = round(r.solve_time; sigdigits = 4) objective =
            round(r.objective_value; sigdigits = 6) relative_gap = gap node_count = nodes
    end
end

# Duals are stored in long form: columns [:DateTime, :name, :value], with
# separate `__lb` / `__ub` containers for the two-sided flow limits. `:name` is the
# line name for base-case duals and "<outage-uuid>__<monitored-line>" for
# post-contingency duals.
"""Read every stored dual whose name starts with `prefix`; return a DataFrame of
   (series, peak_abs_dual), taking the peak |dual| over time per series and merging
   the lb/ub variants (the active side carries the nonzero dual)."""
function peak_abs_duals(res, prefix)
    names = filter(n -> startswith(n, prefix), string.(IOM.list_dual_names(res)))
    peaks = Dict{String, Float64}()
    for nm in names
        df = IOM.read_dual(res, nm)
        for sub in groupby(df, :name)
            key = string(first(sub.name))
            p = maximum(abs.(Float64.(sub.value)); init = 0.0)
            peaks[key] = max(get(peaks, key, 0.0), p)
        end
    end
    out = DataFrame(; series = collect(keys(peaks)), peak_abs_dual = collect(values(peaks)))
    sort!(out, :peak_abs_dual; rev = true)
    return names, out
end

# POM keys post-contingency containers by the tuple (outage_id, monitored_name, t); the
# flattened `:name` written to the store joins the first two with "__". A label that does
# not carry the separator means that format changed — surface it rather than mapping it to
# a misleading contingency name.
function split_post(series)
    parts = split(series, "__"; limit = 2)
    if length(parts) != 2
        return "UNPARSED", series
    end
    outage_id, monitored_name = parts
    return get(outage_to_gate, outage_id, outage_id), monitored_name
end

if SUPPORTS_DUALS
    @info "Dual containers available" containers = string.(IOM.list_dual_names(outputs))

    # Base-case flow duals: one series per modeled line (>= MODELED_KV).
    b_names, b_df = peak_abs_duals(outputs, "FlowRateConstraint__Line")
    rename!(b_df, :series => :line)
    b_df.binding = b_df.peak_abs_dual .> DUAL_BINDING_TOL
    CSV.write(joinpath(CSV_DIR, "base_case_flow_duals.csv"), b_df)
    @info "Base-case FlowRateConstraint" sources = b_names n_lines = nrow(b_df) n_binding =
        count(b_df.binding)
    for r in eachrow(first(b_df[b_df.binding, :], 15))
        @info "  base  $(r.line) : peak |dual| = $(round(r.peak_abs_dual; sigdigits = 4)) \$/MWh"
    end

    # Post-contingency flow duals: split "<outage-uuid>__<monitored-line>" into the
    # readable (contingency line, monitored line) pair.
    p_names, p_df = peak_abs_duals(outputs, "PostContingencyFlowRateConstraint__Line")
    splits = split_post.(p_df.series)
    p_df.contingency_line = first.(splits)
    p_df.monitored_line = last.(splits)
    p_df.binding = p_df.peak_abs_dual .> DUAL_BINDING_TOL
    select!(p_df, :contingency_line, :monitored_line, :peak_abs_dual, :binding)
    sort!(p_df, :peak_abs_dual; rev = true)
    CSV.write(joinpath(CSV_DIR, "post_contingency_flow_duals.csv"), p_df)
    @info "Post-contingency FlowRateConstraint" sources = p_names n_series = nrow(p_df) n_binding =
        count(p_df.binding) n_never = count(.!p_df.binding)
    for r in eachrow(first(p_df[p_df.binding, :], 20))
        @info "  post  $(r.contingency_line)  ⟶  $(r.monitored_line) : peak |dual| = $(round(r.peak_abs_dual; sigdigits = 4)) \$/MWh"
    end
else
    @info "Skipping dual analysis — solver backend does not support MOI.ConstraintDual" backend =
        SOLVER_BACKEND
end

@info "Done." results = RESULTS_ROOT duals_csv = CSV_DIR
