using PowerSystems
using PowerSimulations
using HydroPowerSimulations
using PowerSystemCaseBuilder
using HiGHS # solver
using Dates
using JuMP

mip_gap = 0.001

sys_path = "assemble_system/CATS_1o.json" 
system = System(sys_path) 

transform_single_time_series!(
           system,
           Dates.Hour(12), # horizon 
           Dates.Hour(6), # interval 
       );


template_uc = template_unit_commitment(;
    network = NetworkModel(PTDFPowerModel; use_slacks = true),
)

# set_device_model!(template_uc, Line, StaticBranch) 
set_device_model!(template_uc, Line, StaticBranchBounds) 
# set_device_model!(template_uc, Transformer2W, StaticBranch) 
# set_device_model!(template_uc, TapTransformer, StaticBranch)
# set_device_model!(template_uc, ThermalStandard, ThermalStandardUnitCommitment) 
# set_device_model!(template_uc, RenewableDispatch, RenewableFullDispatch) 
# set_device_model!(template_uc, PowerLoad, StaticPowerLoad) 
# set_device_model!(template_uc, HydroDispatch, HydroDispatchRunOfRiver) 
# set_device_model!(template_uc, RenewableNonDispatch, FixedOutput) 
# set_service_model!(template_uc, VariableReserve{ReserveUp}, RangeReserve)
# set_service_model!(template_uc, VariableReserve{ReserveDown}, RangeReserve)
# set_network_model!(template_uc, NetworkModel(CopperPlatePowerModel)) 

template_ed = template_economic_dispatch(;
    network = NetworkModel(CopperPlatePowerModel; use_slacks = true),
)


solver = optimizer_with_attributes(HiGHS.Optimizer, "mip_rel_gap" => mip_gap) #, "presolve" => "off" ) 


problem_uc = DecisionModel(
    template_uc, 
    system; 
    optimizer = solver, 
    optimizer_solve_log_print = true, 
    calculate_conflict = true, 
    # warm_start=false, 
    # horizon = Hour(24),
    name = "UC"
)

# problem_ed = DecisionModel(
#     template_ed, 
#     system; 
#     optimizer = solver, 
#     optimizer_solve_log_print = true, 
#     calculate_conflict = true, 
#     # warm_start=false, 
#     # horizon = Hour(24), 
#     name = "ED"
# )


# models = SimulationModels(;
#     decision_models = [
#         problem_uc, 
#         problem_ed, 
#     ],
# )


# feedforward = Dict(
#     "ED" => [
#         SemiContinuousFeedforward(;
#             component_type = ThermalStandard,
#             source = OnVariable,
#             affected_values = [ActivePowerVariable],
#         ),
#     ],
# )


# DA_RT_sequence = SimulationSequence(;
#     models = models,
#     ini_cond_chronology = InterProblemChronology(),
#     feedforwards = feedforward,
# )


# sim = Simulation(;
#     name = "UCED-test",
#     steps = 2,
#     models = models,
#     sequence = DA_RT_sequence,
#     simulation_folder = "rts_store",
# )


build!(problem_uc; output_dir = mktempdir())
# build!(sim)

solve!(problem_uc)
# execute!(sim; enable_progress_bar = false)

res = OptimizationProblemResults(problem)
get_optimizer_stats(res)
variables = read_variables(res)
# list_parameter_names(res)
# read_parameter(res, "ActivePowerTimeSeriesParameter__RenewableDispatch")

# println()
# results = SimulationResults(sim);
# uc_results = get_decision_problem_results(results, "UC")
# ed_results = get_decision_problem_results(results, "ED")

