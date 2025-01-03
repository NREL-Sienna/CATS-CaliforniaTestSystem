using Pkg
Pkg.activate(".")
Pkg.develop(path = "/Users/jlara/Documents/HSL_jll.jl-2023.11.7")
using HSL_jll

using PowerModels
using JuMP
using CSV, JSON
using DataFrames
using Ipopt
using Tables

CATS_DIR = "."
DATA_DIR = "."
save_to_JSON = false
# run for N hours (scenarios)
# N = 8760
N = 4

# Specify solver
#const GUROBI_ENV = Gurobi.Env()
# solver = JuMP.optimizer_with_attributes(() -> Gurobi.Optimizer(GUROBI_ENV), "OutputFlag" => 1)

# const IPOPT_ENV = Ipopt.Env()
solver = JuMP.optimizer_with_attributes(() -> Ipopt.Optimizer(),
    "print_level" => 3,
    "hsllib" => HSL_jll.libhsl_path,
    "linear_solver" => "ma57"
)
# 159

include("$CATS_DIR/Script/test_eval_functions.jl")
load_scenarios = CSV.read("$DATA_DIR/Load_Agg_Post_Assignment_v3_latest.csv",header = false, DataFrame)
load_scenarios = load_scenarios[:,1:N]

NetworkData = PowerModels.parse_file("$CATS_DIR/MATPOWER/SimplifiedCaliforniaTestSystem.m")

gen_data = CSV.read("$CATS_DIR/GIS/SimplifiedCATS_gens.csv",DataFrame)
NUM_GENS = size(gen_data)[1]

PMaxOG = [NetworkData["gen"][string(i)]["pmax"] for i in 1:NUM_GENS]
println(sum(PMaxOG))

SolarGenIndex = [g for g in 1:NUM_GENS if occursin("solar", lowercase(gen_data.FuelType[g]))]
WindGenIndex= [g for g in 1:NUM_GENS if occursin("wind", lowercase(gen_data.FuelType[g]))]
condenserIndices = [g for g in 1:size(gen_data)[1] if occursin("condenser", lowercase(gen_data.FuelType[g]))]
condenserReactiveFlows = DataFrame("timestep" => Int[],
                        ("gen $x" => Float64[] for x in condenserIndices)...)

condenserReactiveFlows = DataFrame("timestep" => Int[],
("gen $x" => Float64[] for x in condenserIndices)...)
CSV.write("condenserReactiveFlows.csv", condenserReactiveFlows)

SolarCap = sum(g["pmax"] for (i,g) in NetworkData["gen"] if g["index"] in SolarGenIndex)
WindCap = sum(g["pmax"] for (i,g) in NetworkData["gen"] if g["index"] in WindGenIndex)

load_mapping = map_buses_to_loads(NetworkData)

HourlyData2019 = CSV.read("$DATA_DIR/HourlyProduction2019.csv",DataFrame)
SolarGeneration = HourlyData2019[1:N,"Solar"]
WindGeneration = HourlyData2019[1:N,"Wind"]


# Create dataframe to store results
results = []

@time begin
    # Threads.@threads
    for k in 1:N
        #println("k = $k on thread $(Threads.threadid())")
        println(k)
        # Change renewable generators' pg for the current scenario
        update_rgen!(k,NetworkData,gen_data,SolarGeneration,WindGeneration,PMaxOG,SolarCap,WindCap)
        #println(sum(NetworkData["gen"][string(i)]["pmax"] for i in 1:size(gen_data)[1]))

        # Change load buses' Pd and Qd for the current scenario
        update_loads!(k, load_scenarios, load_mapping, NetworkData)

        # Run power flow
        pm = instantiate_model(NetworkData, ACPPowerModel, PowerModels.build_opf)
        penalize_reactive_power!(pm, condenserIndices)
        solution = optimize_model!(pm, optimizer=solver)

        #push!(results, (renewable_scenarios[!,1][k], solution["termination_status"]))
        #Save solution dictionary to JSON
        if save_to_JSON == true
           export_JSON(solution, k, "solutions")
        end
        # push!(results,  solution["termination_status"])
        #nextRow = solution["solution"]["gen"][FIRST_CONDENSER:NUM_GENS]
        if solution["termination_status"] == LOCALLY_INFEASIBLE
            @error "$k Infeasible skipping write"
            tmp_condenserReactiveFlows = Tables.table([k, [-99 for x in condenserIndices]...]')
            CSV.write("condenserReactiveFlows.csv", tmp_condenserReactiveFlows; append=true)
        else
            tmp_condenserReactiveFlows = Tables.table([k, [round(solution["solution"]["gen"]["$x"]["qg"], digits = 4) for x in condenserIndices]...]')
            CSV.write("condenserReactiveFlows.csv", tmp_condenserReactiveFlows; append=true)
        end
    end
end
#=
open("termination_status_ACOPF_corrected_cc.txt", "w") do file
    for i in 1:length(results)
        if results[i] == LOCALLY_SOLVED
            write(file, "Locally Solved\n")
        elseif results[i] == LOCALLY_INFEASIBLE
            write(file,"Locally Infeasible\n")
        elseif results[i] == ALMOST_LOCALLY_SOLVED
            write(file, " Solved to an acceptable level\n")
        else
            println(i)
        end
    end
end
=#
