using Distributed
addprocs(30)
@everywhere begin
    global CATS_DIR = "/scratch/jlara/CATS-CaliforniaTestSystem"
    global DATA_DIR = "/scratch/jlara/CATS-CaliforniaTestSystem"
    using Pkg
    Pkg.activate(CATS_DIR)
    #Pkg.develop(path = "/home/jlara/HSL_jll.jl-2023.11.7")
    # using HSL_jll
    using PowerModels
    using JuMP
    using CSV, JSON
    using DataFrames
    using Ipopt
    using Tables

    include("$CATS_DIR/Script/test_eval_functions.jl")

    function eval(range, NetworkData_input, load_scenarios, load_mapping, HourlyData2019, gen_data)
        solver = JuMP.optimizer_with_attributes(() -> Ipopt.Optimizer(),
            "print_level" => 3,
            # "hsllib" => HSL_jll.libhsl_path,
            # "linear_solver" => "ma27"
        )

        NetworkData = deepcopy(NetworkData_input)
        N = 8760
        load_scenarios = load_scenarios[:,1:N]
        condenserIndices = [g for g in 1:size(gen_data)[1] if occursin("condenser", lowercase(gen_data.FuelType[g]))]
        NUM_GENS = size(gen_data)[1]

        SolarGenIndex = [g for g in 1:NUM_GENS if occursin("solar", lowercase(gen_data.FuelType[g]))]
        WindGenIndex= [g for g in 1:NUM_GENS if occursin("wind", lowercase(gen_data.FuelType[g]))]

        SolarCap = sum(g["pmax"] for (i,g) in NetworkData["gen"] if g["index"] in SolarGenIndex)
        WindCap = sum(g["pmax"] for (i,g) in NetworkData["gen"] if g["index"] in WindGenIndex)

        SolarGeneration = HourlyData2019[1:N,"Solar"]
        WindGeneration = HourlyData2019[1:N,"Wind"]
        PMaxOG = [NetworkData["gen"][string(i)]["pmax"] for i in 1:NUM_GENS]
        for k in range
            # Change renewable generators' pg for the current scenario
            update_rgen!(k,NetworkData,gen_data,SolarGeneration,WindGeneration,PMaxOG,SolarCap,WindCap)
            #println(sum(NetworkData["gen"][string(i)]["pmax"] for i in 1:size(gen_data)[1]))

            # Change load buses' Pd and Qd for the current scenario
            update_loads!(k, load_scenarios, load_mapping, NetworkData)

            # Run power flow
            pm = instantiate_model(NetworkData, ACPPowerModel, PowerModels.build_opf)
            penalize_reactive_power!(pm, condenserIndices)
            @info "Solving case $k in range $range"
            solution = optimize_model!(pm, optimizer=solver)

            #Save solution dictionary to JSON
            # push!(results,  solution["termination_status"])
            #nextRow = solution["solution"]["gen"][FIRST_CONDENSER:NUM_GENS]
            if solution["termination_status"] == LOCALLY_INFEASIBLE
                @error "$k in range $range infeasible skipping write"
            else
                tmp_condenserReactiveFlows = Tables.table([k, [round(solution["solution"]["gen"]["$x"]["qg"], digits = 4) for x in condenserIndices]...]')
                CSV.write("SimplifiedcondenserReactiveFlows.csv", tmp_condenserReactiveFlows; append=true)
                @info "Wrote case $k in range $range"
            end
        end
    end
end

split_N = 120
splits = [range(N, N + split_N - 1) for N in 1:split_N:8760]

load_scenarios = CSV.read("$DATA_DIR/Load_Agg_Post_Assignment_v3_latest.csv",header = false, DataFrame)
HourlyData2019 = CSV.read("$DATA_DIR/HourlyProduction2019.csv",DataFrame)
gen_data = CSV.read("$CATS_DIR/GIS/SimplifiedCATS_gens.csv",DataFrame)
condenserIndices = [g for g in 1:size(gen_data)[1] if occursin("condenser", lowercase(gen_data.FuelType[g]))]

condenserReactiveFlows = DataFrame("timestep" => Int[],
("gen $x" => Float64[] for x in condenserIndices)...)
CSV.write("condenserReactiveFlows.csv", condenserReactiveFlows)

NetworkData = PowerModels.parse_file("$CATS_DIR/MATPOWER/SimplifiedCaliforniaTestSystem.m")
load_mapping = map_buses_to_loads(NetworkData)

@info "finished reading the data"
pmap(x-> eval(x, NetworkData, load_scenarios, load_mapping, HourlyData2019, gen_data), splits)
