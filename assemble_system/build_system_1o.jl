using PowerSystems
using PowerSimulations
using HydroPowerSimulations
using PowerSystemCaseBuilder
using HiGHS # solver
using Dates

include("parse-matpower_1o.jl");

using CSV
using DataFrames

# parse matpower into data tables.
dataTables = Dict()
# MATPOWER_FILE = "data/CaliforniaTestSystem.m" 
MATPOWER_FILE = "MATPOWER/system_condensers_removed_cutoff75.m" 
for s in ["bus", "gen", "branch"]
    searchKey = s
    if s == "gen"
        searchKey = "generator"
    end
    dataTables[s] = SystemDataTable(MATPOWER_FILE, searchKey)
end
COST_KEYS = ["model", "startup", "shutdown", "n", "c2", "c1", "c0"] # modified by Hongfei 
dataTables["real power cost"] = SystemDataTable(MATPOWER_FILE, "cost", COST_KEYS)

# writeMatpower("data/CaliforniaTestSystem_1o.m", dataTables)
writeMatpower("MATPOWER/system_condensers_removed_cutoff75_1o.m", dataTables)

# system = System("data/CaliforniaTestSystem_1o.m") 
system = System("MATPOWER/system_condensers_removed_cutoff75_1o.m") 
include("../build-system/replace_gens.jl") 
include("../build-system/define_time_series.jl") 

system 
