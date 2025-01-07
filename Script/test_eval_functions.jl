function map_buses_to_loads(NetworkData)
    load_mapping = Dict()
    for (i,l) in NetworkData["load"]
        load_bus = l["load_bus"]
        load_idx = l["index"]
        merge!(load_mapping,Dict(load_bus => load_idx))
    end
    return load_mapping
end

function update_loads!(k,load_scenarios,load_mapping, NetworkData)
    for i in 1:size(load_scenarios)[1]
        load_split = split(load_scenarios[i,k],"+")
        P = parse(Float64,load_split[1])
        Q = parse(Float64,split(load_split[2],"i")[1])
        if haskey(load_mapping,i)
            NetworkData["load"][string(load_mapping[i])]["pd"] = P/(NetworkData["baseMVA"])
            NetworkData["load"][string(load_mapping[i])]["qd"] = Q/(NetworkData["baseMVA"])
        end
    end
    return
end

function update_rgen!(k,NetworkData,gen_data,SolarGeneration,WindGeneration,PMaxOG,SolarCap,WindCap)
    for j in 1:size(gen_data)[1]
        if occursin("solar",lowercase(gen_data[j,"FuelType"]))
            NetworkData["gen"][string(j)]["pmax"] = SolarGeneration[k]/NetworkData["baseMVA"] * PMaxOG[j]/SolarCap
        elseif occursin("wind",lowercase(gen_data[j,"FuelType"]))
            NetworkData["gen"][string(j)]["pmax"] = WindGeneration[k]/NetworkData["baseMVA"] * PMaxOG[j]/WindCap
        end
    end
end

function update_lower_bound_voltages!(NetworkData, condenserIndices)
    for i in condenserIndices
        NetworkData["bus"][string(i)]["vmin"] = 1.0
    end
    return
end

function penalize_reactive_power!(pm, condenserIndices)
    pm.var[:it][:pm][:nw][0][:z] = JuMP.@variable(pm.model, z[i in condenserIndices], lower_bound=0, upper_bound = 2.01)
    pm.con[:it][:pm][:nw][0][:z_ub] = JuMP.@constraint(pm.model, [i in condenserIndices], pm.var[:it][:pm][:nw][0][:qg][i] <= pm.var[:it][:pm][:nw][0][:z][i])
    pm.con[:it][:pm][:nw][0][:z_lb] = JuMP.@constraint(pm.model, [i in condenserIndices], -pm.var[:it][:pm][:nw][0][:qg][i] <= pm.var[:it][:pm][:nw][0][:z][i])
    for i in condenserIndices
        pm.var[:it][:pm][:nw][0][:pg_cost][i] = 5000.0*pm.var[:it][:pm][:nw][0][:z][i]
    end

    JuMP.@objective(pm.model, Min,
            sum(
                sum( var(pm, n,   :pg_cost, i) for (i,gen) in nw_ref[:gen]) +
                sum( var(pm, n, :p_dc_cost, i) for (i,dcline) in nw_ref[:dcline])
            for (n, nw_ref) in nws(pm))
        )
    return
end

#=
function export_JSON(solution, k,path)
    stringdata = JSON.json(solution)

    # write the file with the stringdata variable information
    open("$(path)/solution_$(k).json", "w") do f
        write(f, stringdata)
    end
end
=#
