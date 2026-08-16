"""Contains all the mappings for generator data parsing from MATPOWER format."""

const PM_TYPE_DICT = Dict{String, PSY.PrimeMovers}(
    "Conventional Hydroelectric" => PrimeMovers.HA,
    "Hydroelectric Pumped Storage" => PrimeMovers.HY,

    "Solar Photovoltaic" => PrimeMovers.PVe,
    "Solar Thermal without Energy Storage" => PrimeMovers.PVe,

    "Onshore Wind Turbine" => PrimeMovers.WT,

    "Batteries" => PrimeMovers.BA,

    "Municipal Solid Waste" => PrimeMovers.ST,
    "Other Waste Biomass" => PrimeMovers.ST,
    "Petroleum Liquids" => PrimeMovers.IC,
    "Geothermal" => PrimeMovers.ST,
    "Nuclear" => PrimeMovers.ST,
    "Wood/Wood Waste Biomass" => PrimeMovers.ST,
    "Conventional Steam Coal" => PrimeMovers.ST,
    "Petroleum Coke" => PrimeMovers.ST,
    "Natural Gas Fired Combustion Turbine" => PrimeMovers.GT,
    "Natural Gas Internal Combustion Engine" => PrimeMovers.IC,
    "Natural Gas Fired Combined Cycle" => PrimeMovers.CT,
    "Natural Gas Steam Turbine" => PrimeMovers.ST,
    "Other Natural Gas" => PrimeMovers.OT,

    # no prime mover type--handled as SynchronousCondenser and Source structs--but still
    # included here for code flow simplicity.
    "Synchronous Condenser" => PrimeMovers.OT,
    "IMPORT" => PrimeMovers.OT,

    "All Other" => PrimeMovers.OT,
    "Landfill Gas" => PrimeMovers.OT,
    "Other Gases" => PrimeMovers.OT
)

# Ramp fractions (of Pmax, per minute). The original numbered WECC table cited in
# DURATION_LIMIT_DICT below could not be located online; coal/CC/SC values here are
# corrected against RTS-GMLC's public per-unit gen.csv
# (github.com/GridMod/RTS-GMLC/blob/master/RTS_Data/SourceData/gen.csv) instead.
const RAMP_LIMIT_DICT = Dict(
    (PrimeMovers.ST, ThermalFuels.COAL) => (up = 0.0194, down = 0.0194), # RTS-GMLC STEAM/Coal (155 MW unit; 3 units range 1.14-2.63%/min)

    (PrimeMovers.CA, ThermalFuels.NATURAL_GAS) => (up = 0.0117, down = 0.0117), # RTS-GMLC CC/NG (355 MW units)
    (PrimeMovers.CT, ThermalFuels.NATURAL_GAS) => (up = 0.0673, down = 0.0673), # RTS-GMLC CT/NG (55 MW units)
    (PrimeMovers.GT, ThermalFuels.NATURAL_GAS) => (up = 0.0673, down = 0.0673), # RTS-GMLC has one simple-cycle-gas category (CT/NG); same source as above
    (PrimeMovers.ST, ThermalFuels.NATURAL_GAS) => (up = 0.0194, down = 0.0194), # no NG-fired steam unit in RTS-GMLC; approximated from the coal steam-turbine figure (boiler/turbine dynamics, not fuel, dominate steam-cycle ramp)

    # Not from WECC or RTS-GMLC: ballpark estimates (see DURATION_LIMIT_DICT). RTS-GMLC's
    # generic nuclear entry ramps at 5%/min -- ~500x faster than the value below -- but that
    # reflects a generic test-system assumption, not how the US nuclear fleet is actually
    # operated (near-must-run, essentially no load-following). Intentionally left
    # unmatched to RTS-GMLC; documented here as a known, deliberate outlier.
    (PrimeMovers.ST, ThermalFuels.NUCLEAR) => (up = 0.0001, down = 0.0001),
    (PrimeMovers.ST, ThermalFuels.GEOTHERMAL) => (up = 0.01, down = 0.01), # no geothermal unit in RTS-GMLC or other source found; unverified estimate, left as-is
)

const PSY_TO_WECC_DICT = Dict(
    (PrimeMovers.ST, ThermalFuels.COAL) => "CLLIG",

    (PrimeMovers.CA, ThermalFuels.NATURAL_GAS) => "CC",
    (PrimeMovers.CT, ThermalFuels.NATURAL_GAS) => "SC",
    (PrimeMovers.GT, ThermalFuels.NATURAL_GAS) => "SC",
    (PrimeMovers.ST, ThermalFuels.NATURAL_GAS) => "GS",
    # not from WECC: my own invented abbreviations.
    (PrimeMovers.ST, ThermalFuels.GEOTHERMAL) => "GEO",
    (PrimeMovers.ST, ThermalFuels.NUCLEAR) => "NUC",
)


function get_size(WECC_key::String, maxPower::Float64)
    if WECC_key in ("CC", "SC")
        if maxPower <= 90
            return "LE90"
        else
            return "GT90"
        end
    elseif WECC_key in ("GEO", "NUC")
        return "ANY"
    elseif WECC_key == "CLLIG"
        if maxPower <= 300
            return "SMALL"
        elseif maxPower <= 900
            return "LARGE"
        else
            return "SUPER"
        end
    elseif WECC_key == "GS"
        return "REH"  # default to reheat
    end
    @assert false "Unexpected input to get_size: $WECC_key, $maxPower"
    return "NONE"
end

const DURATION_LIMIT_DICT = Dict(
    # The original numbered WECC table this used to cite could not be located online.
    # Where RTS-GMLC's public per-unit gen.csv (github.com/GridMod/RTS-GMLC) has a matching
    # unit type/size, values below are corrected against it; where it doesn't, the original
    # WECC-cited value is kept, noted below.
    ("CLLIG", "SMALL") => (up = 8.0, down = 6.0), # WECC (1) Small coal; RTS-GMLC 76MW & 155MW coal units: up=8h both, down=4h/8h (midpoint used)
    ("CLLIG", "LARGE") => (up = 24.0, down = 48.0), # WECC (2) Large coal; RTS-GMLC 350MW coal unit: up=24h, down=48h
    ("CLLIG", "SUPER") => (up = 24.0, down = 48.0), # WECC (3) Super-critical coal; no RTS-GMLC unit >900MW -- mirrors LARGE as the best available estimate
    ("CC", "GT90") => (up = 8.0, down = 4.5), # WECC (7) Typical CC; RTS-GMLC 355MW CC/NG units
    ("CC", "LE90") => (up = 2.0, down = 4.0), # WECC (7) Typical CC, modified; no RTS-GMLC CC unit <=90MW -- original WECC-cited value kept
    ("GS", "NONR") => (up = 2.0, down = 4.0), # Gas steam non-reheat -> WECC (4); no NG-fired steam unit in RTS-GMLC -- original WECC-cited value kept
    ("GS", "REH") => (up = 2.0, down = 4.0), # Gas steam reheat boiler -> WECC (4); same as above
    ("GS", "SUP") => (up = 2.0, down = 4.0), # Gas-steam supercritical -> WECC (4); same as above
    ("SC", "GT90") => (up = 1.0, down = 1.0), # Simple-cycle greater than 90 MW -> WECC (5) Large-frame Gas CT; no RTS-GMLC unit >90MW -- original WECC-cited value kept
    ("SC", "LE90") => (up = 2.2, down = 2.2), # Simple-cycle less than 90 MW -> WECC (6) Aero derivative CT; RTS-GMLC 55MW CT/NG units
    # Not from WECC or RTS-GMLC: ballpark estimates given by Jose. RTS-GMLC's generic
    # nuclear entry uses 24h/48h (treating nuclear as a normal cyclable unit); kept far more
    # conservative here since the US nuclear fleet is essentially must-run between
    # refueling outages, not load-following. Documented here as a known, deliberate outlier.
    ("GEO", "ANY") => (up = 1000, down = 300), # no geothermal unit in RTS-GMLC or other source found; unverified estimate, left as-is
    ("NUC", "ANY") => (up = 8000, down = 8000),
)

const OTHER_TYPES = ("Synchronous Condenser", "IMPORT", "All Other")

const FUELS_DICT = Dict(
    # simpler to handle Natural Gas types by looking for "Natural Gas" substring
    "Municipal Solid Waste" => ThermalFuels.MUNICIPAL_WASTE,
    "Other Waste Biomass" => ThermalFuels.MUNICIPAL_WASTE,
    "Petroleum Liquids" => ThermalFuels.RESIDUAL_FUEL_OIL,
    "Geothermal" => ThermalFuels.GEOTHERMAL,
    "Nuclear" => ThermalFuels.NUCLEAR,
    "Wood/Wood Waste Biomass" => ThermalFuels.WOOD_WASTE_SOLIDS,
    "Conventional Steam Coal" => ThermalFuels.COAL,
    "Petroleum Coke" => ThermalFuels.PETROLEUM_COKE,
    "Landfill Gas" => ThermalFuels.MUNICIPAL_WASTE,
    "Other Gases" => ThermalFuels.OTHER_GAS
)
