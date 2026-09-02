<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/WISPO-POP/CATS-CaliforniaTestSystem">
    <img src="logo.png" alt="Logo" width="300" height="300">
  </a>

<h3 align="center">CATS - California Test System</h3>

  <p align="center">
    A geographically-accurate synthetic grid in California.
    <!-- <br /> -->
    <!-- <a href="https://github.com/WISPO-POP/CATS-CaliforniaTestSystem"><strong>View Documentation »</strong></a> -->
    <!-- <br /> -->
    <br />
    <a href="https://github.com/WISPO-POP/CATS-CaliforniaTestSystem/issues">Report Bug</a>
    ·
    <a href="https://github.com/WISPO-POP/CATS-CaliforniaTestSystem/issues">Request Feature</a>
  </p>
</div>


Project Link: [https://github.com/WISPO-POP/SyntheticCaliforniaGrid](https://github.com/WISPO-POP/CATS-CaliforniaTestSystem)

Time-series data (load and renewable generation) link: [https://tinyurl.com/SyntheticCaliforniaGridData](https://drive.google.com/drive/folders/1Zo6ZeZ1OSjHCOWZybbTd6PgO4DQFs8_K?usp=sharing)

## Description
This repository contains the data files the California Test System (CATS), which is a geographically-accurate, synthetic electric grid model that is located in California. This model was created using publicly available geographic data of California's actual transmission lines, substations, and power plants, which we combined with invented connections and parameters that are "realistic but not real". For more information about how this network model was created, please refer to our publication: [California Test System (CATS): A Geographically Accurate Test System based on the California Grid](https://ieeexplore.ieee.org/document/10337776).

The grid model is available in two formats. The MATPOWER file format is suitable for electric power system simulation and optimization. The geographic information systems (GIS) files, in .GEOJSON and .CSV formats, are suitable for geospatial analyses.

We also provide the load and renewable generation profiles that we used in the creation and evaluation of this grid. Since some of these data files exceed the size limit on GitHub, they are available in a Google Drive folder at this link: [https://tinyurl.com/SyntheticCaliforniaGridData](https://drive.google.com/drive/folders/1Zo6ZeZ1OSjHCOWZybbTd6PgO4DQFs8_K?usp=sharing).

## Usage
Clone the repository
```julia
   git clone https://github.com/WISPO-POP/CATS-CaliforniaTestSystem.git
```

Run a DC optimal power flow using [PowerModels.jl](https://github.com/lanl-ansi/PowerModels.jl) by executing the file `run_opf.jl`.

## Sienna / psy6: building and running the system

The Sienna form of CATS is built from source data in this repository, not shipped as a
serialized system. The only serialization format is the **OpenAPI bundle** written to
`CATS_openapi/`; psy5-era `CATS_Sienna*.json`/`.h5` files are gone and will not load into
psy6 PowerSystems.

### Repository layout

| Path | Contents |
|---|---|
| `build/` | Everything that builds the Sienna system: `build_CATS.jl` and its includes, the hydro/reactive enrichment scripts, and the build environment (`Project.toml`). |
| `Sienna/` | The model: `cats_model.jl` (single-instance security-constrained unit commitment) and its own environment. |
| `data/` | Enrichment inputs (small, tracked) plus `download_data.sh` for the large time-series files (not tracked). |
| `MATPOWER/` | The upstream network case — the root input to the build. |
| `GIS/` | `CATS_buses.csv`, `CATS_gens.csv`, `CATS_lines.json` — also build inputs, despite the directory name. |
| `test/` | Round-trip test: rebuilds nothing, reads `CATS_openapi/` back and checks it against the enrichment CSVs. |
| `Archive/` | Upstream provenance (original `.m`, EIA generator data, GeoJSON) that the enrichment cites. |

`build/` and `Sienna/` are separate environments on purpose: the build needs PowerSystems and
the parsers, the model needs PowerOperationsModels and a solver. Neither imports the other.

### Rebuilding the system from scratch

Requires Julia 1.12+, `gdown` (`pip install gdown`), and the psy6 workspace checkouts — both
environments resolve PowerSystems, InfrastructureSystems and the OpenAPI model packages from
sibling directories via `[sources]` path pins, so this repository must sit inside that
workspace.

```bash
# 1. Fetch the two large time-series files (~580 MB); everything else is in the repo.
./data/download_data.sh

# 2. Instantiate the build environment.
julia --project=build -e 'using Pkg; Pkg.instantiate()'

# 3. Build. Parses the MATPOWER case, applies the EIA/CAISO and hydro enrichment, and writes
#    CATS_openapi/{system.json,time_series.h5}. Takes several minutes on ~8,870 buses.
julia --project=build build/build_CATS.jl

# 4. Verify. Reads the bundle back through PowerSystems.from_file and checks every count and
#    field against the enrichment CSVs (492 assertions).
julia --project=build test/runtests.jl
```

Step 3 converts `Load_Agg_Post_Assignment_v3_latest.csv` to a `.jld2` cache on first run,
which takes a few minutes and ~1.2 GB. To do it as its own step instead:

```bash
julia --project=build build/convert_load_csv_to_jld2.jl
```

The `.jld2` is a cache, not an input — deleting it only costs rebuild time.

### Running the model

```bash
julia --project=Sienna -e 'using Pkg; Pkg.instantiate()'
julia --project=Sienna Sienna/cats_model.jl
```

Solves one unit-commitment instance in which the dispatch must respect line flows in the base
case and under N-1 contingencies, and writes peak dual reports to `Sienna/csv_results/`. Size
it with environment variables rather than editing the file — `CATS_N_GATES` (number of 500 kV
contingencies, default 5), `CATS_N_MONITORED` (230 kV lines watched under each, default 50),
`CATS_PTDF_TOL`. The defaults solve in about a minute; the post-contingency constraint count
scales as their product, so raise both with care.

For a fast end-to-end check:

```bash
CATS_N_GATES=1 CATS_N_MONITORED=5 CATS_PTDF_TOL=0.1 julia --project=Sienna Sienna/cats_model.jl
```

### Recent Updates
The CATS was updated on November 11, 2023. All previous versions are available in the 'Archive' directory.
<!-- Due to formatting restrictions, the MATPOWER and GIS formats of the CATS model have different indices for components. The CSV files in the `Additional Data Files` folder map this relationship, in addition to providing additional data fields. 

In `branch_data.csv`, "Branch Number" is the MATPOWER index in `CaliforniaTestSystem.m`, while "FID" is the GIS index in `lines.geojson`. WARNING: There is currently an issue with the ID mapping in `branch_data.csv`. This issue should be resolved soon. Thank you for your patience.

In `bus_data.csv`, "Bus number" is the MATPOWER index in `CaliforniaTestSystem.m`, while "FID" is the GIS index in `added_nodes.geojson` and `substations.geojson`. Note: An identifier to distinguish between substations and added nodes will be added to `bus_data.csv` soon. Note: In the network creation process, not all buses are included in the largest connected grid. Therefore, there are some buses that are present in the GIS data, but are not included in the MATPOWER version of CATS.

In `gen_data.csv`, "Generator number" and "Bus number" are the MATPOWER indices in `CaliforniaTestSystem.m`, while PlantCode and GenId identify generators in `EIA_Generator_Y2019.csv`. Note: In the network creation process, not all generators are included in the largest connected grid. Therefore, there are some generators that are present in the data from the EIA-860, but are not included in the MATPOWER version of CATS. -->

<!-- ## Contents of this repository
Key files and folders of this repository are described below.

* `MATPOWER` -- folder that contains the MATPOWER version of the grid
  * `CaliforniaTestSystem.m` -- MATPOWER file of the Synthetic California Grid
* `GIS` -- folder that contains the GIS version of the grid 
  * `lines.geojson` -- GEOJSON file of the transmission lines (modified from the CEC version)
  * `substations.geojson` -- GEOJSON file of the substations (modified from the CEC version)
  * `added_nodes.geojson` -- GEOJSON file of the nodes added to the system for connectivity
  * `EIA_Generator_Y2019.csv` -- CSV files of the generators (unmodified from EIA), contains geographic coordinates
* `Additional Data Files` -- folder that contains additional component data. Includes IDs to map between GIS and MATPOWER files.
  * `branch_data.csv` -- CSV file of additional branch data
  * `bus_data.csv` -- CSV file of additional bus data
  * `gen_data.csv` -- CSV file of additional generator data
* `run_opf.jl` -- Julia script to run a DC optimal power flow analysis of the grid -->

## Citation
If you use this repository, please cite our publication:
```
@article{CATS2024,
  author={Taylor, Sofia and Rangarajan, Aditya and Rhodes, Noah and Snodgrass, Jonathan and Lesieutre, Bernard C. and Roald, Line A.},
  journal={IEEE Transactions on Energy Markets, Policy and Regulation}, 
  title={California Test System (CATS): A Geographically Accurate Test System Based on the California Grid}, 
  year={2024},
  volume={2},
  number={1},
  pages={107-118},
  doi={10.1109/TEMPR.2023.3338568}
}
```

<!-- LICENSE -->
<!-- ## License
UNCOMMENT THIS SECTION AND ADD LICENSE FILE IF MADE PUBLIC.
Distributed under the UW License. See `LICENSE.txt` for more information. -->

<!-- ## Contact
UNCOMMENT THIS SECTION AND ADD CONTACT DETAILS IF MADE PUBLIC.
Your Name - [@twitter_handle](https://twitter.com/twitter_handle) - email@email_client.com

Project Link: [https://github.com/github_username/repo_name](https://github.com/github_username/repo_name) -->

## Acknowledgments

The California Test System was developed by researchers at the University of Wisconsin-Madison and Texas A&M University:

* Sofia Taylor\*, UW Madison
* Aditya Rangarajan\*, UW Madison
* Noah Rhodes, UW Madison
* Jonathan Snodgrass, Texas A&M
* Bernie Lesieutre, UW Madison
* Line A. Roald, UW Madison

\* Indicates equal contribution.


This work is funded in part by the Power Systems Engineering Research Center (PSERC) through project S-91, the National Science Foundation (NSF) under Grant. No. ECCS-2045860, and the NSF Graduate Research Fellowship Program under Grant No. DGE-1747503.

<p align="right">(<a href="#top">back to top</a>)</p>
