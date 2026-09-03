# Changelog

Changes to the CATS system data beyond the upstream WISPO-POP release. Newest first.

Sources are cited inline; see [Data sources](#data-sources) for the full list.

---

## 2026-08-17 — CAISO reactive resources

Replaced the placeholder reactive support with the CAISO transmission planning inventory.

**Source:** CAISO, *Board-Approved 2025-2026 ISO Transmission Plan, Appendix A: System Data*,
May 19 2026, section 3 "Reactive Resources".
<https://www.caiso.com/documents/board-approved-2025-2026-transmission-plan-appendix-a-system-data.pdf>
(retrieved 2026-08-17).

### Why

CATS inherited 1743 synchronous condensers from the MATPOWER file, every one of them carrying
an identical `Qmax = +200` / `Qmin = -200` / `Pmax = 0` — not a single distinct value in the
entire set. The number is a placeholder, not a measurement, and the reactive-limit assertions in
`build_CATS.jl` are commented out, so nothing checked it.

The build already culled 1418 of those condensers, leaving 122. That still amounted to
24,400 MVAr of *dynamic* reactive support against CAISO's real 2,923 MVAr (10 synchronous
condensers plus 3 SVCs) — **8.3× over-provisioned**. Normalised against the CATS 2019 peak load
of 44,001 MW, CATS carried 0.55 MVAr per MW of peak where CAISO carries 0.066.

The repo's own measurements agree and are blunter still: `data/fixed_admittance_candidates.csv`
records observed injections with a **median of 0.064 MVAr** against the 200 MVAr nameplate —
0.03 % utilisation.

Meanwhile CATS modelled **no bulk shunt compensation at all** (2.5 MVAr total), against CAISO's
3,666 MVAr across 18 banks. The system was over-providing dynamic support and missing static
support — backwards from the real grid.

### Changes

| | before | after |
|---|---|---|
| Placeholder synchronous condensers | 122 × ±200 MVAr = 24,400 MVAr | 122 × ±100 MVAr = 12,200 MVAr |
| Real synchronous condensers | none | 10 units, 1,818 MVAr |
| SVCs (as synchronous condensers) | none | 3 units, 1,105 MVAr |
| Bulk shunt capacitors | none | 18 banks, 3,666 MVAr |

Real dynamic support now totals **2,923 MVAr**, matching CAISO's actual fleet exactly.

1. **Curtailed the placeholder fleet.** Every `SynchronousCondenser` present before the CAISO
   units are added is re-rated to ±100 MVAr (`CURTAILED_CONDENSER_MVAR`). Ordering matters: the
   curtailment runs first, so the real units added in step 2 keep their true ratings.

2. **Added the 18 Appendix A shunt capacitor banks** as `FixedAdmittance`, totalling 3,666 MVAr:
   Gates 225, Los Banos 225, Gregg 150, McCall 132, Mesa (PG&E) 100, Metcalf 350, Olinda 200,
   Table Mountain 454, Mira Loma 230 kV 158, Mira Loma 500 kV 300, Mesa 500/230 kV 405,
   San Luis Rey 63, Bay Boulevard 100, Miguel 126, Escondido 126, Suncrest 126, Capistrano 150,
   Penasquitos 276.

3. **Added the 10 real synchronous condensers** at their Appendix A ratings, totalling
   1,818 MVAr: Santiago 3×81, San Luis Rey 2×225, Talega 2×225, Miguel 2×225, San Onofre 225.

4. **Added the 3 SVCs, modelled as synchronous condensers**, totalling 1,105 MVAr: Devers 605,
   Rector 200, Suncrest 300. Both device classes give continuously variable dynamic reactive
   support and CATS has no SVC representation, so a condenser is the simple stand-in.
   `reactive_resources.csv` keeps `technology` (what Appendix A says) separate from
   `component_type` (what is built), and these components are named `SVC_<substation>`, so the
   substitution is visible in results rather than hidden.

   Appendix A gives Devers as "156 & 605 (dynamic capability)". Only the 605 MVAr dynamic
   figure is modelled; the 156 MVAr fixed portion is not.

### Siting

Each device is placed on the nearest CATS bus at its rated voltage, matched through
`Archive/substations.geojson` (which is in **Web Mercator, EPSG:3857**, not lat/lon — it must be
reprojected before comparing against `GIS/CATS_buses.csv`).

All 23 devices sited **within 0.01 km** of their real substation, because the CATS buses were
themselves derived from this same substation dataset. The siting is effectively exact.

Three name resolutions were needed, and two disambiguations:

- `McCall` → `Mc Call` (Fresno), `Table Mountain` → `Table Mt.` (Butte).
- `Bay Boulevard` → `South Bay` (220 kV, Chula Vista). **Inferred**, not an exact name match —
  the SDG&E Bay Boulevard site adjoins the former South Bay Power Plant. The one soft match here.
- `Mesa` is ambiguous: Appendix A's `Mesa (PG&E)` is the San Luis Obispo station (owner PG&E),
  while `Mesa 500/230 kV` in the SCE block is the Los Angeles station. The geojson `Owner` field
  resolves it exactly as the Appendix A parenthetical intends.
- `Olinda` likewise: the PG&E-block entry is `Olinda (Vic Fazio)` in Shasta at 500 kV, not the
  SCE `Olinda` in Los Angeles.

CATS carries only four voltage levels (66, 115, 230, 500 kV), so **Capistrano and Talega — both
138 kV in reality — are sited at 115 kV**, the nearest available.

### Naming

Components are named `ShuntCapacitor_<substation>`, `SynchronousCondenser_<substation>_<n>`, and
`SVC_<substation>_<n>` — the prefix comes from the `name_prefix` column, so an SVC stays
identifiable even though it is built as a `SynchronousCondenser`. The substation label is
reduced to word characters by `_component_label`. Names become
serialization keys and result-file columns, so `Mesa 500/230 kV` must not carry a path
separator; it becomes `Mesa_500_230_kV`.

### Files

- `data/reactive_resources.csv` — the sited device table (new, tracked via a `.gitignore`
  negation, since `data/*` is otherwise ignored).
- `Sienna/site_reactive_resources.py` — regenerates that CSV from Appendix A plus the geojson.
- `Sienna/build_CATS.jl` — `add_caiso_reactive_resources!`, called at the end of
  `build_CATS_system`.

### Not done

**SVCs are approximated, not modelled.** A synchronous condenser has rotating inertia and a
short-term overload capability an SVC does not, and an SVC's response is faster. For steady-state
UC/ED and power flow the distinction does not bind; for dynamic or short-circuit studies it does.

**The placeholder fleet is still the dominant reactive source.** The real inventory now
reproduces CAISO exactly at 3,666 MVAr static and 2,923 MVAr dynamic, but the 122 curtailed
placeholders add a further 12,200 MVAr of dynamic support on top — over 4× the real dynamic
fleet, and their ±100 MVAr is still ~1,500× the 0.064 MVAr median those units were measured
injecting. Retiring or further curtailing them is the remaining work; with the real devices now
in place, they no longer carry any modelling burden.

**Devers' 156 MVAr fixed portion** is not represented (see change 4).

---

## 2026-08-17 — EIA enrichment: names, prime movers, plants, hydro reservoirs

Design: [`.claude/plans/2026-08-17-cats-hydro-enrichment-design.md`](.claude/plans/2026-08-17-cats-hydro-enrichment-design.md).
Per-unit audit trail: `data/enrichment_report.md`.

**Source:** EIA-860 (2019), `Archive/EIA_Generator_Y2019.csv`, joined on the `PlantCode`/`GenID`
already carried by `GIS/CATS_gens.csv`. Reservoir parameters from ORNL HILARRI v4 + EHA FY2026,
USACE NID, CDEC, and USGS NWIS.

| change | result |
|---|---|
| Generators renamed to `<Plant Name>_<Generator ID>` | 2123 (zero `gen-N` left among real units) |
| Prime movers corrected from EIA | 720 of 2123 |
| Plant supplemental attributes | 1028, plus 65 `CombinedCycleBlock` |
| Hydro promoted | 80 `HydroTurbine` + 13 `HydroPumpTurbine`, 32 `HydroReservoir` |
| Reservoirs with real 2019 inflow | 14 of 32 |
| `DataSource` provenance | 10 shared attributes, 2264 associations |

Four generator identities were recovered from Excel date corruption (`44927` → `1-Jan`), which
rescued Otay Mesa, a 688.5 MW combined-cycle plant that would otherwise have been dropped.

Provenance is queryable rather than documentary: the 7 reservoirs whose head came from a
dam-height fallback and the 13 on a defaulted initial level carry their own **low**-confidence
`DataSource`, distinct from the 25 and 19 backed by real measurements.

### Requires a PowerSystems.jl fix

`Sienna/Project.toml` currently points `PowerSystems` at a **local checkout** rather than the
`psy6` git rev, because exporting a `HydroPumpTurbine` hits
`UndefVarError: _turbinepump_po`. The converter is declared in `generate_structs.jl:226` and
called six times by the generated code, but never defined — so no psy6 system containing pumped
storage can be serialized. One line in `src/openapi/export_generated_types.jl` fixes it:

```julia
_turbinepump_po(nt) = PC.TurbinePump(; turbine = nt.turbine, pump = nt.pump)
```

**Restore the git pin once that lands upstream.**

---

## Data sources

| Source | Used for | Location |
|---|---|---|
| CAISO *Board-Approved 2025-2026 ISO Transmission Plan, Appendix A: System Data*, 2026-05-19 | shunt capacitor and synchronous condenser inventory, ratings, substations | [caiso.com](https://www.caiso.com/documents/board-approved-2025-2026-transmission-plan-appendix-a-system-data.pdf) |
| EIA-860 (2019) | generator identity, prime movers, plant grouping, combined-cycle blocks | `Archive/EIA_Generator_Y2019.csv` |
| HIFLD / CEC substations | substation names and coordinates (EPSG:3857) | `Archive/substations.geojson` |
| CAISO 2019 hourly production | load and generation profiles | `data/HourlyProduction2019.csv` |
