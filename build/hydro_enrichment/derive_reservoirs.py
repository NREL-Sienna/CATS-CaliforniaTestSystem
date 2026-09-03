#!/usr/bin/env python3
"""Derive Stage 5 of
.claude/plans/2026-08-17-cats-hydro-enrichment-design.md ("Stage 5 -- Reservoirs").

Reads (both produced by earlier agents, already in the repo):
  - data/hydro_reservoir_sources.csv  (32 plants, raw NID/HILARRI/EHA/CDEC pull,
    with per-field *_source and *_confidence columns)
  - data/hydro_units.csv              (341 hydro units, 93 promoted=true)

Writes:
  - data/hydro_reservoirs.csv   (one row per promoted plant, 32 rows)
  - appends a "## 8. Reservoirs (Stage 5)" section to data/enrichment_report.md
    (sections 1-7, written by derive_enrichment.py, are never touched; a
    prior Stage 5 section from an earlier run of this script is replaced,
    not duplicated, so re-running is idempotent)

Every populated cell traces to a source named in param_source, or is one of
the sanctioned defaults/guesses called out in the design doc (50% initial
level, 0.0 min/evaporative loss, relative elevation datum, turbine-type
guess from head band) -- each labelled as such, never presented as measured.

Run from anywhere: `python3 build/hydro_enrichment/derive_reservoirs.py`
"""
import csv
import re
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = REPO_ROOT / "data"

RESERVOIR_SOURCES_CSV = DATA_DIR / "hydro_reservoir_sources.csv"
HYDRO_UNITS_CSV = DATA_DIR / "hydro_units.csv"
INFLOWS_CSV = DATA_DIR / "hydro_inflows.csv"
HYDRO_RESERVOIRS_CSV = DATA_DIR / "hydro_reservoirs.csv"
ENRICHMENT_REPORT_MD = DATA_DIR / "enrichment_report.md"

ACREFT_TO_M3 = 1233.4818

STAGE5_MARKER = "## 8. Reservoirs (Stage 5)"

FIELDNAMES = [
    "reservoir_name", "eia_plant_code", "plant_name",
    "storage_level_min_m3", "storage_level_max_m3", "initial_level_m3",
    "initial_level_fraction",
    "intake_elevation_m", "powerhouse_elevation_m", "head_m", "turbine_type",
    "level_data_type", "evaporative_loss", "inflow_m3s",
    "head_to_volume_slope", "head_is_fallback", "initial_is_default",
    "inflow_source", "param_source", "confidence", "notes",
]


def sanitize(name):
    """Same three rules as Stage 1's sanitize() in derive_enrichment.py:
    strip commas, '#' -> 'No', collapse repeated whitespace."""
    name = name.replace(",", "")
    name = name.replace("#", "No")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def load_reservoir_sources():
    with open(RESERVOIR_SOURCES_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 32, f"hydro_reservoir_sources.csv row count: {len(rows)} (expected 32)"
    return rows


def load_inflows():
    """PlantCode -> the hydro_inflows.csv row. Absent file is an error, not an empty dict:
    silently producing 32 inflow-free reservoirs is the failure this join exists to prevent."""
    if not INFLOWS_CSV.exists():
        raise FileNotFoundError(
            f"{INFLOWS_CSV} is missing; run derive_inflows.py before this script"
        )
    with open(INFLOWS_CSV, newline="") as f:
        return {r["eia_plant_code"].strip(): r for r in csv.DictReader(f)}


def load_promoted_plant_pms():
    """PlantCode -> [eia_pm, ...] for promoted=true units only, plus the
    plant_name as carried in hydro_units.csv (used only for a cross-check
    against hydro_reservoir_sources.csv's plant_name)."""
    with open(HYDRO_UNITS_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    by_plant_pm = defaultdict(list)
    by_plant_name = {}
    for r in rows:
        if r["promoted"] != "true":
            continue
        pc = r["PlantCode"].strip()
        by_plant_pm[pc].append(r["eia_pm"])
        by_plant_name[pc] = r["plant_name"].strip()
    assert len(by_plant_pm) == 32, f"promoted plants: {len(by_plant_pm)} (expected 32)"
    return by_plant_pm, by_plant_name


def turbine_type_for_plant(pm_list, head_m_final):
    """Design doc's head-band guess, with the pumped-storage exception
    applied only when every promoted unit at the plant is PS -- for a mixed
    plant (e.g. Edward C Hyatt, 3 HY + 3 PS) the head band is used instead,
    since the reservoir has both turbine kinds and a single per-reservoir
    field can't represent both. Returns (turbine_type, reason_string)."""
    if pm_list and all(pm == "PS" for pm in pm_list):
        return "FRANCIS", (
            "pumped-storage plant (all promoted units PS); reversible "
            f"Francis is the standard pump-turbine (head {head_m_final:.2f} m, not used for this guess)"
        )
    if head_m_final is None:
        return "FRANCIS", "no head data available; FRANCIS is the most common type overall"
    if head_m_final > 300:
        return "PELTON", f"head {head_m_final:.2f} m > 300 m"
    if head_m_final >= 30:
        return "FRANCIS", f"head {head_m_final:.2f} m in [30, 300] m band"
    return "KAPLAN", f"head {head_m_final:.2f} m < 30 m"


def derive_rows(source_rows, plant_pms, plant_names_from_units, inflow_by_plant):
    rows = []
    for sr in source_rows:
        pc = sr["eia_plant_code"].strip()
        plant_name = sr["plant_name"].strip()
        assert pc in plant_pms, f"eia_plant_code {pc} ({plant_name}) not found among promoted plants"
        assert plant_names_from_units[pc] == plant_name, (
            f"plant_name mismatch for {pc}: sources={plant_name!r} "
            f"hydro_units={plant_names_from_units[pc]!r}"
        )

        reservoir_name = f"Reservoir_{sanitize(plant_name)}"

        # storage_level_max_m3: NID storage volume. Uses nid_storage_acreft
        # (32/32 coverage per the design doc), not nid_max_storage_acreft
        # (31/32 -- James B Black's max-storage field is blank).
        nid_storage_acreft = float(sr["nid_storage_acreft"])
        storage_level_max_m3 = round(nid_storage_acreft * ACREFT_TO_M3, 1)
        storage_level_min_m3 = 0.0

        # initial_level_m3: CDEC 2019-01-01 storage where available (19/32),
        # else the sanctioned 50% default.
        cdec_2019 = sr["cdec_storage_20190101_acreft"].strip()
        if cdec_2019:
            initial_level_m3 = round(float(cdec_2019) * ACREFT_TO_M3, 1)
            initial_source = (
                f"CDEC station {sr['cdec_station']} storage on 2019-01-01 "
                "(matches CATS time-series epoch)"
            )
            initial_confidence = sr["cdec_confidence"] or "high"
            initial_is_default = False
        else:
            initial_level_m3 = round(0.5 * storage_level_max_m3, 1)
            initial_source = (
                "50% of storage_level_max_m3 (sanctioned default; no CDEC "
                "2019-01-01 reading for this plant)"
            )
            initial_confidence = "low (default)"
            initial_is_default = True

        # head_m: EHA unit-database head where available (25/32), else the
        # sanctioned NID dam_height_m fallback (32/32), recorded as such.
        eha_head = sr["head_m"].strip()
        if eha_head:
            head_m_final = round(float(eha_head), 2)
            head_source = "ORNL EHA Unit Database FY2026 (Head_ft, converted)"
            head_confidence = sr["head_confidence"] or "medium"
            head_is_fallback = False
        else:
            head_m_final = round(float(sr["dam_height_m"]), 2)
            head_source = (
                "NID dam_height_m fallback (sanctioned substitution; no EHA "
                "head for this plant)"
            )
            head_confidence = "medium (fallback: dam height used as head proxy)"
            head_is_fallback = True

        # Relative datum: powerhouse at 0.0, intake at head_m, so their
        # difference reproduces the real head (design doc, "Elevations have
        # no source at all").
        powerhouse_elevation_m = 0.0
        intake_elevation_m = head_m_final

        # head_to_volume_slope: LinearFunctionData coefficient for PSY's
        # head_to_volume_factor. POM's ReservoirHeadToVolumeConstraint reads it as
        # v = h * factor (core/constraints.jl:871), so the coefficient is m^3 per m --
        # volume over head, not head over volume.
        head_to_volume_slope = (
            float(f"{storage_level_max_m3 / head_m_final:.6g}")
            if head_m_final else ""
        )

        pm_list = plant_pms[pc]
        turbine_type, turbine_reason = turbine_type_for_plant(pm_list, head_m_final)

        level_data_type = "USABLE_VOLUME"
        evaporative_loss = 0.0
        # inflow_m3s: mean 2019 daily inflow from data/hydro_inflows.csv (CDEC sensor 76 or
        # USGS NWIS 00060). Real for 14 of 32 plants; the rest stay empty rather than
        # estimated, and build_CATS.jl reads an empty cell as 0.0.
        inflow_row = inflow_by_plant.get(pc, {})
        inflow_m3s = (inflow_row.get("mean_inflow_m3s") or "").strip()
        inflow_source = inflow_row.get("inflow_source", "").strip()

        param_source_parts = [
            f"storage_level_max_m3=USACE NID Public FeatureServer (nid_storage_acreft, {ACREFT_TO_M3} m3/acre-ft)",
            f"initial_level_m3={initial_source}",
            f"head_m={head_source}",
            "intake_elevation_m=relative datum, set equal to head_m (sanctioned; no elevation source exists)",
            "powerhouse_elevation_m=relative datum, sanctioned default 0.0",
            f"turbine_type=guessed from head band ({turbine_reason})",
            "level_data_type=USABLE_VOLUME (NID and CDEC both report volume directly)",
            "evaporative_loss=sanctioned default 0.0",
            f"inflow_m3s={inflow_source}" if inflow_m3s else
            "inflow_m3s=left empty; no CDEC or USGS flow gauge for this reservoir",
            "head_to_volume_slope=derived, storage_level_max_m3 / head_m (m^3 per m, POM v=h*factor)",
            "storage_level_min_m3=sanctioned default 0.0",
        ]
        confidence_parts = [
            f"storage_level_max_m3={sr['nid_confidence'] or 'high'}",
            f"initial_level_m3={initial_confidence}",
            f"head_m={head_confidence}",
            "turbine_type=guess (sanctioned)",
            "intake_elevation_m/powerhouse_elevation_m=default (sanctioned)",
            "evaporative_loss/storage_level_min_m3=default (sanctioned)",
            "head_to_volume_slope=derived",
        ]

        notes_parts = []
        if initial_is_default:
            notes_parts.append(
                "initial_level_m3 defaulted to 50% of max storage -- no CDEC station "
                "or no 2019-01-01 reading"
            )
        if head_is_fallback:
            notes_parts.append(
                "head_m substituted from NID dam_height_m -- no EHA unit head available"
            )
        notes_parts.append(f"turbine_type guess basis: {turbine_reason}")
        if sr["notes"]:
            notes_parts.append(f"hydro_reservoir_sources.csv notes: {sr['notes']}")

        rows.append({
            "reservoir_name": reservoir_name,
            "eia_plant_code": pc,
            "plant_name": plant_name,
            "storage_level_min_m3": storage_level_min_m3,
            "storage_level_max_m3": storage_level_max_m3,
            "initial_level_m3": initial_level_m3,
            "intake_elevation_m": intake_elevation_m,
            "powerhouse_elevation_m": powerhouse_elevation_m,
            "head_m": head_m_final,
            "turbine_type": turbine_type,
            "level_data_type": level_data_type,
            "evaporative_loss": evaporative_loss,
            "initial_level_fraction": round(initial_level_m3 / storage_level_max_m3, 6),
            "inflow_m3s": inflow_m3s,
            "head_is_fallback": "true" if head_is_fallback else "false",
            "initial_is_default": "true" if initial_is_default else "false",
            "inflow_source": inflow_source,
            "head_to_volume_slope": head_to_volume_slope,
            "param_source": "; ".join(param_source_parts),
            "confidence": "; ".join(confidence_parts),
            "notes": "; ".join(notes_parts),
            # kept only for the report writer, stripped before CSV write:
            "_initial_is_default": initial_is_default,
            "_head_is_fallback": head_is_fallback,
            "_turbine_reason": turbine_reason,
            "_pm_list": pm_list,
        })

    rows.sort(key=lambda r: int(r["eia_plant_code"]))
    return rows


def write_csv(rows):
    with open(HYDRO_RESERVOIRS_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in FIELDNAMES})


def write_report_section(rows):
    existing = ENRICHMENT_REPORT_MD.read_text()
    marker_pos = existing.find(f"\n{STAGE5_MARKER}")
    if marker_pos != -1:
        existing = existing[:marker_pos] + "\n"
    elif not existing.endswith("\n"):
        existing += "\n"

    lines = []

    def p(s=""):
        lines.append(s)

    p(STAGE5_MARKER)
    p()
    p(
        f"Derived by `build/hydro_enrichment/derive_reservoirs.py` from "
        f"`data/hydro_reservoir_sources.csv` (32 plants, NID/HILARRI/EHA/CDEC) and "
        f"`data/hydro_units.csv` (promoted units). One `HydroReservoir` row per "
        f"promoted plant, {len(rows)} total. Sections 1-7 above cover Stages 1-4 "
        f"and are unchanged by this script."
    )
    p()
    p("### 8a. Reservoir parameters, per plant")
    p()
    p(
        "| eia_plant_code | reservoir_name | storage_level_max_m3 (source, confidence) "
        "| initial_level_m3 (source, confidence) | head_m (source, confidence) "
        "| turbine_type (basis) | head_to_volume_slope |"
    )
    p("|---|---|---|---|---|---|---|")
    for r in rows:
        initial_src = "CDEC 2019-01-01" if not r["_initial_is_default"] else "50% of max (default)"
        initial_conf = r["confidence"].split("initial_level_m3=")[1].split(";")[0]
        head_src = "EHA unit head" if not r["_head_is_fallback"] else "NID dam_height_m (fallback)"
        head_conf = r["confidence"].split("head_m=")[1].split(";")[0]
        storage_conf = r["confidence"].split("storage_level_max_m3=")[1].split(";")[0]
        p(
            f"| {r['eia_plant_code']} | {r['reservoir_name']} "
            f"| {r['storage_level_max_m3']:,} (NID nid_storage_acreft, {storage_conf}) "
            f"| {r['initial_level_m3']:,} ({initial_src}, {initial_conf}) "
            f"| {r['head_m']} m ({head_src}, {head_conf}) "
            f"| {r['turbine_type']} ({r['_turbine_reason']}) "
            f"| {r['head_to_volume_slope']} |"
        )
    p()
    p(
        "`storage_level_min_m3` = 0.0, `powerhouse_elevation_m` = 0.0, "
        "`intake_elevation_m` = `head_m` (relative datum), `evaporative_loss` = 0.0, "
        "`inflow_m3s` = empty, and `level_data_type` = `USABLE_VOLUME` for all "
        f"{len(rows)} rows -- sanctioned defaults, not measured, see 8b."
    )
    p()

    n_initial_default = sum(1 for r in rows if r["_initial_is_default"])
    n_head_fallback = sum(1 for r in rows if r["_head_is_fallback"])
    n_ps_only = sum(1 for r in rows if all(pm == "PS" for pm in r["_pm_list"]))
    n_head_band = len(rows) - n_ps_only

    p("### 8b. Consolidated gaps, defaults, and guesses")
    p()
    p(
        f"- **`storage_level_min_m3` = 0.0 for all {len(rows)} rows** -- sanctioned "
        "default, not a measured empty reservoir floor."
    )
    p(
        f"- **`initial_level_m3` defaulted to 50% of `storage_level_max_m3` for "
        f"{n_initial_default} of {len(rows)} plants** (no CDEC station, or a station "
        "with no 2019-01-01 reading): "
        + ", ".join(sorted(r["reservoir_name"] for r in rows if r["_initial_is_default"]))
        + "."
    )
    p(
        f"- **`head_m` substituted from NID `dam_height_m` for {n_head_fallback} of "
        f"{len(rows)} plants** (no EHA unit head available): "
        + ", ".join(sorted(r["reservoir_name"] for r in rows if r["_head_is_fallback"]))
        + "."
    )
    p(
        f"- **`intake_elevation_m` and `powerhouse_elevation_m` are a relative datum "
        f"for all {len(rows)} rows**, not absolute elevations -- no source in scope "
        "(NID, EHA plant or unit databases) carries dam or reservoir elevation at all. "
        "`powerhouse_elevation_m` = 0.0 and `intake_elevation_m` = `head_m`, so their "
        "difference reproduces the real head."
    )
    p(
        f"- **`turbine_type` is guessed for all {len(rows)} rows.** "
        f"{n_ps_only} plants (all promoted units PS) use the pumped-storage override "
        f"to `FRANCIS`; the remaining {n_head_band} use the head band "
        "(>300 m PELTON, 30-300 m FRANCIS, <30 m KAPLAN) against the `head_m` value "
        "in this row (post-fallback where applicable) -- see column 8a "
        "'turbine_type (basis)' for the head that produced each guess."
    )
    p(
        f"- **`evaporative_loss` = 0.0 for all {len(rows)} rows** -- sanctioned "
        "default, not a measured zero-loss reservoir."
    )
    p(
        f"- **`inflow_m3s` left empty for all {len(rows)} rows.** "
        "`data/hydro_reservoir_sources.csv` carries no CDEC inflow/full-natural-flow "
        "field at all (only station, capacity, 2019-01-01 storage, and 2019-01-01 "
        "elevation) -- there is nothing genuinely derivable from CDEC for any plant, "
        "not just the ones lacking a station."
    )
    p(
        "- **`head_to_volume_slope` is a single linear coefficient "
        "(`head_m / storage_level_max_m3`) for the entire reservoir**, not a fitted "
        "storage-elevation curve -- CDEC's `cdec_elevation_20190101_ft` is a single "
        "point per plant (23/32 have one at all), not a curve, so a multi-point fit "
        "is not supported by the source data."
    )
    p(
        "- **Known-thin reservoirs still receive full-fidelity rows**, per the "
        "design doc's accepted limitation: Pit 5 (HILARRI states outright \"no "
        "reservoir\", NID storage 330 acre-ft), Pit 4 (1,970 acre-ft diversion pool), "
        "Kerckhoff 2 (4,252 acre-ft forebay shared with Kerckhoff 1), and Keswick "
        "(25,132 acre-ft re-regulating basin below Shasta) are promoted by the "
        "unchanged Stage 4 capacity rule and modelled the same as Shasta's "
        "4,661,860 acre-ft reservoir; this is unchanged by Stage 5 and not a new gap "
        "introduced here."
    )
    p(
        "- **`upstream_reservoirs` (cascade topology) is out of this CSV's schema "
        "entirely** and is deferred by the design doc (refinement item 3, Big Creek "
        "and Pit River chains) -- not represented as an empty column here because "
        "the requested schema does not include it."
    )
    p()

    with open(ENRICHMENT_REPORT_MD, "w") as f:
        f.write(existing + "\n".join(lines) + "\n")


def main():
    source_rows = load_reservoir_sources()
    plant_pms, plant_names_from_units = load_promoted_plant_pms()
    inflow_by_plant = load_inflows()

    source_codes = {r["eia_plant_code"].strip() for r in source_rows}
    assert source_codes == set(plant_pms), (
        "eia_plant_code sets differ between hydro_reservoir_sources.csv and "
        f"promoted plants in hydro_units.csv: "
        f"sources-only={source_codes - set(plant_pms)} "
        f"promoted-only={set(plant_pms) - source_codes}"
    )

    rows = derive_rows(source_rows, plant_pms, plant_names_from_units, inflow_by_plant)
    assert len(rows) == 32, f"reservoir rows: {len(rows)} (expected 32)"

    names = [r["reservoir_name"] for r in rows]
    assert len(names) == len(set(names)), "duplicate reservoir_name values"

    DATA_DIR.mkdir(exist_ok=True)
    write_csv(rows)
    write_report_section(rows)
    print(f"Wrote {HYDRO_RESERVOIRS_CSV} ({len(rows)} rows)")
    print(f"Appended Stage 5 section to {ENRICHMENT_REPORT_MD}")


if __name__ == "__main__":
    main()
