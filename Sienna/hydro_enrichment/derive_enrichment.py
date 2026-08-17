#!/usr/bin/env python3
"""Derive CATS generator names, prime movers, plant groupings, and hydro
classification from EIA-860 identifiers already present in GIS/CATS_gens.csv.

Implements Stages 1-4 of
.claude/plans/2026-08-17-cats-hydro-enrichment-design.md. Reads:
  - GIS/CATS_gens.csv           (CATS generator rows, PlantCode/GenID keys)
  - Archive/EIA_Generator_Y2019.csv  (EIA-860 generator table, same vintage)
  - Sienna/generator_types.jl   (current PM_TYPE_DICT, parsed not duplicated)

Writes:
  - data/generator_names.csv
  - data/generator_prime_movers.csv
  - data/generator_plants.csv
  - data/hydro_units.csv
  - data/enrichment_report.md

Every number in the design doc's "Facts already verified" list is
recomputed and asserted here. An assertion failure means the design doc's
premise no longer holds against the data -- do not loosen it, investigate.

Run from anywhere: `python3 Sienna/hydro_enrichment/derive_enrichment.py`
"""
import csv
import datetime
import re
from collections import Counter, defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = REPO_ROOT / "data"

CATS_GENS_CSV = REPO_ROOT / "GIS" / "CATS_gens.csv"
EIA_GENERATOR_CSV = REPO_ROOT / "Archive" / "EIA_Generator_Y2019.csv"
GENERATOR_TYPES_JL = REPO_ROOT / "Sienna" / "generator_types.jl"

EXCEL_EPOCH = datetime.date(1899, 12, 30)  # Excel 1900-system serial-date epoch

# PrimeMovers enum values as defined in
# psy6 PowerSystems.jl/src/definitions.jl (verified against the checkout at
# /home/jdlara/Sienna_work/psy6/PowerSystems.jl on 2026-08-17).
PRIME_MOVERS_ENUM = {
    "BA", "BT", "CA", "CC", "CE", "CP", "CS", "CT", "ES", "FC", "FW", "GT",
    "HA", "HB", "HK", "HY", "IC", "PS", "OT", "ST", "PVe", "WT", "WS",
}

# CombinedCycleConfiguration mapping per the design doc's block-composition
# table, matched against PowerSystems.jl/src/definitions.jl's enum values.
def cc_configuration(ct_count, ca_count):
    if ct_count == 1 and ca_count == 1:
        return "SeparateShaftCombustionSteam"
    if ct_count == 2 and ca_count == 1:
        return "DoubleCombustionOneSteam"
    if ct_count == 3 and ca_count == 1:
        return "TripleCombustionOneSteam"
    return "Other"


def sanitize(name):
    """Design doc Stage 1 sanitization: strip commas, '#' -> 'No', collapse
    repeated whitespace. NOT a blanket \\w-only reduction -- the design's own
    canonical example ('Otay Mesa Generating Project_1-Jan') keeps a hyphen,
    so uniqueness is re-checked after these three rules only."""
    name = name.replace(",", "")
    name = name.replace("#", "No")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def classify(fuel_type):
    """Mirrors the FuelType branching in Sienna/build_CATS.jl's STEP 1 loop,
    used here only to decide which PowerPlant grouping category a matched
    unit falls into. Order matters and matches the Julia elseif chain."""
    if "Hydroelectric" in fuel_type:
        return "hydro"
    if "Solar" in fuel_type or "Wind" in fuel_type:
        return "renewable"
    if fuel_type == "IMPORT":
        return "import"
    if fuel_type == "Synchronous Condenser":
        return "sc"
    if "Batteries" in fuel_type:
        return "battery"
    return "thermal"


def load_cats_gens():
    with open(CATS_GENS_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    for i, row in enumerate(rows, start=1):
        row["row_index"] = i
    return rows


def load_eia_generators():
    with open(EIA_GENERATOR_CSV, encoding="latin-1", newline="") as f:
        return list(csv.DictReader(f))


def parse_pm_type_dict():
    """Parse PM_TYPE_DICT out of generator_types.jl instead of duplicating it
    by hand, so Stage 2's 'current_pm' column can never drift from the real
    dict."""
    src = GENERATOR_TYPES_JL.read_text()
    m = re.search(
        r"const PM_TYPE_DICT = Dict\{String, PSY\.PrimeMovers\}\((.*?)\n\)",
        src,
        re.S,
    )
    if not m:
        raise RuntimeError("Could not locate PM_TYPE_DICT in generator_types.jl")
    block = m.group(1)
    pm_dict = {}
    for line in block.splitlines():
        entry = re.match(
            r'\s*"([^"]+)"\s*=>\s*PrimeMovers\.(\w+),?\s*(#.*)?$', line,
        )
        if entry:
            pm_dict[entry.group(1)] = entry.group(2)
    return pm_dict


def build_join(cats_rows, eia_rows):
    """Join CATS_gens.csv to EIA-860 on (PlantCode, GenID), with the Excel
    serial-date repair for the residual unmatched rows whose GenID is purely
    numeric. Returns matched: {row_index: (cats_row, eia_row, method)} and
    unmatched: [cats_row, ...]."""
    eia_by_plant = defaultdict(list)
    eia_lookup = {}
    for r in eia_rows:
        pc = r["Plant Code"].strip()
        gid = r["Generator ID"].strip()
        eia_by_plant[pc].append(r)
        # First occurrence wins -- the raw EIA-860 archive carries exactly
        # one duplicate (Plant Code, Generator ID) key (56032, '1'), two
        # rows differing only in Summer/Winter Capacity. That plant does not
        # appear in CATS_gens.csv, so it never reaches this join either way;
        # first-wins keeps the lookup deterministic regardless.
        if (pc, gid) not in eia_lookup:
            eia_lookup[(pc, gid)] = r

    matched = {}
    unmatched = []
    for row in cats_rows:
        pc = row["PlantCode"].strip()
        gid = row["GenID"].strip()
        key = (pc, gid)
        if key in eia_lookup:
            matched[row["row_index"]] = (row, eia_lookup[key], "eia_exact")
        else:
            unmatched.append(row)

    excel_repairs = []
    still_unmatched = []
    for row in unmatched:
        gid = row["GenID"].strip()
        pc = row["PlantCode"].strip()
        if gid.isdigit():
            serial = int(gid)
            d = EXCEL_EPOCH + datetime.timedelta(days=serial)
            d_mmm = f"{d.day}-{d.strftime('%b')}"
            mmm_yy = d.strftime("%b-%y")
            hit = None
            for candidate in eia_by_plant.get(pc, []):
                cid = candidate["Generator ID"].strip()
                if cid == d_mmm or cid == mmm_yy:
                    hit = candidate
                    break
            if hit is not None:
                matched[row["row_index"]] = (row, hit, "eia_excel_repair")
                excel_repairs.append((row, hit, serial, d, d_mmm, mmm_yy))
                continue
        still_unmatched.append(row)

    return matched, still_unmatched, excel_repairs


def main():
    cats_rows = load_cats_gens()
    eia_rows = load_eia_generators()
    pm_type_dict = parse_pm_type_dict()

    assert len(cats_rows) == 3892, f"CATS_gens.csv row count changed: {len(cats_rows)}"

    matched, unmatched, excel_repairs = build_join(cats_rows, eia_rows)

    exact_count = sum(1 for _, _, m in matched.values() if m == "eia_exact")
    assert exact_count == 2119, f"exact join count: {exact_count} (expected 2119)"
    assert len(excel_repairs) == 4, f"excel repairs: {len(excel_repairs)} (expected 4)"
    assert len(matched) == 2123, f"total matched: {len(matched)} (expected 2123)"

    unmatched_fuel_counts = Counter(r["FuelType"] for r in unmatched)
    assert len(unmatched) == 1769, f"unmatched count: {len(unmatched)} (expected 1769)"
    assert unmatched_fuel_counts["Synchronous Condenser"] == 1743, unmatched_fuel_counts
    assert unmatched_fuel_counts["IMPORT"] == 26, unmatched_fuel_counts
    assert set(unmatched_fuel_counts) == {"Synchronous Condenser", "IMPORT"}, (
        f"unmatched rows outside SC/IMPORT: {unmatched_fuel_counts}"
    )

    # Excel repair must recover exactly Otay Mesa (3) + Hesperia (1), by
    # PlantCode/GenID and rendered date, per the design doc's table.
    repair_by_key = {
        (r["PlantCode"].strip(), r["GenID"].strip()): (d_mmm, mmm_yy, hit)
        for r, hit, serial, d, d_mmm, mmm_yy in excel_repairs
    }
    expected_repairs = {
        ("55345", "44927"): "1-Jan",
        ("55345", "44928"): "2-Jan",
        ("55345", "44929"): "3-Jan",
        ("59182", "34608"): "Oct-94",
    }
    for key, expected_id in expected_repairs.items():
        assert key in repair_by_key, f"expected excel repair missing: {key}"
        d_mmm, mmm_yy, hit = repair_by_key[key]
        matched_id = hit["Generator ID"].strip()
        assert matched_id == expected_id, (
            f"{key} matched EIA Generator ID {matched_id!r}, expected {expected_id!r}"
        )

    # ---------------- Stage 1: names ----------------
    raw_names = {}
    sanitized_names = {}
    for row_index, (cats_row, eia_row, method) in matched.items():
        plant_name = eia_row["Plant Name"].strip()
        gen_id = eia_row["Generator ID"].strip()
        raw = f"{plant_name}_{gen_id}"
        raw_names[row_index] = raw
        sanitized_names[row_index] = sanitize(raw)

    dup_raw = len(raw_names) - len(set(raw_names.values()))
    assert dup_raw == 0, f"duplicate raw names before sanitization: {dup_raw}"
    dup_san = len(sanitized_names) - len(set(sanitized_names.values()))
    assert dup_san == 0, f"duplicate names AFTER sanitization: {dup_san}"

    plant_name_to_codes = defaultdict(set)
    for row_index, (cats_row, eia_row, method) in matched.items():
        plant_name_to_codes[eia_row["Plant Name"].strip()].add(
            cats_row["PlantCode"].strip()
        )
    multi_code_plants = {k: v for k, v in plant_name_to_codes.items() if len(v) > 1}
    assert not multi_code_plants, f"plant names mapping to >1 PlantCode: {multi_code_plants}"

    punctuation_affected = sum(
        1 for ri in matched if ("," in raw_names[ri] or "#" in raw_names[ri])
    )

    # ---------------- Stage 2: prime movers ----------------
    eia_pm_map = {}
    current_pm_map = {}
    changed_map = {}
    for row_index, (cats_row, eia_row, method) in matched.items():
        eia_pm_raw = eia_row["Prime Mover"].strip()
        eia_pm = "PVe" if eia_pm_raw == "PV" else eia_pm_raw
        assert eia_pm in PRIME_MOVERS_ENUM, (
            f"row {row_index}: EIA prime mover {eia_pm_raw!r} has no PrimeMovers equivalent"
        )
        fuel_type = cats_row["FuelType"]
        assert fuel_type in pm_type_dict, (
            f"row {row_index}: FuelType {fuel_type!r} missing from PM_TYPE_DICT"
        )
        current_pm = pm_type_dict[fuel_type]
        eia_pm_map[row_index] = eia_pm
        current_pm_map[row_index] = current_pm
        changed_map[row_index] = current_pm != eia_pm

    changed_count = sum(1 for v in changed_map.values() if v)
    unchanged_count = len(changed_map) - changed_count
    assert changed_count == 720, f"prime movers changed: {changed_count} (expected 720)"
    assert unchanged_count == 1403, f"prime movers unchanged: {unchanged_count} (expected 1403)"

    # ---------------- Stage 3: plant grouping ----------------
    category_map = {ri: classify(cr["FuelType"]) for ri, (cr, er, m) in matched.items()}

    cc_row_indices = [ri for ri in matched if eia_pm_map[ri] in ("CT", "CA", "CS")]
    assert len(cc_row_indices) == 188, f"CC-prime-mover units: {len(cc_row_indices)} (expected 188)"
    cc_plants = {matched[ri][0]["PlantCode"] for ri in cc_row_indices}
    assert len(cc_plants) == 62, f"CC plants: {len(cc_plants)} (expected 62)"

    with_unit_code = [
        ri for ri in cc_row_indices if matched[ri][1]["Unit Code"].strip() != ""
    ]
    without_unit_code = [
        ri for ri in cc_row_indices if matched[ri][1]["Unit Code"].strip() == ""
    ]
    assert len(with_unit_code) == 183, f"CC units with Unit Code: {len(with_unit_code)} (expected 183)"
    assert len(without_unit_code) == 5, f"CC units without Unit Code: {len(without_unit_code)} (expected 5)"
    assert all(eia_pm_map[ri] == "CS" for ri in without_unit_code), (
        "CC units without Unit Code are not all CS as expected"
    )

    blocks = defaultdict(list)
    for ri in with_unit_code:
        cats_row, eia_row, _ = matched[ri]
        key = (cats_row["PlantCode"], eia_row["Unit Code"].strip())
        blocks[key].append(ri)
    assert len(blocks) == 65, f"CC blocks: {len(blocks)} (expected 65)"

    block_info = {}
    composition_counts = Counter()
    for key, members in blocks.items():
        ct = sum(1 for ri in members if eia_pm_map[ri] == "CT")
        ca = sum(1 for ri in members if eia_pm_map[ri] == "CA")
        other = [ri for ri in members if eia_pm_map[ri] not in ("CT", "CA")]
        assert not other, f"block {key} has non-CT/CA member(s): {other}"
        config = cc_configuration(ct, ca)
        block_info[key] = {"ct": ct, "ca": ca, "configuration": config}
        composition_counts[(ct, ca)] += 1

    expected_compositions = {(1, 1): 25, (2, 1): 32, (3, 1): 2}
    for comp, expected_n in expected_compositions.items():
        actual_n = composition_counts.get(comp, 0)
        assert actual_n == expected_n, (
            f"CC composition {comp}: {actual_n} blocks (expected {expected_n})"
        )
    other_blocks = sum(
        n for comp, n in composition_counts.items() if comp not in expected_compositions
    )
    assert other_blocks == 6, f"CC 'other' composition blocks: {other_blocks} (expected 6)"

    # group_index assignment: sequential per plant, in row_index order, for
    # every non-CC grouped unit ("one unit per group"); CC block members all
    # share HRSG number 1 (design doc: "HRSG number 1").
    plant_groupable = defaultdict(list)  # PlantCode -> [row_index, ...] (non-CC)
    cc_member_of = {}  # row_index -> block key, for the 183 blocked units
    for key, members in blocks.items():
        for ri in members:
            cc_member_of[ri] = key

    for ri in sorted(matched, key=lambda x: matched[x][0]["PlantCode"]):
        cat = category_map[ri]
        if cat in ("import", "sc"):
            continue
        if ri in cc_member_of:
            continue
        plant_groupable[matched[ri][0]["PlantCode"]].append(ri)

    group_index_map = {}
    for plant_code, members in plant_groupable.items():
        for idx, ri in enumerate(sorted(members), start=1):
            group_index_map[ri] = idx

    def plant_type_for(ri):
        if ri in cc_member_of:
            return "CombinedCycleBlock"
        cat = category_map[ri]
        if cat == "hydro":
            return "HydroPowerPlant"
        if cat in ("renewable", "battery"):
            return "RenewablePowerPlant"
        if cat == "thermal":
            return "ThermalPowerPlant"
        raise AssertionError(f"unexpected category for grouping: {cat} (row {ri})")

    # ---------------- Stage 4: hydro classification ----------------
    hydro_row_indices = [ri for ri in matched if category_map[ri] == "hydro"]
    assert len(hydro_row_indices) == 341, f"hydro units: {len(hydro_row_indices)} (expected 341)"
    hydro_plants = defaultdict(list)
    for ri in hydro_row_indices:
        hydro_plants[matched[ri][0]["PlantCode"]].append(ri)
    assert len(hydro_plants) == 182, f"hydro plants: {len(hydro_plants)} (expected 182)"

    hydro_eia_pms = {eia_pm_map[ri] for ri in hydro_row_indices}
    assert hydro_eia_pms <= {"HY", "PS"}, (
        f"hydro units with unexpected EIA prime mover: {hydro_eia_pms}"
    )

    plant_pmax = {
        pc: sum(float(matched[ri][0]["Pmax"]) for ri in members)
        for pc, members in hydro_plants.items()
    }
    capacity_plants = {pc for pc, mw in plant_pmax.items() if mw >= 100}
    assert len(capacity_plants) == 31, f"plants >=100MW: {len(capacity_plants)} (expected 31)"

    capacity_promoted = [
        ri for ri in hydro_row_indices if matched[ri][0]["PlantCode"] in capacity_plants
    ]
    assert len(capacity_promoted) == 91, (
        f"units promoted by capacity rule: {len(capacity_promoted)} (expected 91)"
    )
    capacity_mw = sum(float(matched[ri][0]["Pmax"]) for ri in capacity_promoted)
    assert abs(capacity_mw - 6260.1) < 0.5, (
        f"MW from capacity rule: {capacity_mw} (expected ~6260)"
    )

    ps_units = [ri for ri in hydro_row_indices if eia_pm_map[ri] == "PS"]
    assert len(ps_units) == 13, f"PS units: {len(ps_units)} (expected 13)"
    ps_sub_threshold = [
        ri for ri in ps_units if matched[ri][0]["PlantCode"] not in capacity_plants
    ]
    assert len(ps_sub_threshold) == 2, (
        f"sub-threshold PS units promoted: {len(ps_sub_threshold)} (expected 2, Lake Hodges)"
    )

    promoted = set(capacity_promoted) | set(ps_sub_threshold)
    assert len(promoted) == 93, f"total promoted: {len(promoted)} (expected 93)"

    promoted_pump_turbine = [ri for ri in promoted if eia_pm_map[ri] == "PS"]
    promoted_turbine = [ri for ri in promoted if eia_pm_map[ri] == "HY"]
    assert len(promoted_pump_turbine) == 13, len(promoted_pump_turbine)
    assert len(promoted_turbine) == 80, len(promoted_turbine)

    print("All required assertions passed.")

    # ================= write CSVs =================
    DATA_DIR.mkdir(exist_ok=True)

    write_generator_names(matched, raw_names, sanitized_names, excel_repairs)
    write_generator_prime_movers(
        matched, sanitized_names, current_pm_map, eia_pm_map, changed_map,
    )
    write_generator_plants(
        matched, sanitized_names, category_map, plant_type_for, group_index_map,
        cc_member_of, block_info,
    )
    write_hydro_units(
        matched, sanitized_names, hydro_row_indices, plant_pmax, eia_pm_map, promoted,
        capacity_plants,
    )
    write_report(
        matched=matched,
        unmatched=unmatched,
        excel_repairs=excel_repairs,
        raw_names=raw_names,
        sanitized_names=sanitized_names,
        punctuation_affected=punctuation_affected,
        current_pm_map=current_pm_map,
        eia_pm_map=eia_pm_map,
        changed_map=changed_map,
        category_map=category_map,
        plant_type_for=plant_type_for,
        group_index_map=group_index_map,
        cc_member_of=cc_member_of,
        block_info=block_info,
        hydro_row_indices=hydro_row_indices,
        plant_pmax=plant_pmax,
        promoted=promoted,
        capacity_plants=capacity_plants,
    )
    print(f"Wrote CSVs and report to {DATA_DIR}")


def write_generator_names(matched, raw_names, sanitized_names, excel_repairs):
    excel_repair_keys = {
        (r["PlantCode"].strip(), r["GenID"].strip()) for r, _, _, _, _, _ in excel_repairs
    }
    rows = []
    for row_index, (cats_row, eia_row, method) in matched.items():
        rows.append({
            "row_index": row_index,
            "old_name": f"gen-{row_index}",
            "PlantCode": cats_row["PlantCode"],
            "GenID": cats_row["GenID"],
            "new_name": sanitized_names[row_index],
            "source": method,
        })
    rows.sort(key=lambda r: r["row_index"])
    with open(DATA_DIR / "generator_names.csv", "w", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["row_index", "old_name", "PlantCode", "GenID", "new_name", "source"]
        )
        w.writeheader()
        w.writerows(rows)


def write_generator_prime_movers(matched, sanitized_names, current_pm_map, eia_pm_map, changed_map):
    rows = []
    for row_index, (cats_row, eia_row, method) in matched.items():
        rows.append({
            "row_index": row_index,
            "new_name": sanitized_names[row_index],
            "FuelType": cats_row["FuelType"],
            "current_pm": current_pm_map[row_index],
            "eia_pm": eia_pm_map[row_index],
            "changed": "true" if changed_map[row_index] else "false",
        })
    rows.sort(key=lambda r: r["row_index"])
    with open(DATA_DIR / "generator_prime_movers.csv", "w", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["row_index", "new_name", "FuelType", "current_pm", "eia_pm", "changed"]
        )
        w.writeheader()
        w.writerows(rows)


def write_generator_plants(
    matched, sanitized_names, category_map, plant_type_for, group_index_map,
    cc_member_of, block_info,
):
    # A plant can host several independent combined-cycle trains (El Segundo 1011/2021,
    # Mountainview 3/4, Pastoria PB01/PB02, Moss Landing CC1/CC2, El Centro STM2/STM3). Each
    # EIA Unit Code is its own block and commits separately, so group_index must distinguish
    # them; a constant 1 merged them and silently dropped the minority configuration.
    block_ordinal = {}
    for plant_code in sorted({k[0] for k in block_info}):
        codes = sorted(k[1] for k in block_info if k[0] == plant_code)
        for ordinal, unit_code in enumerate(codes, start=1):
            block_ordinal[(plant_code, unit_code)] = ordinal

    rows = []
    for row_index, (cats_row, eia_row, method) in matched.items():
        cat = category_map[row_index]
        if cat in ("import", "sc"):
            continue
        plant_type = plant_type_for(row_index)
        if row_index in cc_member_of:
            key = cc_member_of[row_index]
            cc_unit_code = key[1]
            cc_config = block_info[key]["configuration"]
            group_index = block_ordinal[key]
        else:
            cc_unit_code = ""
            cc_config = ""
            group_index = group_index_map[row_index]
        rows.append({
            "row_index": row_index,
            "new_name": sanitized_names[row_index],
            "PlantCode": cats_row["PlantCode"],
            "plant_name": eia_row["Plant Name"].strip(),
            "plant_type": plant_type,
            "group_index": group_index,
            "cc_unit_code": cc_unit_code,
            "cc_configuration": cc_config,
        })
    rows.sort(key=lambda r: r["row_index"])
    with open(DATA_DIR / "generator_plants.csv", "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "row_index", "new_name", "PlantCode", "plant_name", "plant_type",
                "group_index", "cc_unit_code", "cc_configuration",
            ],
        )
        w.writeheader()
        w.writerows(rows)


def write_hydro_units(
    matched, sanitized_names, hydro_row_indices, plant_pmax, eia_pm_map, promoted,
    capacity_plants,
):
    rows = []
    for row_index in hydro_row_indices:
        cats_row, eia_row, method = matched[row_index]
        pc = cats_row["PlantCode"]
        pm = eia_pm_map[row_index]
        target_type = "HydroPumpTurbine" if pm == "PS" else "HydroTurbine"
        is_promoted = row_index in promoted
        plant_mw = plant_pmax[pc]
        capacity_qualifies = pc in capacity_plants
        if capacity_qualifies:
            reason = f"plant aggregate Pmax {plant_mw:.1f} MW >= 100 MW threshold"
        elif pm == "PS":
            reason = (
                f"PS (pumped storage) unit promoted regardless of capacity; "
                f"plant aggregate Pmax {plant_mw:.1f} MW < 100 MW"
            )
        else:
            reason = (
                f"not promoted: plant aggregate Pmax {plant_mw:.1f} MW < 100 MW "
                f"and unit is not PS"
            )
        rows.append({
            "row_index": row_index,
            "new_name": sanitized_names[row_index],
            "PlantCode": pc,
            "plant_name": eia_row["Plant Name"].strip(),
            "plant_pmax_mw": round(plant_mw, 3),
            "eia_pm": pm,
            "target_type": target_type,
            "promoted": "true" if is_promoted else "false",
            "reason": reason,
            # Stage 5 (reservoirs, EHA plant-mode classification) is out of
            # scope for this script -- another agent derives it from
            # external NID/EHA/CDEC data. Left blank rather than guessed.
            "eha_plant_mode": "",
        })
    rows.sort(key=lambda r: r["row_index"])
    with open(DATA_DIR / "hydro_units.csv", "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "row_index", "new_name", "PlantCode", "plant_name", "plant_pmax_mw",
                "eia_pm", "target_type", "promoted", "reason", "eha_plant_mode",
            ],
        )
        w.writeheader()
        w.writerows(rows)


def write_report(**ctx):
    matched = ctx["matched"]
    unmatched = ctx["unmatched"]
    excel_repairs = ctx["excel_repairs"]
    raw_names = ctx["raw_names"]
    sanitized_names = ctx["sanitized_names"]
    punctuation_affected = ctx["punctuation_affected"]
    current_pm_map = ctx["current_pm_map"]
    eia_pm_map = ctx["eia_pm_map"]
    changed_map = ctx["changed_map"]
    category_map = ctx["category_map"]
    plant_type_for = ctx["plant_type_for"]
    group_index_map = ctx["group_index_map"]
    cc_member_of = ctx["cc_member_of"]
    block_info = ctx["block_info"]
    hydro_row_indices = ctx["hydro_row_indices"]
    plant_pmax = ctx["plant_pmax"]
    promoted = ctx["promoted"]
    capacity_plants = ctx["capacity_plants"]

    lines = []

    def p(s=""):
        lines.append(s)

    p("# CATS EIA enrichment audit report")
    p()
    p(
        "Generated by `Sienna/hydro_enrichment/derive_enrichment.py`. Regenerate "
        "after any change to `GIS/CATS_gens.csv`, `Archive/EIA_Generator_Y2019.csv`, "
        "or `Sienna/generator_types.jl`; do not hand-edit."
    )
    p()

    # ---------------- 1. Summary ----------------
    p("## 1. Summary")
    p()
    exact = sum(1 for _, _, m in matched.values() if m == "eia_exact")
    p(f"- CATS generator rows: {len(matched) + len(unmatched)}")
    p(f"- Exact `(PlantCode, GenID)` matches: {exact}")
    p(f"- Excel-serial repairs recovered: {len(excel_repairs)}")
    p(f"- Total matched (renamed): {len(matched)}")
    p(
        f"- Unmatched: {len(unmatched)} "
        f"({Counter(r['FuelType'] for r in unmatched)['Synchronous Condenser']} "
        f"synchronous condensers + "
        f"{Counter(r['FuelType'] for r in unmatched)['IMPORT']} imports)"
    )
    changed_count = sum(1 for v in changed_map.values() if v)
    p(f"- Prime movers changed: {changed_count} of {len(matched)} "
      f"({len(matched) - changed_count} unchanged)")
    p(f"- Combined-cycle blocks: {len(block_info)} blocks from "
      f"{len(cc_member_of)} blocked units")
    p(f"- Hydro units: {len(hydro_row_indices)} over "
      f"{len({matched[ri][0]['PlantCode'] for ri in hydro_row_indices})} plants; "
      f"{len(promoted)} promoted to reservoir-backed types")
    p()

    # ---------------- 2. Renames ----------------
    p("## 2. Renames")
    p()
    p(f"All {len(matched)} matched units are renamed `gen-N` -> `<Plant Name>_<Generator ID>`. "
      f"{punctuation_affected} names contained a comma or `#` before sanitization.")
    p()
    excel_repair_keys = {
        (r["PlantCode"].strip(), r["GenID"].strip()): (d_mmm, mmm_yy, serial)
        for r, hit, serial, d, d_mmm, mmm_yy in excel_repairs
    }
    p("### 2a. Excel-repaired units (least obvious 4 rows)")
    p()
    p("| row_index | old_name | PlantCode | GenID (serial) | decoded date | EIA Generator ID matched | new_name |")
    p("|---|---|---|---|---|---|---|")
    excel_repair_rows = sorted(
        [(ri, matched[ri]) for ri in matched if matched[ri][2] == "eia_excel_repair"],
        key=lambda x: x[0],
    )
    for ri, (cats_row, eia_row, method) in excel_repair_rows:
        key = (cats_row["PlantCode"].strip(), cats_row["GenID"].strip())
        d_mmm, mmm_yy, serial = excel_repair_keys[key]
        matched_id = eia_row["Generator ID"].strip()
        decoded = f"{d_mmm} / {mmm_yy}"
        p(
            f"| {ri} | gen-{ri} | {cats_row['PlantCode']} | {cats_row['GenID']} | "
            f"{decoded} | `{matched_id}` | {sanitized_names[ri]} |"
        )
    p()
    p("### 2b. All other renamed units")
    p()
    p("| row_index | old_name | PlantCode | GenID | new_name |")
    p("|---|---|---|---|---|")
    other_rows = sorted(
        [ri for ri in matched if matched[ri][2] == "eia_exact"],
    )
    for ri in other_rows:
        cats_row, eia_row, method = matched[ri]
        p(f"| {ri} | gen-{ri} | {cats_row['PlantCode']} | {cats_row['GenID']} | {sanitized_names[ri]} |")
    p()

    # ---------------- 3. Prime mover changes ----------------
    p("## 3. Prime mover changes")
    p()
    p(f"{changed_count} of {len(matched)} matched units change prime mover; "
      f"{len(matched) - changed_count} are unchanged (not listed).")
    p()
    p("| row_index | new_name | FuelType | current_pm | eia_pm |")
    p("|---|---|---|---|---|")
    changed_rows = sorted(ri for ri, v in changed_map.items() if v)
    for ri in changed_rows:
        cats_row, eia_row, method = matched[ri]
        p(
            f"| {ri} | {sanitized_names[ri]} | {cats_row['FuelType']} | "
            f"{current_pm_map[ri]} | {eia_pm_map[ri]} |"
        )
    p()

    # ---------------- 4. Plant grouping ----------------
    p("## 4. Plant grouping")
    p()
    p(
        "One `PowerPlant` supplemental attribute per `PlantCode`. Non-CC groups are "
        "one unit per group (own shaft/penstock/PCC); combined-cycle blocks group by "
        "EIA `Unit Code` with HRSG number 1."
    )
    p()

    plants = defaultdict(list)
    for ri in matched:
        if category_map[ri] in ("import", "sc"):
            continue
        plants[matched[ri][0]["PlantCode"]].append(ri)
    multi_unit_plants = {pc: members for pc, members in plants.items() if len(members) > 1}
    p(f"Plants with more than one grouped unit: {len(multi_unit_plants)} "
      f"(single-unit plants are counted, not listed individually).")
    p()

    p("### 4a. Combined-cycle blocks")
    p()
    p("| PlantCode | plant_name | Unit Code | composition (CT/CA) | configuration | members (row_index: name, role) |")
    p("|---|---|---|---|---|---|")
    for key in sorted(block_info):
        pc, unit_code = key
        members = [ri for ri in cc_member_of if cc_member_of[ri] == key]
        members.sort()
        plant_name = matched[members[0]][1]["Plant Name"].strip()
        info = block_info[key]
        member_str = "; ".join(
            f"{ri}: {sanitized_names[ri]} ({eia_pm_map[ri]})" for ri in members
        )
        p(
            f"| {pc} | {plant_name} | {unit_code} | {info['ct']}CT+{info['ca']}CA | "
            f"{info['configuration']} | {member_str} |"
        )
    p()

    p("### 4b. Other multi-unit plants (grouping only, not blocks)")
    p()
    p("| PlantCode | plant_name | plant_type | members (row_index: name, group_index) |")
    p("|---|---|---|---|")
    for pc in sorted(multi_unit_plants, key=lambda x: (len(multi_unit_plants[x]) == 1, x)):
        members = sorted(multi_unit_plants[pc])
        non_cc_members = [ri for ri in members if ri not in cc_member_of]
        if not non_cc_members:
            continue  # pure CC-block plant, already shown in 4a
        plant_name = matched[non_cc_members[0]][1]["Plant Name"].strip()
        ptype = plant_type_for(non_cc_members[0])
        member_str = "; ".join(
            f"{ri}: {sanitized_names[ri]} (#{group_index_map.get(ri, 'HRSG1')})"
            for ri in non_cc_members
        )
        p(f"| {pc} | {plant_name} | {ptype} | {member_str} |")
    p()

    # ---------------- 5. Hydro promotions ----------------
    p("## 5. Hydro promotions")
    p()
    p(f"All {len(hydro_row_indices)} hydro units. Selection rule: plant aggregate "
      f"Pmax >= 100 MW, OR any `PS` (pumped-storage) unit regardless of plant size.")
    p()
    p("| row_index | new_name | PlantCode | plant_name | plant_pmax_mw | eia_pm | target_type | promoted | reason |")
    p("|---|---|---|---|---|---|---|---|---|")
    for ri in sorted(hydro_row_indices):
        cats_row, eia_row, method = matched[ri]
        pc = cats_row["PlantCode"]
        pm = eia_pm_map[ri]
        target_type = "HydroPumpTurbine" if pm == "PS" else "HydroTurbine"
        is_promoted = ri in promoted
        plant_mw = plant_pmax[pc]
        if pc in capacity_plants:
            reason = f"Pmax {plant_mw:.1f} MW >= 100"
        elif pm == "PS":
            reason = f"PS unit, sub-threshold plant ({plant_mw:.1f} MW)"
        else:
            reason = f"Pmax {plant_mw:.1f} MW < 100, not PS"
        p(
            f"| {ri} | {sanitized_names[ri]} | {pc} | {eia_row['Plant Name'].strip()} | "
            f"{plant_mw:.1f} | {pm} | {target_type} | {is_promoted} | {reason} |"
        )
    p()

    # ---------------- 6. Reservoirs ----------------
    p("## 6. Reservoirs")
    p()
    p(
        "Out of scope for this script (Stage 5). Reservoir derivation from NID / "
        "EHA / CDEC data is being done concurrently in a separate task; "
        "`data/hydro_units.csv` carries an empty `eha_plant_mode` column as a "
        "placeholder for that work, not a guessed value."
    )
    p()

    # ---------------- 7. Gaps and guesses ----------------
    p("## 7. Gaps and guesses")
    p()
    p("- **`eha_plant_mode` left blank** in `data/hydro_units.csv` for all "
      f"{len(hydro_row_indices)} hydro rows -- Stage 5 (external NID/EHA/CDEC data) "
      "is out of scope for this script.")
    p("- **Batteries grouped under `RenewablePowerPlant`.** The design doc's Stage 3 "
      "table does not mention `Batteries` explicitly; `plant_attribute.jl`'s "
      "`add_supplemental_attribute!` accepts `EnergyReservoirStorage` under "
      "`RenewablePowerPlant`, so the 41 raw `Batteries` rows are grouped there. "
      "This is an interpretation, not stated outright in the design doc.")
    p("- **`generator_names.csv` and `generator_prime_movers.csv` only cover the "
      f"{len(matched)} matched rows.** The 1769 unmatched rows (synchronous "
      "condensers, imports) keep their positional `gen-N` name and never receive "
      "a `prime_mover_type` in `build_CATS.jl` regardless, so no lookup row is "
      "needed for them.")
    p("- **`generator_plants.csv` excludes synchronous condensers and imports** "
      "entirely -- neither category receives a `PowerPlant` supplemental attribute.")
    p(
        "- **Sanitization kept literal to the design doc's three rules** (strip "
        "commas, `#` -> `No`, collapse whitespace) rather than reducing names to "
        "`\\w`-only characters, since the design doc's own canonical example "
        "(`Otay Mesa Generating Project_1-Jan`) keeps a hyphen and spaces."
    )
    p()

    with open(DATA_DIR / "enrichment_report.md", "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
