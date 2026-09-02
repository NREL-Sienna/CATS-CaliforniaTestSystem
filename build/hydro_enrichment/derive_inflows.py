#!/usr/bin/env python3
"""Derive 2019 hydro reservoir inflow data for the 32 CATS California hydro reservoirs.

Reads cached raw downloads (already fetched by this script's companion fetch step,
cached under data/sources/cdec/ and data/sources/usgs/, gitignored):
  - data/sources/cdec/inflow_<STATION>_2019.csv   CDEC CSVDataServlet, sensor 76
    (RESERVOIR INFLOW, CFS, daily), for the 11 stations confirmed to have full-year
    2019 coverage (see data/sources/cdec/meta_<STATION>.html sensor tables).
  - data/sources/usgs/usgs_<SITE>_2019.rdb        USGS NWIS dv service, parameter
    00060 (discharge, cfs, daily mean), for the 3 sites confirmed to have full-year
    2019 coverage and a defensible river/reservoir match.

Writes:
  - data/hydro_inflows.csv               one row per plant (32), summary statistics
  - data/hydro_inflow_2019_daily.csv     long-format daily series, only for plants
    with real data (14 of 32)

No value here is invented, estimated, or interpolated. A plant with no confirmed
inflow/discharge gauge gets empty statistics and an explicit note in both outputs.
CDEC values with the '---' missing-data flag are dropped, not filled.

Run from anywhere: `python3 Sienna/hydro_enrichment/derive_inflows.py`
"""
import csv
import statistics
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = REPO_ROOT / "data"
CDEC_DIR = DATA_DIR / "sources" / "cdec"
USGS_DIR = DATA_DIR / "sources" / "usgs"

CFS_TO_M3S = 0.0283168466

HYDRO_INFLOWS_CSV = DATA_DIR / "hydro_inflows.csv"
HYDRO_INFLOW_DAILY_CSV = DATA_DIR / "hydro_inflow_2019_daily.csv"

# reservoir_name must match data/hydro_reservoirs.csv exactly:
# "Reservoir_" + plant_name, commas stripped, "#" -> "No", whitespace collapsed.
# None of the 32 plant names in this set contain a comma or "#", so this is a
# straight prefix -- verified against data/hydro_reservoirs.csv at load time below.


def reservoir_name(plant_name):
    return f"Reservoir_{plant_name}"


# --- CDEC stations: sensor 76 (RESERVOIR INFLOW, CFS), confirmed full-year 2019
# coverage from data/sources/cdec/meta_<STATION>.html. Sensor 76 exists at station
# but with NO 2019 data (checked directly against CSVDataServlet, all '---' or
# empty): SNL (ends 1997), HTH (all missing 2019), JNC (no sensor 76 at all).
# No flow/inflow sensor of any kind at station: MPL, IRC, CMI, CHV/CHY, WSN, HHL,
# BTV, SHV, HDG.
CDEC_SOURCES = {
    "445": ("Shasta", "SHA"),
    "437": ("Edward C Hyatt", "ORO"),
    "454": ("Colgate", "BUL"),
    "6158": ("New Melones", "NML"),
    "441": ("Folsom", "FOL"),
    "450": ("Spring Creek", "WHI"),
    "439": ("Don Pedro", "DNP"),
    "7907": ("Pine Flat", "PNF"),
    "442": ("Judge F Carr", "LEW"),
    "451": ("Trinity", "CLE"),
    "443": ("Keswick", "KES"),
}

# --- USGS NWIS sites: parameter 00060 (discharge, cfs), daily mean. Each is the
# river gauge judged closest to a genuine upstream-inflow match for the named
# reservoir; see notes per plant for the specific caveat.
USGS_SOURCES = {
    "382": (
        "R C Kirkwood", "11274790",
        "TUOLUMNE R A GRAND CYN OF TUOLUMNE AB HETCH HETCHY -- explicitly "
        "'above Hetch Hetchy', standard long-term primary-inflow gauge for "
        "Hetch Hetchy Reservoir (O'Shaughnessy Dam); drainage area 3813.65 sq mi.",
    ),
    "447": (
        "Parker Dam", "09423000",
        "COLORADO RIVER BELOW DAVIS DAM, AZ-NV -- approximation, not immediately "
        "upstream: ~150 river miles above Parker Dam/Lake Havasu via the Bullhead "
        "City/Topock reach; Bill Williams River joins in between (usually minor). "
        "Used because Parker Dam has no dedicated inflow gauge and the Colorado "
        "is fully regulated by Hoover/Davis releases in this reach.",
    ),
    "322": (
        "Big Creek 2A", "11241500",
        "STEVENSON C A SHAVER LK CA -- natural-inflow gauge for Shaver Lake "
        "(Big Creek 2A's reservoir); drainage area only 29.4 sq mi, so it likely "
        "does not capture water imported into Shaver Lake via the Big Creek "
        "powerhouse tunnel system from the upper Big Creek watershed.",
    ),
}


def load_cdec_daily(station):
    path = CDEC_DIR / f"inflow_{station}_2019.csv"
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            date_str = r["OBS DATE"].strip()[:8]
            date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
            val = r["VALUE"].strip()
            if val in ("---", "", "m", "ART"):
                continue
            try:
                cfs = float(val)
            except ValueError:
                continue
            if cfs < 0:
                continue  # CDEC computed-inflow noise artifact, not a real negative flow
            rows.append((date, round(cfs * CFS_TO_M3S, 3)))
    return rows


def count_cdec_excluded(station):
    """(n_missing, n_negative) -- both dropped from the daily series, counted
    separately here only so the summary notes can say so explicitly."""
    path = CDEC_DIR / f"inflow_{station}_2019.csv"
    n_missing = 0
    n_negative = 0
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            val = r["VALUE"].strip()
            if val in ("---", "", "m", "ART"):
                n_missing += 1
                continue
            try:
                cfs = float(val)
            except ValueError:
                n_missing += 1
                continue
            if cfs < 0:
                n_negative += 1
    return n_missing, n_negative


def load_usgs_daily(site):
    path = USGS_DIR / f"usgs_{site}_2019.rdb"
    rows = []
    with open(path, newline="") as f:
        header = None
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split("\t")
            if header is None:
                header = parts
                continue
            if parts[0] == "5s" or parts[2] == "20d":
                continue  # rdb format-spec row
            if parts[0] != "USGS":
                continue
            date = parts[2].strip()
            # mean-daily column is always the LAST _00060_0000N (no _cd suffix)
            # value column that isn't a qualification code; identify by header.
            mean_col_idx = None
            for i, colname in enumerate(header):
                if colname.endswith("_00003") and not colname.endswith("_cd"):
                    mean_col_idx = i
            if mean_col_idx is None or mean_col_idx >= len(parts):
                continue
            val = parts[mean_col_idx].strip()
            if not val:
                continue
            try:
                cfs = float(val)
            except ValueError:
                continue
            if cfs < 0:
                continue
            rows.append((date, round(cfs * CFS_TO_M3S, 3)))
    return rows


def notes_for_cdec(n_present, station):
    n_missing, n_negative = count_cdec_excluded(station)
    parts = []
    if n_present == 365:
        parts.append("full 365-day 2019 series")
    else:
        parts.append(f"{n_present} of 365 days present")
    if n_missing:
        parts.append(f"{n_missing} day(s) missing ('---' in CDEC feed)")
    if n_negative:
        parts.append(
            f"{n_negative} day(s) excluded for negative computed inflow "
            "(CDEC mass-balance noise artifact, not a real reverse flow)"
        )
    return "; ".join(parts)


def summarize(values):
    if not values:
        return None
    return {
        "mean": round(statistics.mean(values), 3),
        "median": round(statistics.median(values), 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
        "n": len(values),
    }


def main():
    reservoirs_csv = DATA_DIR / "hydro_reservoirs.csv"
    with open(reservoirs_csv, newline="") as f:
        known_reservoir_names = {r["reservoir_name"] for r in csv.DictReader(f)}
        known_plant_codes = {}
        with open(reservoirs_csv, newline="") as f2:
            for r in csv.DictReader(f2):
                known_plant_codes[r["eia_plant_code"]] = (
                    r["plant_name"], r["reservoir_name"]
                )
    assert len(known_plant_codes) == 32, (
        f"expected 32 plants in hydro_reservoirs.csv, got {len(known_plant_codes)}"
    )

    daily_rows = []  # (reservoir_name, date, inflow_m3s)
    summary_rows = []

    handled_codes = set()

    for pc, (plant_name_check, station) in CDEC_SOURCES.items():
        plant_name, resv_name = known_plant_codes[pc]
        assert plant_name == plant_name_check, (pc, plant_name, plant_name_check)
        assert resv_name in known_reservoir_names
        daily = load_cdec_daily(station)
        values = [v for _, v in daily]
        stats = summarize(values)
        for date, v in daily:
            daily_rows.append((resv_name, date, v))
        summary_rows.append({
            "eia_plant_code": pc,
            "plant_name": plant_name,
            "reservoir_name": resv_name,
            "mean_inflow_m3s": stats["mean"],
            "median_inflow_m3s": stats["median"],
            "min_inflow_m3s": stats["min"],
            "max_inflow_m3s": stats["max"],
            "n_days_2019": stats["n"],
            "inflow_source": f"CDEC station {station}, sensor 76 (RESERVOIR INFLOW, CFS), CSVDataServlet, daily, 2019-01-01 to 2019-12-31",
            "source_station_id": station,
            "confidence": "high",
            "notes": notes_for_cdec(stats["n"], station),
        })
        handled_codes.add(pc)

    for pc, (plant_name_check, site, note) in USGS_SOURCES.items():
        plant_name, resv_name = known_plant_codes[pc]
        assert plant_name == plant_name_check, (pc, plant_name, plant_name_check)
        assert resv_name in known_reservoir_names
        daily = load_usgs_daily(site)
        values = [v for _, v in daily]
        stats = summarize(values)
        for date, v in daily:
            daily_rows.append((resv_name, date, v))
        summary_rows.append({
            "eia_plant_code": pc,
            "plant_name": plant_name,
            "reservoir_name": resv_name,
            "mean_inflow_m3s": stats["mean"],
            "median_inflow_m3s": stats["median"],
            "min_inflow_m3s": stats["min"],
            "max_inflow_m3s": stats["max"],
            "n_days_2019": stats["n"],
            "inflow_source": f"USGS NWIS site {site}, parameter 00060 (discharge, mean daily, CFS), dv service, 2019-01-01 to 2019-12-31",
            "source_station_id": f"USGS-{site}",
            "confidence": "medium",
            "notes": note + (
                f" {stats['n']} of 365 days present." if stats["n"] < 365 else " Full 365-day 2019 series."
            ),
        })
        handled_codes.add(pc)

    for pc, (plant_name, resv_name) in known_plant_codes.items():
        if pc in handled_codes:
            continue
        summary_rows.append({
            "eia_plant_code": pc,
            "plant_name": plant_name,
            "reservoir_name": resv_name,
            "mean_inflow_m3s": "",
            "median_inflow_m3s": "",
            "min_inflow_m3s": "",
            "max_inflow_m3s": "",
            "n_days_2019": 0,
            "inflow_source": "",
            "source_station_id": "",
            "confidence": "none",
            "notes": "No confirmed inflow gauge found (CDEC: no inflow/full-natural-flow sensor at any candidate station, or sensor exists but reports no 2019 data; USGS: no gauge identified that unambiguously measures inflow to this reservoir rather than an engineered release, tunnel diversion, or downstream tailrace). Not estimated, not interpolated.",
        })
        handled_codes.add(pc)

    assert len(handled_codes) == 32

    summary_rows.sort(key=lambda r: int(r["eia_plant_code"]))
    daily_rows.sort(key=lambda r: (r[0], r[1]))

    fieldnames = [
        "eia_plant_code", "plant_name", "reservoir_name",
        "mean_inflow_m3s", "median_inflow_m3s", "min_inflow_m3s", "max_inflow_m3s",
        "n_days_2019", "inflow_source", "source_station_id", "confidence", "notes",
    ]
    with open(HYDRO_INFLOWS_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in summary_rows:
            w.writerow(r)

    with open(HYDRO_INFLOW_DAILY_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["reservoir_name", "date", "inflow_m3s"])
        for resv_name, date, v in daily_rows:
            w.writerow([resv_name, date, v])

    n_with_data = sum(1 for r in summary_rows if r["mean_inflow_m3s"] != "")
    print(f"Wrote {HYDRO_INFLOWS_CSV} (32 rows, {n_with_data} with real inflow data)")
    print(f"Wrote {HYDRO_INFLOW_DAILY_CSV} ({len(daily_rows)} daily rows across {n_with_data} reservoirs)")


if __name__ == "__main__":
    main()
