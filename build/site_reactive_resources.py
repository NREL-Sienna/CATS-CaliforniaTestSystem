"""Site CAISO Appendix A reactive resources onto CATS buses."""
import csv, json, math

BASE = "/home/jdlara/CATS/CATS-CaliforniaTestSystem"

# CAISO Board-Approved 2025-2026 Transmission Plan, Appendix A, section 3.
# (substation, geojson name, county, technology, MVAr, n_units, target_kV)
#
# `technology` is what Appendix A says; COMPONENT_TYPE below is what we actually build.
# StaticVArCompensator is modelled as a SynchronousCondenser: both give continuously variable
# dynamic reactive support, and CATS has no SVC representation.
COMPONENT_TYPE = {
    "ShuntCapacitor": "FixedAdmittance",
    "SynchronousCondenser": "SynchronousCondenser",
    "StaticVArCompensator": "SynchronousCondenser",
}
NAME_PREFIX = {
    "ShuntCapacitor": "ShuntCapacitor",
    "SynchronousCondenser": "SynchronousCondenser",
    "StaticVArCompensator": "SVC",
}

DEVICES = [
    ("Gates",            "Gates",              "Fresno",          "ShuntCapacitor", 225.0, 1, 500),
    ("Los Banos",        "Los Banos",          "Merced",          "ShuntCapacitor", 225.0, 1, 500),
    ("Gregg",            "Gregg",              "Madera",          "ShuntCapacitor", 150.0, 1, 230),
    ("McCall",           "Mc Call",            "Fresno",          "ShuntCapacitor", 132.0, 1, 230),
    ("Mesa (PG&E)",      "Mesa",               "San Luis Obispo", "ShuntCapacitor", 100.0, 1, 230),
    ("Metcalf",          "Metcalf 2",          "Santa Clara",     "ShuntCapacitor", 350.0, 1, 500),
    ("Olinda",           "Olinda (Vic Fazio)", "Shasta",          "ShuntCapacitor", 200.0, 1, 500),
    ("Table Mountain",   "Table Mt.",          "Butte",           "ShuntCapacitor", 454.0, 1, 500),
    ("Mira Loma 230kV",  "Mira Loma",          "San Bernardino",  "ShuntCapacitor", 158.0, 1, 230),
    ("Mira Loma 500kV",  "Mira Loma",          "San Bernardino",  "ShuntCapacitor", 300.0, 1, 500),
    ("Mesa 500/230 kV",  "Mesa",               "Los Angeles",     "ShuntCapacitor", 405.0, 1, 500),
    ("San Luis Rey",     "San Luis Rey",       "San Diego",       "ShuntCapacitor",  63.0, 1, 230),
    ("Bay Boulevard",    "South Bay",          "San Diego",       "ShuntCapacitor", 100.0, 1, 230),
    ("Miguel",           "Miguel",             "San Diego",       "ShuntCapacitor", 126.0, 1, 230),
    ("Escondido",        "Escondido",          "San Diego",       "ShuntCapacitor", 126.0, 1, 230),
    ("Suncrest",         "Suncrest",           "San Diego",       "ShuntCapacitor", 126.0, 1, 230),
    ("Capistrano",       "Capistrano",         "Orange",          "ShuntCapacitor", 150.0, 1, 138),
    ("Penasquitos",      "Penasquitos",        "San Diego",       "ShuntCapacitor", 276.0, 1, 230),
    ("Santiago",         "Santiago",           "Orange",          "SynchronousCondenser",  81.0, 3, 230),
    ("San Luis Rey",     "San Luis Rey",       "San Diego",       "SynchronousCondenser", 225.0, 2, 230),
    ("Talega",           "Talega",             "San Diego",       "SynchronousCondenser", 225.0, 2, 138),
    ("Miguel",           "Miguel",             "San Diego",       "SynchronousCondenser", 225.0, 2, 230),
    ("San Onofre",       "San Onofre",         "San Diego",       "SynchronousCondenser", 225.0, 1, 230),
    # Appendix A gives Devers as "156 & 605 (dynamic capability)"; 605 is the dynamic figure and
    # the only part a condenser stand-in represents. The 156 MVAr fixed portion is not modelled.
    ("Devers",           "Devers",             "Riverside",       "StaticVArCompensator", 605.0, 1, 500),
    ("Rector",           "Rector",             "Tulare",          "StaticVArCompensator", 200.0, 1, 230),
    ("Suncrest",         "Suncrest",           "San Diego",       "StaticVArCompensator", 300.0, 1, 230),
]


def webmerc_to_wgs84(x, y):
    lon = x / 20037508.34 * 180.0
    lat = y / 20037508.34 * 180.0
    lat = 180.0 / math.pi * (2.0 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
    return lat, lon


def haversine(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


subs = {}
for f in json.load(open(f"{BASE}/Archive/substations.geojson"))["features"]:
    p = f["properties"]
    name = (p.get("Substati_1") or "").strip()
    county = (p.get("County") or "").strip()
    if not name:
        continue
    lat, lon = webmerc_to_wgs84(*f["geometry"]["coordinates"])
    subs[(name, county)] = (lat, lon, (p.get("Owner") or "").strip())

buses = []
for r in csv.DictReader(open(f"{BASE}/GIS/CATS_buses.csv")):
    buses.append((int(r["bus_i"]), float(r["kV"]), float(r["Lat"]), float(r["Lon"])))

kvs = sorted({b[1] for b in buses})
print("distinct CATS bus kV levels:", kvs)
print()

out = []
for label, gname, county, tech, mvar, n, kv in DEVICES:
    key = (gname, county)
    if key not in subs:
        print(f"  !! substation not found: {gname} / {county}")
        continue
    slat, slon, owner = subs[key]
    # exact kV first; fall back to the nearest available level above 100 kV
    cand = [b for b in buses if abs(b[1] - kv) < 1e-6]
    used_kv = kv
    if not cand:
        alt = min((x for x in kvs if x >= 100), key=lambda x: abs(x - kv))
        cand = [b for b in buses if abs(b[1] - alt) < 1e-6]
        used_kv = alt
    bus, bkv, blat, blon = min(cand, key=lambda b: haversine(slat, slon, b[2], b[3]))
    dist = haversine(slat, slon, blat, blon)
    out.append((label, gname, county, tech, COMPONENT_TYPE[tech], NAME_PREFIX[tech],
                mvar, n, kv, used_kv, bus, round(dist, 2), owner))
    flag = "" if dist <= 25 else "  <-- FAR"
    kvflag = "" if used_kv == kv else f" (no {kv}kV bus; used {used_kv:g})"
    print(f"  {label:18s} {tech:22s} {n}x{mvar:6.1f} MVAr -> bus {bus:5d} @ {used_kv:5.1f}kV  {dist:6.2f} km{kvflag}{flag}")

with open(f"{BASE}/data/reactive_resources.csv", "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["substation", "geojson_name", "county", "technology", "component_type",
                "name_prefix", "mvar_per_unit", "n_units", "appendix_kv", "sited_kv", "bus",
                "siting_distance_km", "owner"])
    w.writerows(out)


def total(tech):
    return (sum(n for o in out for t, m, n in [(o[3], o[6], o[7])] if t == tech),
            sum(m * n for o in out for t, m, n in [(o[3], o[6], o[7])] if t == tech))


print()
for tech in ("ShuntCapacitor", "SynchronousCondenser", "StaticVArCompensator"):
    n, mvar = total(tech)
    print(f"  {tech:22s}: {n:3d} units  {mvar:7,.0f} MVAr")
dyn = total("SynchronousCondenser")[1] + total("StaticVArCompensator")[1]
print(f"  {'dynamic total':22s}: {dyn:12,.0f} MVAr")
print(f"  wrote data/reactive_resources.csv ({len(out)} rows)")
