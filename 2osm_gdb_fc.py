# -*- coding: utf-8 -*-
"""
Group OSM features into themed feature classes based on 'tags'.
Fix: ensure unique FC names in a single .gdb by prefixing with layer alias (points_/lines_/polygons_).
Also reuses existing FCs on re-runs instead of recreating them.
"""

import os
import re
import json
import ast
import arcpy

# ---------------- CONFIG ----------------
SOURCE_GDB    = r"C:\Users\name\Desktop\osm_batches\osm_clipped100.gdb"
POINTS_FC     = "osm_points"
LINES_FC      = "osm_lines"
POLYGONS_FC   = "osm_polygons"
LAYER_NAMES   = [POINTS_FC, LINES_FC, POLYGONS_FC]

# tags field (auto-detect if None)
TAGS_FIELD    = None
TAG_FIELD_CANDIDATES = ["tags", "other_tags", "osm_tags", "TAGS", "TAG"]

# Where to write (folder or .gdb)
OUT_ROOT      = r"C:\Users\name\Desktop\osm_batches\osm_fc100.gdb"

# Limit safety
MAX_FCS_PER_LAYER = 64

# ---------------- THEMES (edit as needed) ----------------
THEMES = {
    "Transportation_Roads": {
        "keys_any": ["highway","lanes","sidewalk","cycleway","maxspeed","oneway","traffic_sign","traffic_calming","junction","surface","tracktype","footway"],
        "key_prefixes": [],
        "key_values": {}
    },
    "Transportation_Rail": {
        "keys_any": ["railway","gauge","electrified","voltage","uic_ref"],
        "key_prefixes": ["railway:"],
        "key_values": {}
    },
    "Transportation_Transit": {
        "keys_any": ["public_transport","bus","tram","subway","stop","platform","station","train","psv"],
        "key_prefixes": ["gtfs:"],
        "key_values": {}
    },
    "Aviation_Aerialway": {
        "keys_any": ["aeroway","aircraft","airfield","heliport","aerialway"],
        "key_prefixes": ["aeroway:"],
        "key_values": {}
    },
    "Water_Maritime": {
        "keys_any": ["waterway","ferry","harbour","mooring","lock","boat"],
        "key_prefixes": ["seamark:"],
        "key_values": {}
    },
    "Buildings_Addresses": {
        "keys_any": ["building","entrance","level","addr:full"],
        "key_prefixes": ["addr:"],
        "key_values": {}
    },
    "Amenities_POI": {
        "keys_any": ["amenity","shop","tourism","leisure","office","craft","man_made","historic"],
        "key_prefixes": [],
        "key_values": {}
    },
    "Health": {
        "keys_any": ["healthcare","emergency","pharmacy"],
        "key_prefixes": [],
        "key_values": {"amenity": {"hospital","clinic","doctors","dentist","pharmacy","ambulance_station","blood_donation","nursing_home"}}
    },
    "Education": {
        "keys_any": [],
        "key_prefixes": [],
        "key_values": {"amenity": {"school","college","university","kindergarten","library","research_institute"}}
    },
    "Utilities_Power_Comms": {
        "keys_any": ["power","utility","substation"],
        "key_prefixes": ["communication:","telecom:"],
        "key_values": {}
    },
    "Waste_Recycling": {
        "keys_any": ["waste","waste_basket"],
        "key_prefixes": ["recycling:"],
        "key_values": {"amenity": {"waste_basket","recycling","waste_disposal"}}
    },
    "Parks_Nature_Landuse": {
        "keys_any": ["natural","park","pitch","playground","landuse"],
        "key_prefixes": [],
        "key_values": {"leisure": {"park","pitch","playground","garden","nature_reserve","golf_course","track","dog_park"}}
    },
    "Barriers_Access": {
        "keys_any": ["barrier","bollard","gate","access","hgv","motor_vehicle","foot","bicycle","fee"],
        "key_prefixes": ["kerb","traffic_sign:","crossing:"],
        "key_values": {}
    },
    "Admin_Boundaries": {
        "keys_any": ["boundary","admin_level","place"],
        "key_prefixes": [],
        "key_values": {}
    },
    "Names_Metadata": {
        "keys_any": ["name","name:en","old_name","loc_name","alt_name","wikidata","wikipedia","source","source:name","note","fixme","ref","start_date","operator","brand"],
        "key_prefixes": ["name:"],
        "key_values": {}
    }
}
FALLBACK_THEME = "Unclassified"
# ---------------------------------------------------------

arcpy.env.overwriteOutput = True

def log(msg): print(msg)

def ensure_dir(p):
    if not os.path.isdir(p):
        os.makedirs(p, exist_ok=True)

def is_gdb(path): return path.lower().endswith(".gdb")

def ensure_gdb(path):
    folder, name = os.path.split(path)
    ensure_dir(folder)
    if not arcpy.Exists(path):
        arcpy.management.CreateFileGDB(folder, name)

def ensure_feature_dataset(gdb_path, fd_name, spatial_ref):
    fd_path = os.path.join(gdb_path, fd_name)
    if not arcpy.Exists(fd_path):
        arcpy.management.CreateFeatureDataset(gdb_path, fd_name, spatial_ref)
    return fd_path

def sanitize_name(s, limit=64):
    s = str(s).strip()
    s = re.sub(r"[^\w\-\.]+", "_", s)
    return (s or "name")[:limit]

def detect_tag_field(fc):
    if TAGS_FIELD:
        return TAGS_FIELD
    existing = {f.name.lower(): f.name for f in arcpy.ListFields(fc)}
    for cand in TAG_FIELD_CANDIDATES:
        if cand.lower() in existing:
            return existing[cand.lower()]
    return None

def parse_tags(raw):
    if raw is None: return {}
    txt = str(raw).strip()
    if not txt or txt.lower() in ("null","none","nan"): return {}
    # JSON
    try:
        obj = json.loads(txt)
        if isinstance(obj, dict):
            return {str(k): ("" if v is None else str(v)) for k, v in obj.items()}
    except Exception:
        pass
    # Python dict literal
    try:
        obj = ast.literal_eval(txt)
        if isinstance(obj, dict):
            return {str(k): ("" if v is None else str(v)) for k, v in obj.items()}
    except Exception:
        pass
    # Delimited fallback
    out = {}
    for t in re.split(r"[;,]\s*", txt):
        if not t: continue
        if "=" in t:
            k, v = t.split("=", 1)
        elif ":" in t and "://" not in t:
            k, v = t.split(":", 1)
        else:
            k, v = t, "true"
        k = k.strip()
        v = "" if v is None else str(v).strip()
        if k: out[k] = v
    return out

def field_list_for_cursor(fc):
    keep = []
    for f in arcpy.ListFields(fc):
        if f.type in ("OID","Geometry"): continue
        keep.append(f.name)
    return ["SHAPE@"] + keep

def get_geom_type(fc): return getattr(arcpy.Describe(fc), "shapeType", "UNKNOWN")

def clone_schema(container, out_name, template_fc):
    """Create FC if missing; else just return existing path."""
    out_path = os.path.join(container, out_name)
    if arcpy.Exists(out_path):
        return out_path
    geom = get_geom_type(template_fc)
    sr = arcpy.Describe(template_fc).spatialReference
    arcpy.management.CreateFeatureclass(
        out_path=container,
        out_name=out_name,
        geometry_type=geom,
        template=template_fc,
        has_m="SAME_AS_TEMPLATE",
        has_z="SAME_AS_TEMPLATE",
        spatial_reference=sr
    )
    return out_path

def classify_themes(tag_dict):
    found = set()
    keys = set(tag_dict.keys())
    for theme, rule in THEMES.items():
        if any(k in keys for k in rule.get("keys_any", [])):
            found.add(theme); continue
        kp = rule.get("key_prefixes", [])
        if kp and any(any(k.startswith(pfx) for pfx in kp) for k in keys):
            found.add(theme); continue
        kv = rule.get("key_values", {})
        hit = False
        for k, allowed in kv.items():
            if k in tag_dict:
                val = str(tag_dict.get(k, ""))
                if isinstance(allowed, (set, list, tuple)):
                    if val in set(map(str, allowed)):
                        hit = True; break
                elif allowed == "*":
                    hit = True; break
        if hit:
            found.add(theme); continue
    if not found:
        found.add(FALLBACK_THEME)
    return found

def split_layer_by_themes(src_fc, out_container, layer_alias, single_gdb_mode):
    tag_field = detect_tag_field(src_fc)
    if not tag_field:
        log(f"   ! No tag field found in {src_fc}; skipping.")
        return 0

    read_fields  = field_list_for_cursor(src_fc)
    if tag_field not in read_fields: read_fields.append(tag_field)
    write_fields = field_list_for_cursor(src_fc)
    tags_idx = read_fields.index(tag_field)

    total = int(arcpy.management.GetCount(src_fc)[0])
    log(f"   Features: {total} (tags='{tag_field}')")

    out_fc_paths = {}  # theme_name_with_prefix -> path
    created = 0

    with arcpy.da.SearchCursor(src_fc, read_fields) as sc:
        for i, row in enumerate(sc, start=1):
            geom  = row[0]
            attrs = row[1:len(write_fields)]
            td    = parse_tags(row[tags_idx])

            cats = classify_themes(td) if td else {FALLBACK_THEME}

            for theme in cats:
                # IMPORTANT: in a single .gdb, feature class names must be unique across the GDB.
                # Prefix with the layer alias to avoid collisions: e.g., points_Water_Maritime
                base_name = theme
                fc_name = f"{layer_alias}_{base_name}" if single_gdb_mode else base_name
                fc_name = sanitize_name(fc_name)

                if fc_name not in out_fc_paths:
                    if len(out_fc_paths) >= MAX_FCS_PER_LAYER:
                        raise RuntimeError(
                            f"Exceeded MAX_FCS_PER_LAYER={MAX_FCS_PER_LAYER}. "
                            f"Increase the limit or reduce THEMES."
                        )
                    path = clone_schema(out_container, fc_name, src_fc)
                    out_fc_paths[fc_name] = path
                    created += 1

                ic = arcpy.da.InsertCursor(out_fc_paths[fc_name], write_fields)
                ic.insertRow((geom,) + tuple(attrs))
                del ic

            if i % 5000 == 0:
                log(f"   ... processed {i}/{total}")

    log(f"   Created {created} themed FCs under {out_container}")
    return created

def main():
    if not arcpy.Exists(SOURCE_GDB):
        log(f"Source GDB not found: {SOURCE_GDB}")
        return

    single_gdb = is_gdb(OUT_ROOT)
    if single_gdb:
        if not arcpy.Exists(OUT_ROOT):
            parent, name = os.path.split(OUT_ROOT)
            ensure_dir(parent)
            arcpy.management.CreateFileGDB(parent, name)
        log(f"Output mode: single GDB -> {OUT_ROOT}")
    else:
        ensure_dir(OUT_ROOT)
        log(f"Output mode: per-layer GDBs under folder -> {OUT_ROOT}")

    containers = {}
    layer_alias_map = {
        POINTS_FC: "points",
        LINES_FC: "lines",
        POLYGONS_FC: "polygons"
    }

    for lyr in LAYER_NAMES:
        if not lyr: continue
        src_fc = os.path.join(SOURCE_GDB, lyr)
        if not arcpy.Exists(src_fc):
            log(f"(skip) missing layer: {src_fc}")
            continue

        if single_gdb:
            sr = arcpy.Describe(src_fc).spatialReference
            fd_name = sanitize_name(f"{lyr}_THEMED")
            fd_path = ensure_feature_dataset(OUT_ROOT, fd_name, sr)
            containers[lyr] = (fd_path, layer_alias_map.get(lyr, sanitize_name(lyr.lower())))
        else:
            gdb_path = os.path.join(OUT_ROOT, sanitize_name(f"{lyr}_THEMED.gdb"))
            ensure_gdb(gdb_path)
            containers[lyr] = (gdb_path, layer_alias_map.get(lyr, sanitize_name(lyr.lower())))

    grand_total = 0
    for lyr, (container, alias) in containers.items():
        src_fc = os.path.join(SOURCE_GDB, lyr)
        log(f"\n=== Theming layer: {src_fc}")
        made = split_layer_by_themes(src_fc, container, alias, single_gdb)
        grand_total += made

    log("\nDone.")
    log(f"Total themed FCs created: {grand_total}")
    if single_gdb:
        log(f"Outputs written into: {OUT_ROOT}")
    else:
        log(f"Outputs written under folder: {OUT_ROOT}")

if __name__ == "__main__":
    main()
