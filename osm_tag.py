# -*- coding: utf-8 -*-
"""
List all OSM tags found in a file geodatabase and write them to a .txt report.

- Scans the three common OSM layers: points / lines / polygons (if present).
- Auto-detects the tags field (tries: 'tags', 'other_tags', 'osm_tags'; configurable).
- Robust tag parsing: JSON, Python-dict-like, or "key=value;key2=value2" strings.
- Two output modes:
    MODE = "KEYS"       -> unique tag keys with counts (e.g., highway, building)
    MODE = "KEY_VALUES" -> unique key=value pairs with counts (e.g., highway=residential)

Output:
- A human-readable text file with per-layer counts and an overall summary.

Dependencies: ArcGIS Pro / ArcPy (no external packages)
"""

import os
import re
import json
import ast
import arcpy
from collections import Counter

# ---------------- CONFIG ----------------
GDB_PATH      = r"C:\Users\name\Desktop\osm_batches\osm_clipped100.gdb"
POINTS_FC     = "osm_points"
LINES_FC      = "osm_lines"
POLYGONS_FC   = "osm_polygons"

# Candidate field names to search for (first one found will be used)
TAG_FIELD_CANDIDATES = ["tags", "other_tags", "osm_tags", "TAG", "TAGS"]

# "KEYS" or "KEY_VALUES"
MODE          = "KEYS"

# Where to write the text report
OUT_TXT       = r"C:\Users\name\Desktop\osm_tag_inventory.txt"

# Optional: limit output list length per section (None = unlimited)
TOP_N         = None  # e.g., 1000
# ---------------------------------------

arcpy.env.overwriteOutput = True

def log(msg): print(msg)

def detect_tag_field(fc, candidates):
    """Return the first matching tag field name in fc, or None."""
    existing = {f.name.lower(): f.name for f in arcpy.ListFields(fc)}
    for c in candidates:
        if c.lower() in existing:
            return existing[c.lower()]
    return None

def parse_tags(raw):
    """
    Parse a tags cell into dict {key: value}.
    Handles JSON, Python-literal dicts, and delimited "k=v;k2=v2" strings.
    """
    if raw is None:
        return {}
    txt = str(raw).strip()
    if not txt or txt.lower() in ("null", "none", "nan"):
        return {}

    # Try JSON
    try:
        obj = json.loads(txt)
        if isinstance(obj, dict):
            return {str(k): ("" if v is None else str(v)) for k, v in obj.items()}
    except Exception:
        pass

    # Try Python-literal dict
    try:
        obj = ast.literal_eval(txt)
        if isinstance(obj, dict):
            return {str(k): ("" if v is None else str(v)) for k, v in obj.items()}
    except Exception:
        pass

    # Fallback: delimited "k=v;k2=v2" or "k:v"
    out = {}
    tokens = re.split(r"[;,]\s*", txt)
    for t in tokens:
        if not t:
            continue
        if "=" in t:
            k, v = t.split("=", 1)
        elif ":" in t and "://" not in t:
            k, v = t.split(":", 1)
        else:
            # bare key -> treat as boolean true
            k, v = t, "true"
        k = k.strip()
        v = "" if v is None else str(v).strip()
        if k:
            out[str(k)] = v
    return out

def count_tags_in_layer(fc_path, mode, tag_field_name):
    """
    Return a Counter of keys or key=values for a single layer.
    """
    counter = Counter()
    fields = [tag_field_name]
    with arcpy.da.SearchCursor(fc_path, fields) as sc:
        for (tags_raw,) in sc:
            td = parse_tags(tags_raw)
            if not td:
                continue
            if mode.upper() == "KEY_VALUES":
                counter.update([f"{k}={v}" for k, v in td.items()])
            else:
                counter.update(td.keys())
    return counter

def write_counter_section(fh, title, counter, top_n=None):
    fh.write(f"\n## {title}\n")
    total_unique = len(counter)
    total_count  = sum(counter.values())
    fh.write(f"Unique items: {total_unique}\n")
    fh.write(f"Total occurrences: {total_count}\n")
    if total_unique == 0:
        return
    fh.write("Items (sorted by frequency desc, then name):\n")
    items = counter.most_common()
    if top_n is not None:
        items = items[:top_n]
    for k, c in items:
        fh.write(f"  {k}\t{c}\n")

def main():
    if not arcpy.Exists(GDB_PATH):
        log(f"ERROR: GDB not found: {GDB_PATH}")
        return

    layers = []
    for name in (POINTS_FC, LINES_FC, POLYGONS_FC):
        if not name:
            continue
        fc = os.path.join(GDB_PATH, name)
        if arcpy.Exists(fc):
            layers.append(fc)
        else:
            log(f"(skip) not found: {fc}")

    if not layers:
        log("No layers found; nothing to do.")
        return

    overall = Counter()
    per_layer_results = []

    # Scan each layer
    for fc in layers:
        # Detect tag field
        tag_field = detect_tag_field(fc, TAG_FIELD_CANDIDATES)
        if not tag_field:
            log(f"WARNING: No tag field found in {fc}. Checked {TAG_FIELD_CANDIDATES}. Skipping layer.")
            per_layer_results.append((fc, None, Counter()))
            continue

        count = int(arcpy.management.GetCount(fc)[0])
        log(f"Scanning: {fc}  (features: {count}, tags field: {tag_field})")
        c = count_tags_in_layer(fc, MODE, tag_field)
        per_layer_results.append((fc, tag_field, c))
        overall.update(c)

    # Write report
    out_folder = os.path.dirname(OUT_TXT)
    if out_folder and not os.path.isdir(out_folder):
        os.makedirs(out_folder, exist_ok=True)

    with open(OUT_TXT, "w", encoding="utf-8") as fh:
        fh.write("OSM Tag Inventory Report\n")
        fh.write("========================\n")
        fh.write(f"GDB: {GDB_PATH}\n")
        fh.write(f"Mode: {MODE}\n")
        fh.write(f"Layers scanned: {len(layers)}\n")
        fh.write(f"Candidate tag fields: {TAG_FIELD_CANDIDATES}\n")
        if TOP_N is not None:
            fh.write(f"Top-N limit per section: {TOP_N}\n")
        fh.write("\n")

        # Per-layer sections
        for fc, tag_field, counter in per_layer_results:
            title = f"Layer: {fc}  (tag field: {tag_field if tag_field else 'N/A'})"
            write_counter_section(fh, title, counter, top_n=TOP_N)

        # Overall summary
        write_counter_section(fh, "OVERALL (all layers combined)", overall, top_n=TOP_N)

    log(f"Done. Wrote: {OUT_TXT}")

if __name__ == "__main__":
    main()
