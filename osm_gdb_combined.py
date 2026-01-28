# -*- coding: utf-8 -*-
"""
Combine multiple per-batch FileGDBs into one master GDB.
ArcPy-only, no external deps.

How it works:
- Finds all input GDBs matching a pattern (or list).
- For each, appends feature classes "osm_points", "osm_lines", "osm_polygons"
  into a single master GDB (creates if missing).
- Preserves fields: osmid (TEXT), tags (TEXT).
"""

import os
import glob
import arcpy

# ---------------- CONFIG ----------------
# Either: point to a parent folder & pattern...
INPUT_PARENT_FOLDER = r"C:\Users\name\Desktop\osm_batches"
GDB_GLOB_PATTERN    = "osm_fc*.gdb"   # e.g., osm_clipped_batch_001.gdb, etc.

# ...or explicitly list your GDBs (uncomment to use)
# INPUT_GDBS = [
#     r"C:\Users\diaznd\Desktop\osm_clipped_batch_001.gdb",
#     r"C:\Users\diaznd\Desktop\osm_clipped_batch_002.gdb",
# ]

# Output master GDB:
OUTPUT_GDB = r"C:\Users\name\Desktop\osm_combined.gdb"

# Feature class names expected in each input GDB:
FEATURES = ["osm_points", "osm_lines", "osm_polygons"]
# ---------------------------------------


def log(m):
    print(m)

def ensure_gdb(path):
    folder, name = os.path.split(path)
    if folder and not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True)
    if not arcpy.Exists(path):
        arcpy.CreateFileGDB_management(folder, name)

def create_fc(gdb, name, geom_type):
    sr = arcpy.SpatialReference(4326)
    fc = os.path.join(gdb, name)
    if not arcpy.Exists(fc):
        arcpy.CreateFeatureclass_management(gdb, name, geom_type, spatial_reference=sr)
        arcpy.AddField_management(fc, "osmid", "TEXT", field_length=32)
        arcpy.AddField_management(fc, "tags",  "TEXT", field_length=2000)
    return fc

def main():
    # Build list of input GDBs
    if 'INPUT_GDBS' in globals() and INPUT_GDBS:
        input_gdbs = INPUT_GDBS[:]  # explicit list
    else:
        pattern = os.path.join(INPUT_PARENT_FOLDER, GDB_GLOB_PATTERN)
        input_gdbs = sorted(glob.glob(pattern))

    if not input_gdbs:
        log("No input GDBs found. Check INPUT_PARENT_FOLDER and GDB_GLOB_PATTERN.")
        return

    ensure_gdb(OUTPUT_GDB)

    # Create target feature classes (use geometry type taken from first found)
    geom_map = {"osm_points": "POINT", "osm_lines": "POLYLINE", "osm_polygons": "POLYGON"}
    targets = {}
    for name in FEATURES:
        targets[name] = create_fc(OUTPUT_GDB, name, geom_map[name])

    # Append from each input GDB
    for gdb in input_gdbs:
        log(f"Processing {gdb}")
        for name in FEATURES:
            src = os.path.join(gdb, name)
            if arcpy.Exists(src):
                log(f"  Appending {name} …")
                arcpy.management.Append(src, targets[name], "NO_TEST")
            else:
                log(f"  Skipping {name} (not found)")

    # Optional: clean geometries
    log("Repairing geometry on output …")
    for name in FEATURES:
        try:
            arcpy.management.RepairGeometry(targets[name], "DELETE_NULL")
        except Exception as ex:
            log(f"  RepairGeometry warning on {name}: {ex}")

    log("Done.")
    log(f"Master: {OUTPUT_GDB}")

if __name__ == "__main__":
    main()
