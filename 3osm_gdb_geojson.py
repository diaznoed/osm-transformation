import arcpy
import os
import re
import json
import csv

# ============================
#  CONFIGURATION
# ============================

# Base directory containing all the gdb folders
BASE_DIR = r"\\sharedrive\office\gdb"
arcpy.env.overwriteOutput = True

# ---- Batch range control ----
# Only process FID numbers between these values (inclusive)
START_FID = 100
END_FID = 200


# ============================
#  HELPER FUNCTIONS
# ============================

def is_fid_folder(name):
    """Matches folders like FID0, FID1, FID600, etc."""
    return re.match(r'^FID\d+$', name, re.IGNORECASE) is not None


def ensure_folder(path):
    """Create folder if missing."""
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def validate_geojson(file_path):
    """Validate GeoJSON and return summary info."""

    if not os.path.exists(file_path):
        return ("MISSING", 0, "")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return ("INVALID_JSON", 0, "")

    if "type" not in data or data["type"] != "FeatureCollection":
        return ("NOT_FEATURE_COLLECTION", 0, "")

    features = data.get("features", [])
    feature_count = len(features)

    geom_types = set()
    for ft in features:
        geom = ft.get("geometry", {})
        if geom and "type" in geom:
            geom_types.add(geom["type"])

    geom_types_str = ",".join(sorted(list(geom_types)))

    if feature_count == 0:
        return ("EMPTY", 0, geom_types_str)

    return ("OK", feature_count, geom_types_str)


def write_validation_row(csv_path, layer, status, feature_count, geom_types):
    """Append a validation row to CSV."""
    new_file = not os.path.exists(csv_path)

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if new_file:  # Write header first time only
            writer.writerow(["layer", "status", "feature_count", "geometry_types"])

        writer.writerow([layer, status, feature_count, geom_types])


# ============================
#  NEW: TAG FIXER
# ============================

def fix_geojson_tags(path):
    """Flatten the 'tags' string into individual fields and remove the original field."""

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except:
        print(f"      [ERROR] Could not load JSON for tag-fix: {path}")
        return

    if data.get("type") != "FeatureCollection":
        return

    for feature in data.get("features", []):
        props = feature.get("properties", {})

        # If "tags" exists and is a JSON string...
        if "tags" in props and isinstance(props["tags"], str):
            try:
                parsed_tags = json.loads(props["tags"])  # Convert string JSON → dict
            except:
                print("      [WARN] Failed to parse tags JSON, removing field")
                del props["tags"]
                continue

            # Flatten: {"highway":"footway"} → tags_highway="footway"
            for k, v in parsed_tags.items():
                props[f"tags_{k}"] = v

            # Remove original string JSON
            del props["tags"]

        feature["properties"] = props

    # Write back the cleaned GeoJSON
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("      ✔ Tags flattened + cleaned (ArcGIS Pro compatible)")


# ============================
#  CONVERSION FUNCTION
# ============================

def _convert_fc(workspace, fc, fid_folder_path):
    """Convert one feature class to GeoJSON, fix tags, and run validation."""
    in_fc_path = os.path.join(workspace, fc)

    # GeoJSON and validation folders
    geojson_folder = ensure_folder(os.path.join(fid_folder_path, "geojson"))
    validation_folder = ensure_folder(os.path.join(fid_folder_path, "validation"))
    validation_csv = os.path.join(validation_folder, "validation_report.csv")

    out_geojson = os.path.join(geojson_folder, f"{fc}.geojson")

    print(f"    → Converting {in_fc_path}")
    print(f"      Output → {out_geojson}")

    # Export to GeoJSON using correct ArcPy signature
    arcpy.conversion.FeaturesToJSON(
        in_features=in_fc_path,
        out_json_file=out_geojson,
        format_json="FORMATTED",
        include_z_values="NO_Z_VALUES",
        include_m_values="NO_M_VALUES",
        geoJSON="GEOJSON",
        outputToWGS84="WGS84",
        use_field_alias="USE_FIELD_NAME"
    )

    # === NEW: Fix the problematic "tags" property ===
    fix_geojson_tags(out_geojson)

    # Validate GeoJSON
    status, feature_count, geom_types = validate_geojson(out_geojson)
    write_validation_row(validation_csv, fc, status, feature_count, geom_types)

    print(f"      VALIDATION → {status} ({feature_count} features, {geom_types})")


# ============================
#  PROCESS A SINGLE FID FOLDER
# ============================

def convert_fgdb_to_geojson(fid_folder_path):
    """Process a single FID folder and its GDB."""
    print(f"\n[PROCESSING FID] {fid_folder_path}")

    ensure_folder(os.path.join(fid_folder_path, "geojson"))
    ensure_folder(os.path.join(fid_folder_path, "validation"))

    gdbs = [d for d in os.listdir(fid_folder_path) if d.lower().endswith(".gdb")]
    if not gdbs:
        print("  [INFO] No .gdb found.")
        return

    for gdb in gdbs:
        gdb_path = os.path.join(fid_folder_path, gdb)
        print(f"  Found GDB: {gdb_path}")

        # ========== ROOT FEATURE CLASSES ==========
        arcpy.env.workspace = gdb_path
        root_fcs = arcpy.ListFeatureClasses()

        if root_fcs:
            print(f"  Root feature classes: {root_fcs}")
            for fc in root_fcs:
                _convert_fc(arcpy.env.workspace, fc, fid_folder_path)
        else:
            print("  No root feature classes found.")

        # ========== FEATURE DATASETS ==========
        datasets = arcpy.ListDatasets("", "Feature")

        if datasets:
            print(f"  Feature datasets: {datasets}")

            for ds in datasets:
                ds_path = os.path.join(gdb_path, ds)
                arcpy.env.workspace = ds_path

                fcs = arcpy.ListFeatureClasses()

                if not fcs:
                    print(f"    [INFO] No feature classes in dataset: {ds}")
                    continue

                for fc in fcs:
                    _convert_fc(arcpy.env.workspace, fc, fid_folder_path)

        else:
            print("  No feature datasets found.")


# ============================
#  MAIN PROGRAM
# ============================

def main():
    print("=== Starting FGDB → GeoJSON Conversion + Tag Fix + Validation ===\n")
    print(f"Scanning: {BASE_DIR}")
    print(f"Processing batch range: FID{START_FID} to FID{END_FID}\n")

    for folder in os.listdir(BASE_DIR):
        folder_path = os.path.join(BASE_DIR, folder)

        if os.path.isdir(folder_path) and is_fid_folder(folder):

            # Extract numeric part: FID123 → 123
            fid_num = int(folder.replace("FID", ""))

            # Only process if inside user-selected range
            if START_FID <= fid_num <= END_FID:
                convert_fgdb_to_geojson(folder_path)
            else:
                print(f"[SKIPPED] {folder} (outside batch range)")

    print("\n=== Conversion + Tag Fix + Validation Complete ===")


if __name__ == "__main__":
    main()
