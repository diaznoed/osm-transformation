# -*- coding: utf-8 -*-
r"""
OSM → Clip to AOIs → ArcGIS Pro GDB
Standalone ArcPy-only pipeline (no geopandas/fiona/shapely required)

Batch/range control (Option 2):
  set START_AOI=378
  set END_AOI=477
  "C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\propy.bat" C:\Users\name\Desktop\osm_clip_pipeline.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import traceback
import urllib.request
import urllib.error
import urllib.parse
from typing import Dict, List, Tuple

import arcpy

# ------------- CONFIG ---------------------------------------------------------

# AOI shapefile (your path)
#AOI_PATH = r"I:\FFDA_Experiments\Harper\GIGA_2.0\Shapefiles\AOIs\GIGA2_2_5KM_AOIs.shp"
AOI_PATH = r"C:\Users\diaznd\Desktop\AOI_CSV_Join_SHP\CSV_BBoxes_Intersecting_AOIs.shp"

# NGA Overpass endpoint (confirmed)
osmapi_URL = "https://osm.com/osm/interpreter"

# Output GDB (confirmed)
OUTPUT_GDB = os.path.abspath(r"C:\Users\name\Desktop\osm_batches\osm_clipped100.gdb")

# Tile size in degrees for Overpass queries (smaller = lighter queries)
TILE_DEG = 0.25

# Small pad around AOI bbox to avoid edge clipping loss
BBOX_BUFFER_DEG = 0.0005

# Overpass timeouts / retries
HTTP_TIMEOUT_SEC = 300
OVERPASS_TIMEOUT = 180  # seconds for [timeout:] in the Overpass QL
MAX_RETRIES = 5
SLEEP_BETWEEN_TILES = 1.25  # seconds

# Optional guard to avoid pathological AOIs generating tons of tiles
MAX_TILES_PER_AOI = 400

# Tag filters (balanced for stability + useful content)
TAG_QUERIES = {
    "points": [
        'node["amenity"]',
        'node["shop"]',
        'node["man_made"]',
        'node["tourism"]',            # e.g., tourism=viewpoint
        'node["seamark:type"]',
        'node[~"^seamark:.*"~".*"]',  # any seamark:* node
    ],
    "lines": [
        'way["highway"]',
        'way["railway"]',
        'way["waterway"]',
    ],
    "polys": [
        'way["building"]',
        'way["landuse"]',
        'way["natural"]',
        'way["water"]',
    ],
    # Multipolygon relations that represent areas
    "rel_polys": [
        'relation["type"="multipolygon"]["building"]',
        'relation["type"="multipolygon"]["landuse"]',
        'relation["type"="multipolygon"]["natural"]',
        'relation["type"="multipolygon"]["water"]',
        'relation["type"="multipolygon"]["amenity"]',  # e.g., grave_yard
        'relation["type"="multipolygon"]["place"]',    # e.g., island
    ],
}

# Batch/run-window controls (Option 2) — 1-based indexes after MultipartToSinglepart
START_AOI = 3401   # inclusive
END_AOI   = 3500   # inclusive; 0 = no upper limit

# Final layer names inside the GDB
LAYER_POINTS   = "osm_points"
LAYER_LINES    = "osm_lines"
LAYER_POLYGONS = "osm_polygons"

# -----------------------------------------------------------------------------


def log(msg: str) -> None:
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def ensure_gdb(path: str) -> None:
    folder, gdbname = os.path.split(path)
    if not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True)
    if not arcpy.Exists(path):
        arcpy.CreateFileGDB_management(folder, gdbname)


def _add_core_fields(fc: str) -> None:
    # Minimal schema expansion for fuller attribution (non-breaking to workflow)
    existing = {f.name.lower() for f in arcpy.ListFields(fc)}
    def add(name, ftype, **kw):
        if name.lower() not in existing:
            arcpy.AddField_management(fc, name, ftype, **kw)

    add("osmid",     "TEXT", field_length=32)
    add("tags",      "TEXT", field_length=8000)  # bumped from 2000 → 8000
    add("elem_type", "TEXT", field_length=10)    # node | way | relation
    add("version",   "LONG")
    add("timestamp", "TEXT", field_length=30)
    add("changeset", "LONG")
    add("uid",       "LONG")
    add("user",      "TEXT", field_length=255)


def create_fc(gdb: str, name: str, geom_type: str) -> str:
    sr = arcpy.SpatialReference(4326)  # WGS84
    fc = os.path.join(gdb, name)
    if arcpy.Exists(fc):
        arcpy.management.Delete(fc)
    arcpy.CreateFeatureclass_management(gdb, name, geom_type, spatial_reference=sr)
    _add_core_fields(fc)
    return fc


def append_fields_if_missing(fc: str) -> None:
    _add_core_fields(fc)


def bbox_of_geom(geom: arcpy.Geometry, pad: float) -> Tuple[float, float, float, float]:
    ext = geom.extent
    s = ext.YMin - pad
    w = ext.XMin - pad
    n = ext.YMax + pad
    e = ext.XMax + pad
    return (s, w, n, e)


def tiles_from_bbox(bbox: Tuple[float, float, float, float], step: float) -> List[Tuple[float, float, float, float]]:
    s, w, n, e = bbox
    tiles = []
    lat = s
    while lat < n:
        next_lat = min(lat + step, n)
        lon = w
        while lon < e:
            next_lon = min(lon + step, e)
            tiles.append((lat, lon, next_lat, next_lon))
            lon = next_lon
        lat = next_lat
    return tiles


def build_overpass_query_geom(tag_queries: Dict[str, List[str]], bbox: Tuple[float, float, float, float],
                              timeout: int) -> str:
    s, w, n, e = bbox

    # Node/way requests
    nw_parts = []
    for expr in (tag_queries.get("points", []) +
                 tag_queries.get("lines", []) +
                 tag_queries.get("polys", [])):
        nw_parts.append(f"  {expr}({s},{w},{n},{e});")
    nw_union = "\n".join(nw_parts)

    # Relation requests (multipolygons)
    rel_parts = []
    for expr in tag_queries.get("rel_polys", []):
        rel_parts.append(f"  {expr}({s},{w},{n},{e});")
    rel_union = "\n".join(rel_parts)

    # If we have relations, recurse to member ways + nodes
    if rel_union:
        ql = f"""
[out:json][timeout:{timeout}];
(
{nw_union}
{rel_union}
);
(._;>>;);
out body geom;
""".strip()
    else:
        ql = f"""
[out:json][timeout:{timeout}];
(
{nw_union}
);
out body geom;
""".strip()

    return ql


def post_overpass(ql: str, timeout_sec: int) -> dict:
    data = urllib.parse.urlencode({"data": ql}).encode("utf-8")
    req = urllib.request.Request(OVERPASS_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8", errors="replace"))


def fetch_tile_light(bbox: Tuple[float, float, float, float]) -> dict:
    ql = build_overpass_query_geom(TAG_QUERIES, bbox, OVERPASS_TIMEOUT)
    backoff = 2.0
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return post_overpass(ql, HTTP_TIMEOUT_SEC)
        except Exception as ex:
            last_err = ex
            log(f"Overpass transient (attempt {attempt}/{MAX_RETRIES}): {ex}")
            time.sleep(backoff)
            backoff *= 1.8
    raise RuntimeError(f"Overpass failed after retries: {last_err}")


def features_from_overpass(data: dict):
    """
    Parse Overpass JSON into three lists:
      - pts:  [(arcpy.Geometry, osmid, tags_json, elem_type, version, timestamp, changeset, uid, user)]
      - lns:  [(arcpy.Geometry, osmid, tags_json, elem_type, version, timestamp, changeset, uid, user)]
      - pols: [(arcpy.Geometry, osmid, tags_json, elem_type, version, timestamp, changeset, uid, user)]
    Includes robust polygon detection and multipolygon assembly.
    """
    sr = arcpy.SpatialReference(4326)
    pts, lns, pols = [], [], []
    if not data or "elements" not in data:
        return pts, lns, pols

    def tags_safestr(tags):
        try:
            return json.dumps(tags or {}, ensure_ascii=False)  # no truncation
        except Exception:
            return ""

    def is_area_tag(tags: dict) -> bool:
        if not tags:
            return False
        if str(tags.get("area", "")).lower() in ("yes", "1", "true"):
            return True
        return any(k in tags for k in ("building","landuse","amenity","leisure","natural","water","waterway","place"))

    def coords_closed(coords, eps=1e-9):
        if len(coords) < 4:
            return False
        x0, y0 = coords[0]
        x1, y1 = coords[-1]
        return (abs(x0 - x1) <= eps) and (abs(y0 - y1) <= eps)

    def stitch_rings(ways_coords):
        """
        ways_coords: list of coordinate lists for member ways.
        Returns list of closed rings (each a list of (x,y)).
        Greedy endpoint stitching with reversal as needed.
        """
        frags = [list(coords) for coords in ways_coords if len(coords) >= 2]
        rings = []
        used = [False] * len(frags)

        def try_close(chain):
            return chain if coords_closed(chain) else None

        for i in range(len(frags)):
            if used[i]:
                continue
            chain = frags[i][:]
            used[i] = True
            extended = True
            while extended:
                extended = False
                cs, ce = chain[0], chain[-1]
                for j in range(len(frags)):
                    if used[j]:
                        continue
                    w = frags[j]
                    ws, we = w[0], w[-1]
                    if ws == ce:
                        chain.extend(w[1:])
                        used[j] = True
                        extended = True
                    elif we == ce:
                        chain.extend(list(reversed(w[:-1])))
                        used[j] = True
                        extended = True
                    elif we == cs:
                        chain = w[:-1] + chain
                        used[j] = True
                        extended = True
                    elif ws == cs:
                        chain = list(reversed(w[1:])) + chain
                        used[j] = True
                        extended = True
            closed = try_close(chain)
            if closed:
                rings.append(closed)

        return rings

    # First pass: index nodes/ways; retain relations for later
    nodes = {}  # id -> (lon,lat)
    ways  = {}  # id -> {"tags":{}, "coords":[(x,y),...], "meta":{...}}
    rels  = []  # multipolygon relations (keep full element for meta)

    for el in data["elements"]:
        if el.get("type") == "node" and "lon" in el and "lat" in el:
            nodes[el["id"]] = (el["lon"], el["lat"])

    for el in data["elements"]:
        et = el.get("type"); eid = el.get("id"); tags = el.get("tags", {}) or {}
        meta = {
            "version": el.get("version"),
            "timestamp": el.get("timestamp"),
            "changeset": el.get("changeset"),
            "uid": el.get("uid"),
            "user": el.get("user"),
        }

        if et == "node":
            # Keep only POIs (nodes with tags) as point features
            if tags:
                lon, lat = nodes.get(eid, (None, None))
                if lon is not None and lat is not None:
                    pts.append((
                        arcpy.PointGeometry(arcpy.Point(lon, lat), sr),
                        str(eid),
                        tags_safestr(tags),
                        "node",
                        meta["version"], meta["timestamp"], meta["changeset"], meta["uid"], meta["user"]
                    ))

        elif et == "way":
            geom = el.get("geometry")
            if geom:
                coords = [(p["lon"], p["lat"]) for p in geom if "lon" in p and "lat" in p]
            else:
                nds = el.get("nodes") or []
                coords = [nodes[n] for n in nds if n in nodes]
            if len(coords) >= 2:
                ways[eid] = {"tags": tags, "coords": coords, "meta": meta}

        elif et == "relation":
            if (el.get("tags", {}) or {}).get("type") == "multipolygon" and is_area_tag(el.get("tags", {}) or {}):
                el["_meta"] = meta
                rels.append(el)

    # Standalone ways → lines or polygons
    for wid, w in ways.items():
        coords = w["coords"]
        if len(coords) < 2:
            continue
        t = w["tags"]
        m = w["meta"]
        is_closed = len(coords) >= 4 and (coords[0] == coords[-1] or coords_closed(coords))
        if is_closed and is_area_tag(t):
            ring = arcpy.Array([arcpy.Point(x, y) for (x, y) in coords])
            pg = arcpy.Polygon(arcpy.Array([ring]), sr)
            pols.append((pg, str(wid), tags_safestr(t), "way",
                         m["version"], m["timestamp"], m["changeset"], m["uid"], m["user"]))
        else:
            arr = arcpy.Array([arcpy.Point(x, y) for (x, y) in coords])
            ln = arcpy.Polyline(arr, sr)
            lns.append((ln, str(wid), tags_safestr(t), "way",
                        m["version"], m["timestamp"], m["changeset"], m["uid"], m["user"]))

    # Multipolygon relations → stitched polygons (with holes)
    for rel in rels:
        rtags = rel.get("tags", {}) or {}
        rmeta = rel.get("_meta", {})
        members = rel.get("members", []) or []
        outers, inners = [], []
        member_tags_union = {}

        for m in members:
            if m.get("type") != "way":
                continue
            w = ways.get(m.get("ref"))
            if not w:
                continue
            role = (m.get("role") or "").lower()
            if role == "outer":
                outers.append(w["coords"])
            elif role == "inner":
                inners.append(w["coords"])
            # union member way tags (shallow union)
            for k, v in (w.get("tags") or {}).items():
                if k not in member_tags_union:
                    member_tags_union[k] = v

        # Merge member-way tags with relation tags (relation keys win)
        merged_tags = dict(member_tags_union)
        merged_tags.update(rtags)

        outer_rings = stitch_rings(outers)
        inner_rings = stitch_rings(inners)

        if not outer_rings:
            continue

        hole_arrays = [arcpy.Array([arcpy.Point(x, y) for (x, y) in ring]) for ring in inner_rings]
        for outer in outer_rings:
            outer_arr = arcpy.Array([arcpy.Point(x, y) for (x, y) in outer])
            # ArcPy polygon with holes = first array is the outer ring, then inner rings
            pg = arcpy.Polygon(arcpy.Array([outer_arr] + hole_arrays if hole_arrays else [outer_arr]), sr)
            pols.append((pg, str(rel.get("id")), tags_safestr(merged_tags), "relation",
                         rmeta.get("version"), rmeta.get("timestamp"), rmeta.get("changeset"),
                         rmeta.get("uid"), rmeta.get("user")))

    return pts, lns, pols


def insert_rows(fc: str, rows: List[Tuple[arcpy.Geometry, str, str, str, int, str, int, int, str]]) -> None:
    if not rows:
        return
    append_fields_if_missing(fc)
    fields = ["SHAPE@", "osmid", "tags", "elem_type", "version", "timestamp", "changeset", "uid", "user"]
    with arcpy.da.InsertCursor(fc, fields) as cur:
        for r in rows:
            # ensure tuple is the right length (fill Nones if needed)
            if len(r) < len(fields):
                r = tuple(list(r) + [None] * (len(fields) - len(r)))
            cur.insertRow(r)


def clip_append(raw_fc: str, aoi_fc: str, final_fc: str) -> None:
    """Clip raw features to AOI geometry then append to final."""
    tmp = arcpy.CreateUniqueName(os.path.basename(raw_fc) + "_clip", "in_memory")
    try:
        arcpy.analysis.Clip(raw_fc, aoi_fc, tmp)
        if not arcpy.Exists(final_fc):
            geom_type = arcpy.Describe(raw_fc).shapeType.upper()
            create_fc(os.path.dirname(final_fc), os.path.basename(final_fc), geom_type)
        append_fields_if_missing(final_fc)
        arcpy.management.Append(tmp, final_fc, "NO_TEST")
    finally:
        if arcpy.Exists(tmp):
            arcpy.management.Delete(tmp)


def main():
    log(f"Overpass: {OVERPASS_URL}")
    ensure_gdb(OUTPUT_GDB)

    # Ensure final layers
    out_pts = os.path.join(OUTPUT_GDB, LAYER_POINTS)
    out_lns = os.path.join(OUTPUT_GDB, LAYER_LINES)
    out_pol = os.path.join(OUTPUT_GDB, LAYER_POLYGONS)
    if not arcpy.Exists(out_pts): create_fc(OUTPUT_GDB, LAYER_POINTS, "POINT")
    if not arcpy.Exists(out_lns): create_fc(OUTPUT_GDB, LAYER_LINES, "POLYLINE")
    if not arcpy.Exists(out_pol): create_fc(OUTPUT_GDB, LAYER_POLYGONS, "POLYGON")

    # In-memory staging (reset per AOI)
    def new_raw():
        rp = create_fc(OUTPUT_GDB, LAYER_POINTS + "_raw", "POINT")
        rl = create_fc(OUTPUT_GDB, LAYER_LINES + "_raw", "POLYLINE")
        ro = create_fc(OUTPUT_GDB, LAYER_POLYGONS + "_raw", "POLYGON")
        return rp, rl, ro

    raw_pts, raw_lns, raw_pol = new_raw()

    # Prep AOIs in WGS84 + explode to single parts
    sr_wgs84 = arcpy.SpatialReference(4326)
    aoi_4326 = arcpy.CreateUniqueName("aoi_4326", "in_memory")
    aoi_parts = arcpy.CreateUniqueName("aoi_parts", "in_memory")

    log("Projecting AOIs to WGS84 & exploding to single parts…")
    arcpy.management.Project(AOI_PATH, aoi_4326, sr_wgs84)
    arcpy.management.MultipartToSinglepart(aoi_4326, aoi_parts)

    # Cursor through AOIs with index-based selection window
    with arcpy.da.SearchCursor(aoi_parts, ["OID@", "SHAPE@"]) as rows:
        for idx, (oid, geom) in enumerate(rows, start=1):
            # Range controls
            if idx < START_AOI:
                continue
            if END_AOI and idx > END_AOI:
                break

            aoi_fc = arcpy.CreateUniqueName(f"aoi_part_{oid}", "in_memory")
            arcpy.management.CopyFeatures([geom], aoi_fc)

            bbox = bbox_of_geom(geom, BBOX_BUFFER_DEG)
            tiles = tiles_from_bbox(bbox, TILE_DEG)
            log(f"AOI index {idx} (OID {oid}): bbox {bbox} → {len(tiles)} tile(s)")

            if len(tiles) > MAX_TILES_PER_AOI:
                log(f"  AOI {oid}: {len(tiles)} tiles exceeds MAX_TILES_PER_AOI={MAX_TILES_PER_AOI} — skipping.")
                if arcpy.Exists(aoi_fc): arcpy.management.Delete(aoi_fc)
                continue

            # Process tiles
            for ti, tile in enumerate(tiles, start=1):
                log(f"  AOI {oid} | tile {ti}/{len(tiles)}: {tile}")
                try:
                    data = fetch_tile_light(tile)
                except Exception as ex:
                    log(f"    Tile failed (skipping): {ex}")
                    continue

                pts, lns, pols = features_from_overpass(data)
                log(f"    parsed: {len(pts)} pts, {len(lns)} lines, {len(pols)} polys")
                insert_rows(raw_pts, pts)
                insert_rows(raw_lns, lns)
                insert_rows(raw_pol, pols)

                time.sleep(SLEEP_BETWEEN_TILES)

            # Clip raw to this AOI and append to finals
            log(f"  AOI {oid}: clipping accumulators to this AOI and appending…")
            try:
                clip_append(raw_pts, aoi_fc, out_pts)
                clip_append(raw_lns, aoi_fc, out_lns)
                clip_append(raw_pol, aoi_fc, out_pol)
            except Exception as ex:
                log(f"  AOI {oid}: clip/append error: {ex}")

            # Reset RAW accumulators for next AOI
            for fc in (raw_pts, raw_lns, raw_pol):
                if arcpy.Exists(fc):
                    arcpy.management.Delete(fc)
            raw_pts, raw_lns, raw_pol = new_raw()

            if arcpy.Exists(aoi_fc):
                arcpy.management.Delete(aoi_fc)

    # Final geometry cleanup
    log("Repairing geometries…")
    try:
        arcpy.management.RepairGeometry(out_pts, "DELETE_NULL")
        arcpy.management.RepairGeometry(out_lns, "DELETE_NULL")
        arcpy.management.RepairGeometry(out_pol, "DELETE_NULL")
    except Exception as ex:
        log(f"RepairGeometry warning: {ex}")

    log("Done.")
    log(f"Open in ArcGIS Pro: {OUTPUT_GDB}")
    log(f"Layers: {LAYER_POINTS}, {LAYER_LINES}, {LAYER_POLYGONS}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("FATAL:")
        log(str(e))
        traceback.print_exc()
        sys.exit(1)
