#!/usr/bin/env python3
# load_townlands.py — CLI tool to create schema and populate the DB from CSV/GeoJSON.
# Commands: create-schema, populate-no-geom, load-townland-touch, populate-geom

import argparse
import csv
import sys
import os
import glob
import json
import math
from copy import deepcopy
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Tuple, List



try:
    import psycopg2
except ImportError:
    print("This script requires psycopg2-binary. Install with: pip install psycopg2-binary", file=sys.stderr)
    raise

# -----------------------------
# Helpers / parsing
# -----------------------------
HEADERS = [
    "OSM_ID","NAME_TAG","NAME_GA","NAME_EN","ALT_NAME","ALT_NAME_G",
    "OSM_USER","OSM_TIMEST","ATTRIBUTIO","LOGAINM_RE",
    "CO_NAME","CO_OSM_ID","CO_LOGAINM",
    "CP_NAME","CP_OSM_ID","CP_LOGAINM",
    "ED_NAME","ED_OSM_ID","ED_LOGAINM",
    "BAR_NAME","BAR_OSM_ID","BAR_LOGAIN",
    "T_IE_URL","AREA","LATITUDE","LONGITUDE","EPOCH_TSTM"
]

def parse_ts(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None

def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None

def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return int(float(s)) if any(c in s for c in (".","e","E")) else int(s)
    except Exception:
        return None

def bucket8(deg):
    if deg is None:
        return None
    try:
        d = float(deg) % 360.0
    except Exception:
        return None
    if d >= 337.5 or d < 22.5:  return "N"
    if d < 67.5:   return "NE"
    if d < 112.5:  return "E"
    if d < 157.5:  return "SE"
    if d < 202.5:  return "S"
    if d < 247.5:  return "SW"
    if d < 292.5:  return "W"
    return "NW"

# -----------------------------
# Schema application (reads schema.sql from script folder or provided path)
# -----------------------------
def apply_schema(conn, schema_path: Optional[str] = None) -> None:
    base = Path(__file__).resolve().parent
    path = Path(schema_path) if schema_path else (base / "schema.sql")
    if not path.exists():
        print(f"ERROR: schema.sql not found at {path}", file=sys.stderr)
        sys.exit(2)
    sql = path.read_text(encoding="utf-8")
    with conn, conn.cursor() as cur:
        cur.execute(sql)

# -----------------------------
# Upsert helpers
# -----------------------------
def upsert_and_get_id(cur, table: str, uniq_col: str, data: Dict[str, Any]) -> int:
    cols = list(data.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    assignments = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != uniq_col])
    sql = f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT ({uniq_col}) DO UPDATE SET {assignments}
        RETURNING id
    """
    cur.execute(sql, [data[c] for c in cols])
    return cur.fetchone()[0]

# -----------------------------
# Loaders
# -----------------------------
def load_csv_populate(conn, csv_path: str, batch_commit: int = 1000) -> Tuple[int,int,int,int,int]:
    county_cache: Dict[int,int] = {}
    barony_cache: Dict[int,int] = {}
    cp_cache: Dict[int,int] = {}
    ed_cache: Dict[int,int] = {}

    seen_rows = 0

    with conn, conn.cursor() as cur, open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        missing = [h for h in HEADERS if h not in reader.fieldnames]
        if missing:
            raise RuntimeError(f"CSV is missing expected headers: {missing}")

        for row in reader:
            seen_rows += 1

            co_osm_id = parse_int(row.get("CO_OSM_ID"))
            if co_osm_id is not None and co_osm_id not in county_cache:
                county_id = upsert_and_get_id(cur, "counties", "osm_id", {
                    "osm_id": co_osm_id,
                    "name": (row.get("CO_NAME") or "").strip() or None,
                    "logainm": (row.get("CO_LOGAINM") or "").strip() or None
                })
                county_cache[co_osm_id] = county_id
            county_id = county_cache.get(co_osm_id) if co_osm_id is not None else None

            bar_osm_id = parse_int(row.get("BAR_OSM_ID"))
            if bar_osm_id is not None and bar_osm_id not in barony_cache:
                barony_id = upsert_and_get_id(cur, "baronies", "osm_id", {
                    "osm_id": bar_osm_id,
                    "name": (row.get("BAR_NAME") or "").strip() or None,
                    "logainm": (row.get("BAR_LOGAIN") or "").strip() or None,
                    "county_id": county_id
                })
                barony_cache[bar_osm_id] = barony_id
            barony_id = barony_cache.get(bar_osm_id) if bar_osm_id is not None else None

            cp_osm_id = parse_int(row.get("CP_OSM_ID"))
            if cp_osm_id is not None and cp_osm_id not in cp_cache:
                cp_id = upsert_and_get_id(cur, "civil_parishes", "osm_id", {
                    "osm_id": cp_osm_id,
                    "name": (row.get("CP_NAME") or "").strip() or None,
                    "logainm": (row.get("CP_LOGAINM") or "").strip() or None
                })
                cp_cache[cp_osm_id] = cp_id
            cp_id = cp_cache.get(cp_osm_id) if cp_osm_id is not None else None

            ed_osm_id = parse_int(row.get("ED_OSM_ID"))
            if ed_osm_id is not None and ed_osm_id not in ed_cache:
                ed_id = upsert_and_get_id(cur, "electoral_divisions", "osm_id", {
                    "osm_id": ed_osm_id,
                    "name": (row.get("ED_NAME") or "").strip() or None,
                    "logainm": (row.get("ED_LOGAINM") or "").strip() or None
                })
                ed_cache[ed_osm_id] = ed_id
            ed_id = ed_cache.get(ed_osm_id) if ed_osm_id is not None else None

            tl_osm_id = parse_int(row.get("OSM_ID"))
            dt = parse_ts(row.get("OSM_TIMEST"))
            lat = parse_float(row.get("LATITUDE"))
            lon = parse_float(row.get("LONGITUDE"))
            area = parse_float(row.get("AREA"))
            epoch = parse_int(row.get("EPOCH_TSTM"))

            sql_tl = """
                INSERT INTO townlands (
                    osm_id, name_tag, name_ga, name_en, alt_name, alt_name_g, wikidata,
                    t_ie_url, area, latitude, longitude, osm_user, osm_timestamp,
                    attribution, logainm_ref, epoch_tstm,
                    county_id, barony_id, civil_parish_id, electoral_division_id
                ) VALUES (
                    %(osm_id)s, %(name_tag)s, %(name_ga)s, %(name_en)s, %(alt_name)s, %(alt_name_g)s, %(wikidata)s,
                    %(t_ie_url)s, %(area)s, %(latitude)s, %(longitude)s, %(osm_user)s, %(osm_timestamp)s,
                    %(attribution)s, %(logainm_ref)s, %(epoch_tstm)s,
                    %(county_id)s, %(barony_id)s, %(civil_parish_id)s, %(electoral_division_id)s
                )
                ON CONFLICT (osm_id) DO UPDATE SET
                    name_tag=EXCLUDED.name_tag,
                    name_ga=EXCLUDED.name_ga,
                    name_en=EXCLUDED.name_en,
                    alt_name=EXCLUDED.alt_name,
                    alt_name_g=EXCLUDED.alt_name_g,
                    wikidata=EXCLUDED.wikidata,
                    t_ie_url=EXCLUDED.t_ie_url,
                    area=EXCLUDED.area,
                    latitude=EXCLUDED.latitude,
                    longitude=EXCLUDED.longitude,
                    osm_user=EXCLUDED.osm_user,
                    osm_timestamp=EXCLUDED.osm_timestamp,
                    attribution=EXCLUDED.attribution,
                    logainm_ref=EXCLUDED.logainm_ref,
                    epoch_tstm=EXCLUDED.epoch_tstm,
                    county_id=EXCLUDED.county_id,
                    barony_id=EXCLUDED.barony_id,
                    civil_parish_id=EXCLUDED.civil_parish_id,
                    electoral_division_id=EXCLUDED.electoral_division_id
            """
            params = {
                "osm_id": tl_osm_id,
                "name_tag": (row.get("NAME_TAG") or "").strip() or None,
                "name_ga": (row.get("NAME_GA") or "").strip() or None,
                "name_en": (row.get("NAME_EN") or "").strip() or None,
                "alt_name": (row.get("ALT_NAME") or "").strip() or None,
                "alt_name_g": (row.get("ALT_NAME_G") or "").strip() or None,
                "wikidata": None,
                "t_ie_url": (row.get("T_IE_URL") or "").strip() or None,
                "area": area,
                "latitude": lat,
                "longitude": lon,
                "osm_user": (row.get("OSM_USER") or "").strip() or None,
                "osm_timestamp": dt,
                "attribution": (row.get("ATTRIBUTIO") or "").strip() or None,
                "logainm_ref": (row.get("LOGAINM_RE") or "").strip() or None,
                "epoch_tstm": epoch,
                "county_id": county_id,
                "barony_id": barony_id,
                "civil_parish_id": cp_id,
                "electoral_division_id": ed_id
            }
            cur.execute(sql_tl, params)

            if seen_rows % batch_commit == 0:
                conn.commit()
                print(f"Processed {seen_rows} rows...", file=sys.stderr)

        conn.commit()
    return seen_rows, len(county_cache), len(barony_cache), len(cp_cache), len(ed_cache)

def load_touch_csv(conn, csv_path: str, batch_commit: int = 500):
    """Load townland touch CSV with headers: t1_osm_id,t2_osm_id,direction,length_m"""
    with conn.cursor() as cur:
        cur.execute("SELECT osm_id, id FROM townlands")
        osm_to_id = dict(cur.fetchall())

    skipped = 0
    n = 0
    with conn, conn.cursor() as cur, open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = ["t1_osm_id","t2_osm_id","direction","length_m"]
        missing = [h for h in required if h not in reader.fieldnames]
        if missing:
            raise RuntimeError(f"Touch CSV missing headers: {missing}")

        for row in reader:
            n += 1
            t1 = parse_int(row.get("t1_osm_id"))
            t2 = parse_int(row.get("t2_osm_id"))
            deg = parse_float(row.get("direction"))
            length_m = parse_float(row.get("length_m"))

            src_id = osm_to_id.get(t1)
            dst_id = osm_to_id.get(t2)
            if src_id is None or dst_id is None:
                skipped += 1
                continue

            cur.execute(
                """
                INSERT INTO townland_touches (src_id, dst_id, direction_deg, dir_bucket8, shared_length_m)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (src_id, dst_id) DO UPDATE SET
                    direction_deg = EXCLUDED.direction_deg,
                    dir_bucket8 = EXCLUDED.dir_bucket8,
                    shared_length_m = EXCLUDED.shared_length_m
                """,
                (src_id, dst_id, deg, bucket8(deg), length_m)
            )

            if n % batch_commit == 0:
                conn.commit()
                print(f"Processed {n} touch rows...", file=sys.stderr)

        conn.commit()

    return {"rows_read": n, "skipped_missing_townland": skipped}

def _osm_id_from_feature_id(fid: str) -> Optional[int]:
    if not fid:
        return None
    fid = fid.strip()
    if fid.startswith("relation/"):
        try:
            return -int(fid.split("/",1)[1])
        except Exception:
            return None
    try:
        n = int(fid)
        return -abs(n)
    except Exception:
        return None


def populate_geom(conn, folder: str, source_srid: int = 4326, batch: int = 200):
    """
    Load county GeoJSON files (FeatureCollection); each feature is a townland polygon.
    Map feature.id "relation/<id>" -> osm_id = -<id>.
    Store canonical geometry in EPSG:2157; keep raw GeoJSON for provenance.
    Update townlands.wikidata when present in feature.properties.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT osm_id, id FROM townlands")
        osm_to_id = dict(cur.fetchall())

    files = sorted(glob.glob(os.path.join(folder, "*.geojson"))) + sorted(glob.glob(os.path.join(folder, "*.json")))
    total_feats = 0; loaded = 0; skipped = 0; fixed_geoms = 0

    with conn, conn.cursor() as cur:
        for fp in files:
            with open(fp, "r", encoding="utf-8") as f:
                print(f"file: {f.name}")
                fc = json.load(f)

            for feat in fc.get("features", []):
                total_feats += 1
                fid = feat.get("id") or (feat.get("properties") or {}).get("@id")
                osm_id = _osm_id_from_feature_id(str(fid) if fid is not None else "")
                if osm_id is None:
                    skipped += 1; continue
                townland_id = osm_to_id.get(osm_id)
                if townland_id is None:
                    skipped += 1; continue
                geom = feat.get("geometry")
                if not geom:
                    print(f"WARNING: feature {fid} in file {f.name} has no geometry; skipping", file=sys.stderr)
                    skipped += 1; continue

                props = feat.get("properties") or {}
                wikidata = props.get("wikidata")
                if wikidata:
                    cur.execute("UPDATE townlands SET wikidata=%s WHERE id=%s", (str(wikidata).strip(), townland_id))

                raw_geom = json.dumps(geom)
                try:
                    cur.execute(
                        """
                        INSERT INTO townland_geoms (townland_id, geom_2157, raw_geojson)
                        VALUES (
                            %s,
                            ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), %s), 2157)), 3)),
                            %s
                        )
                        ON CONFLICT (townland_id) DO UPDATE SET
                            geom_2157 = EXCLUDED.geom_2157,
                            raw_geojson = EXCLUDED.raw_geojson
                        """,
                        (townland_id, raw_geom, source_srid, json.dumps(feat))
                    )
                except psycopg2.errors.InvalidParameterValue as e:
                    print(f"ERROR processing file {fp}: feature: {fid}")
                    print(e, file=sys.stderr)
                    print(feat)
                loaded += 1
                if loaded % batch == 0:
                    conn.commit()
                    print(f"Loaded {loaded}/{total_feats} features...", file=sys.stderr)

        conn.commit()

    return {"files": len(files), "features_total": total_feats, "features_loaded": loaded, "features_skipped": skipped, "features_fixed_geom": fixed_geoms}

# -----------------------------
# CLI
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Populate a Postgres DB with Irish townlands metadata and geometry.")
    parser.add_argument("--dsn", required=True, help="PostgreSQL DSN, e.g. postgresql://user:pass@host:5432/dbname")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_schema = subparsers.add_parser("create-schema", help="Create/ensure database schema from schema.sql in the same folder (or pass --schema)." )
    p_schema.add_argument("--schema", help="Path to schema.sql (defaults to file next to this script)." )
    p_schema.set_defaults(cmd="create-schema")

    p_pop = subparsers.add_parser("populate-no-geom", help="Load CSV and populate normalized tables.")
    p_pop.add_argument("--csv", required=True, help="Path to input CSV file.")
    p_pop.add_argument("--batch", type=int, default=1000, help="Commit every N rows (default 1000).")
    p_pop.set_defaults(cmd="populate-no-geom")

    p_touch = subparsers.add_parser("load-townland-touch", help="Load townland touch adjacency CSV.")
    p_touch.add_argument("--csv", required=True, help="CSV with headers: t1_osm_id,t2_osm_id,direction,length_m")
    p_touch.add_argument("--batch", type=int, default=500, help="Commit every N rows (default 500).")
    p_touch.set_defaults(cmd="load-townland-touch")

    p_geom = subparsers.add_parser("populate-geom", help="Load geometry from a folder of county GeoJSON files.")
    p_geom.add_argument("--folder", required=True, help="Folder containing *.geojson or *.json files (one per county)." )
    p_geom.add_argument("--source-srid", type=int, default=4326, help="SRID of input GeoJSON (default 4326)." )
    p_geom.add_argument("--batch", type=int, default=200, help="Commit every N features (default 200)." )
    p_geom.set_defaults(cmd="populate-geom")

    args = parser.parse_args()

    try:
        conn = psycopg2.connect(args.dsn)
    except Exception as e:
        print(f"Failed to connect to PostgreSQL using DSN. Error: {e}", file=sys.stderr)
        sys.exit(2)

    if args.command == "create-schema":
        apply_schema(conn, args.schema)
        print("Schema created/ensured from schema.sql.")
        return

    if args.command == "populate-no-geom":
        totals = load_csv_populate(conn, args.csv, args.batch)
        print(f"Done. Processed rows: {totals[0]}. Unique counties: {totals[1]}. Unique baronies: {totals[2]}. Unique civil parishes: {totals[3]}. Unique EDs: {totals[4]}.")
        return

    if args.command == "load-townland-touch":
        stats = load_touch_csv(conn, args.csv, args.batch)
        print(f"Done. Touch rows read: {stats['rows_read']}. Skipped (unknown townland): {stats['skipped_missing_townland']}.")
        return

    if args.command == "populate-geom":
        stats = populate_geom(conn, args.folder, args.source_srid, args.batch)
        print(f"Done. Files: {stats['files']}. Features total: {stats['features_total']}. Loaded: {stats['features_loaded']}. Skipped: {stats['features_skipped']}.")
        return

if __name__ == "__main__":
    main()
