# Irish Townlands Database — Setup & Loader

End-to-end instructions to **create a PostgreSQL/PostGIS database** and **populate it** with:

- Townland attributes (CSV)
- Townland–townland “touch”/adjacency (CSV)
- Townland geometries (county GeoJSON files)

It also includes example queries to look up the **townland, barony, civil parish, and county** for any lon/lat.

---

## Contents

- [What you’ll need](#what-youll-need)
- [Install prerequisites](#install-prerequisites)
- [Create the database and user](#create-the-database-and-user)
- [Initialize the schema](#initialize-the-schema)
- [Load data](#load-data)
  - [1) Attributes (CSV)](#1-attributes-csv)
  - [2) Touch adjacency (CSV)](#2-touch-adjacency-csv)
  - [3) Geometries (GeoJSON folder)](#3-geometries-geojson-folder)
- [Verify your data](#verify-your-data)
- [Example queries](#example-queries)
- [EPSG choice (why 2157 + 4326)](#epsg-choice-why-2157--4326)
- [Troubleshooting](#troubleshooting)
- [Commands quick reference](#commands-quick-reference)
- [Files in this repo](#files-in-this-repo)

---

## What you’ll need

### Source data

1) **Townlands CSV** (one row per townland) — with **exact headers**:

```
OSM_ID,NAME_TAG,NAME_GA,NAME_EN,ALT_NAME,ALT_NAME_G,
OSM_USER,OSM_TIMEST,ATTRIBUTIO,LOGAINM_RE,
CO_NAME,CO_OSM_ID,CO_LOGAINM,
CP_NAME,CP_OSM_ID,CP_LOGAINM,
ED_NAME,ED_OSM_ID,ED_LOGAINM,
BAR_NAME,BAR_OSM_ID,BAR_LOGAIN,
T_IE_URL,AREA,LATITUDE,LONGITUDE,EPOCH_TSTM
```
There is a version on [townlands.ie].(https://townlands.ie/static/downloads/townlands-no-geom.csv.zip)

2) **Townland touch (adjacency) CSV** — headers:

```
t1_osm_id,t2_osm_id,direction,length_m
```

- `direction` = bearing **degrees** from `t1` to `t2` (0° = North, clockwise).
- `length_m` = shared border length (meters).
- A public dataset is linked under **“[townland touch](https://townlands.ie/static/downloads/townlandtouch.csv.zip)”** on townlands.ie downloads.

3) **County GeoJSON files (one per county)** — each is a **FeatureCollection**:
- Each **feature** is one townland.
- `feature.id` looks like `"relation/<id>"`. The loader maps this to the townland **OSM_ID = -<id>** (negative integer).
- `feature.properties.wikidata` (if present) is stored in the townland record.
- Coordinates are **EPSG:4326** (WGS84 lon/lat).

> If your files differ (e.g., a different ID field), tweak `_osm_id_from_feature_id()` in the loader.

---

## Install prerequisites

- **Python 3.9+**
- **PostgreSQL 12+** (14+ recommended)
- **PostGIS** (spatial extension)
- Python package:
  ```bash
  pip install psycopg2-binary
  ```

### Installing PostGIS (pick one approach)

- **EnterpriseDB (EDB) macOS installer + StackBuilder**: run StackBuilder → *Spatial Extensions* → install PostGIS → restart service.
- **Postgres.app (macOS)**: includes PostGIS; enable with `CREATE EXTENSION postgis;`.
- **Homebrew**:
  ```bash
  brew install postgresql@14 postgis
  brew services start postgresql@14
  ```
- **Docker**:
  ```bash
  docker run --name pg -e POSTGRES_PASSWORD=pass -p 5432:5432 -d postgis/postgis:14-3.4
  ```

---

## Create the database and user

### SQL (psql)

```sql
-- Create a dedicated user and database
CREATE USER townlands_app WITH PASSWORD 'STRONG_PASSWORD';
CREATE DATABASE townlands OWNER townlands_app;

-- Connect to the database (in psql): \c townlands
CREATE EXTENSION IF NOT EXISTS postgis;

-- Ensure the user can create objects in public schema
GRANT USAGE, CREATE ON SCHEMA public TO townlands_app;

-- (Optional) Default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO townlands_app;
```

**DSN example** (use in commands below):
```
postgresql://townlands_app:STRONG_PASSWORD@localhost:5432/townlands
```

---

## Initialize the schema

This creates normalized tables for **counties**, **baronies**, **civil parishes**, **electoral divisions**, **townlands**, an adjacency table **townland_touches**, and a geometry table **townland_geoms**.

- Canonical geometry is stored in **EPSG:2157** (`geom_2157`).
- A synced **EPSG:4326** copy (`geom_4326`) is maintained by trigger for web/bbox queries.

**Files** you should have locally (from this repo):
- `schema.sql`
- `load_townlands.py`

Apply the schema:

```bash
psql "postgresql://townlands_app:STRONG_PASSWORD@localhost:5432/townlands" -f schema.sql
```

(Alternatively, run `python load_townlands.py create-schema --dsn <DSN>` to apply the same SQL bundled in the script.)

---

## Load data

### 1) Attributes (CSV)

```bash
python load_townlands.py  \
  --dsn "postgresql://townlands_app:STRONG_PASSWORD@localhost:5432/townlands" \
  populate-no-geom \
  --csv ~/Documents/maps/townlands/townlands-no-geom.csv
```

What it does:
- Upserts **counties**, **baronies**, **civil parishes**, **EDs** by `osm_id`.
- Upserts **townlands** by `osm_id` and sets FKs.
- Stores centroids (`latitude`, `longitude`), URLs, attribution, timestamps, etc.

### 2) Touch adjacency (CSV)

```bash
python load_townlands.py  \
  --dsn "postgresql://townlands_app:STRONG_PASSWORD@localhost:5432/ireland_map" \
  load-townland-touch \
  --csv ~/Documents/maps/townlands/townlandtouch.csv
```

What it does:
- Inserts **directed** rows (A→B) into `townland_touches`.
- Saves `direction_deg` and an 8-way compass bucket (`dir_bucket8`) plus `shared_length_m`.
- Skips rows if either townland OSM ID wasn’t found in the DB.

> If your CSV contains only A→B, queries from B won’t see A unless you look up B→A; you can add an option to auto-insert the reverse (bearing+180°).

### 3) Geometries (GeoJSON folder)

```bash
python load_townlands.py  \
  --dsn "postgresql://user:pass@localhost:5432/townlands" \
  populate-geom --source-srid 4326 \
  --folder ~/Documents/maps/townlands/counties
```

What it does:
- For each FeatureCollection file (`*.geojson` / `*.json`), reads features:
  - `feature.id` `"relation/<id>"` → **`osm_id = -<id>`** (negative integer)
  - Upserts geometry → `geom_2157` using `ST_GeomFromGeoJSON` → `SetSRID(4326)` → `Transform(2157)` → `MakeValid` → `Multi`
  - Updates `townlands.wikidata` if present
  - Stores the entire feature JSON in `raw_geojson`
- A trigger fills **`geom_4326`** for web mapping.

---

## Verify your data

Basic counts:

```sql
SELECT COUNT(*) FROM counties;
SELECT COUNT(*) FROM baronies;
SELECT COUNT(*) FROM civil_parishes;
SELECT COUNT(*) FROM electoral_divisions;
SELECT COUNT(*) FROM townlands;
SELECT COUNT(*) FROM townland_geoms;
SELECT COUNT(*) FROM townland_touches;
```

Check PostGIS:

```sql
SELECT postgis_full_version();
```

Spot-check one county:

```sql
SELECT c.name, COUNT(b.*) AS baronies
FROM counties c
LEFT JOIN baronies b ON b.county_id = c.id
GROUP BY c.name ORDER BY c.name;
```

---

## Example queries

### A) Reverse-geocode a lon/lat → townland + admin units

```sql
-- Params: $1 = longitude, $2 = latitude (WGS84)
WITH pt AS (
  SELECT ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 4326), 2157) AS g
)
SELECT
  tl.osm_id AS townland_osm_id,
  COALESCE(tl.name_en, tl.name_ga, tl.name_tag) AS townland_name,
  b.name  AS barony,
  cp.name AS civil_parish,
  c.name  AS county,
  ed.name AS electoral_division,
  tl.wikidata
FROM pt
JOIN townland_geoms tg ON ST_Covers(tg.geom_2157, pt.g)
JOIN townlands tl      ON tl.id = tg.townland_id
LEFT JOIN baronies b         ON b.id  = tl.barony_id
LEFT JOIN civil_parishes cp  ON cp.id = tl.civil_parish_id
LEFT JOIN counties c         ON c.id  = tl.county_id
LEFT JOIN electoral_divisions ed ON ed.id = tl.electoral_division_id;
```

### B) Neighbours (touching townlands) for a given townland name

```sql
-- $1 = townland name pattern (e.g. '%Ballybeg%'), $2 = county filter or NULL
WITH src AS (
  SELECT t.id
  FROM townlands t
  LEFT JOIN counties c ON c.id = t.county_id
  WHERE (t.name_en ILIKE $1 OR t.name_ga ILIKE $1 OR t.name_tag ILIKE $1
         OR t.alt_name ILIKE $1 OR t.alt_name_g ILIKE $1)
    AND ($2 IS NULL OR c.name ILIKE $2)
)
SELECT
  COALESCE(n.name_en, n.name_ga, n.name_tag) AS neighbour_name,
  c1.name  AS neighbour_county,
  tt.dir_bucket8,
  ROUND(tt.direction_deg::numeric, 1) AS bearing_deg,
  tt.shared_length_m
FROM src s
JOIN townland_touches tt ON tt.src_id = s.id
JOIN townlands n ON n.id = tt.dst_id
LEFT JOIN counties c1 ON c1.id = n.county_id
ORDER BY
  CASE tt.dir_bucket8
    WHEN 'N' THEN 1 WHEN 'NE' THEN 2 WHEN 'E' THEN 3 WHEN 'SE' THEN 4
    WHEN 'S' THEN 5 WHEN 'SW' THEN 6 WHEN 'W' THEN 7 WHEN 'NW' THEN 8
  END, tt.shared_length_m DESC;
```

---

## EPSG choice (why 2157 + 4326)

- **Canonical storage:** `geom_2157` (Irish Transverse Mercator). Accurate meters → correct area/length/buffer/azimuth operations, fast indexes.
- **Input/serving:** 4326 is the web/native GeoJSON CRS. A trigger keeps `geom_4326` in sync for quick bbox and API responses—no transform cost at query time.

---

## Troubleshooting

**`ERROR: could not open extension control file ... postgis.control`**
PostGIS isn’t installed for your server build. Install via StackBuilder (EDB), Postgres.app (macOS), Homebrew (`brew install postgis`), or use a PostGIS Docker image. Then run `CREATE EXTENSION postgis;`.

**Permission errors creating schema**
Run the schema with a superuser once, or ensure your app role has `USAGE, CREATE` on `public`. Creating PostGIS usually requires superuser.

**“Touch CSV missing headers”**
Your file must have exactly: `t1_osm_id,t2_osm_id,direction,length_m`.

**“Skipped (unknown townland)” during touch load**
The OSM IDs in the touch CSV weren’t found in the `townlands` table—load attributes first, and ensure your OSM IDs match the rule above (GeoJSON relation IDs are mapped to **negative** OSM IDs).

**GeoJSON ID mapping**
The loader converts `feature.id == "relation/<id>"` → `osm_id = -<id>`. If your features use a different field or positive IDs, adjust `_osm_id_from_feature_id()` accordingly.

---

## Commands quick reference

```bash
# Create schema (or apply schema.sql)
python load_townlands.py create-schema --dsn "postgresql://user:pass@host:5432/townlands"

# Load attributes (CSV)
python load_townlands.py populate-no-geom --csv /path/to/townlands.csv --dsn "..."

# Load townland touch adjacency (CSV)
python load_townlands.py load-townland-touch --csv /path/to/townland_touch.csv --dsn "..."

# Load geometries (folder of county GeoJSONs)
python load_townlands.py populate-geom --folder /path/to/geojson_counties --source-srid 4326 --dsn "..."
```

---

## Files in this repo

- `schema.sql` – database DDL (PostGIS, tables, indexes, trigger to sync `geom_4326`)
- `load_townlands.py` – CLI loader (create schema, load attributes, load touches, load geometries)
