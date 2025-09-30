-- schema.sql — Normalized schema with geometry support (PostGIS)
-- Safe to run multiple times.

-- Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

--api_keys table for managing access
CREATE TABLE IF NOT EXISTS api_keys (
    id          BIGSERIAL PRIMARY KEY,
    key_hash    TEXT UNIQUE NOT NULL,
    email       TEXT NOT NULL,
    description TEXT,
    is_active    BOOLEAN DEFAULT TRUE,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    last_used_at TIMESTAMPTZ
);

-- Counties
CREATE TABLE IF NOT EXISTS counties (
    id          BIGSERIAL PRIMARY KEY,
    osm_id      BIGINT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    logainm     TEXT
);

-- Baronies
CREATE TABLE IF NOT EXISTS baronies (
    id          BIGSERIAL PRIMARY KEY,
    osm_id      BIGINT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    logainm     TEXT,
    county_id   BIGINT REFERENCES counties(id) ON UPDATE CASCADE ON DELETE SET NULL
);

-- Civil Parishes
CREATE TABLE IF NOT EXISTS civil_parishes (
    id          BIGSERIAL PRIMARY KEY,
    osm_id      BIGINT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    logainm     TEXT
);

-- Electoral Divisions
CREATE TABLE IF NOT EXISTS electoral_divisions (
    id          BIGSERIAL PRIMARY KEY,
    osm_id      BIGINT UNIQUE NOT NULL,
    name        TEXT NOT NULL,
    logainm     TEXT
);

-- Townlands (no polygon geometry here; see townland_geoms)
CREATE TABLE IF NOT EXISTS townlands (
    id                  BIGSERIAL PRIMARY KEY,
    osm_id              BIGINT UNIQUE NOT NULL,
    name_tag            TEXT,
    name_ga             TEXT,
    name_en             TEXT,
    alt_name            TEXT,
    alt_name_g          TEXT,
    wikidata            TEXT,
    t_ie_url            TEXT,
    area                DOUBLE PRECISION,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    osm_user            TEXT,
    osm_timestamp       TIMESTAMPTZ,
    attribution         TEXT,
    logainm_ref         TEXT,
    epoch_tstm          BIGINT,
    county_id           BIGINT REFERENCES counties(id) ON UPDATE CASCADE ON DELETE SET NULL,
    barony_id           BIGINT REFERENCES baronies(id)  ON UPDATE CASCADE ON DELETE SET NULL,
    civil_parish_id     BIGINT REFERENCES civil_parishes(id) ON UPDATE CASCADE ON DELETE SET NULL,
    electoral_division_id BIGINT REFERENCES electoral_divisions(id) ON UPDATE CASCADE ON DELETE SET NULL
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_townlands_county    ON townlands (county_id);
CREATE INDEX IF NOT EXISTS idx_townlands_barony    ON townlands (barony_id);
CREATE INDEX IF NOT EXISTS idx_townlands_cp        ON townlands (civil_parish_id);
CREATE INDEX IF NOT EXISTS idx_townlands_ed        ON townlands (electoral_division_id);
CREATE INDEX IF NOT EXISTS idx_townlands_latlon    ON townlands (latitude, longitude);

-- Adjacency: townland-to-townland touch relationships
CREATE TABLE IF NOT EXISTS townland_touches (
    src_id           BIGINT NOT NULL REFERENCES townlands(id) ON DELETE CASCADE,
    dst_id           BIGINT NOT NULL REFERENCES townlands(id) ON DELETE CASCADE,
    direction_deg    DOUBLE PRECISION,
    dir_bucket8      TEXT,   -- N, NE, E, SE, S, SW, W, NW
    shared_length_m  DOUBLE PRECISION,
    PRIMARY KEY (src_id, dst_id)
);
CREATE INDEX IF NOT EXISTS idx_touch_src ON townland_touches (src_id);
CREATE INDEX IF NOT EXISTS idx_touch_src_dir ON townland_touches (src_id, dir_bucket8);

-- Geometry store (canonical in EPSG:2157); keep original GeoJSON for provenance.
CREATE TABLE IF NOT EXISTS townland_geoms (
    townland_id     BIGINT PRIMARY KEY REFERENCES townlands(id) ON DELETE CASCADE,
    geom_2157       geometry(MultiPolygon,2157) NOT NULL,
    raw_geojson     JSONB
);
CREATE INDEX IF NOT EXISTS townland_geoms_gix ON townland_geoms USING GIST (geom_2157);

-- Ensure a 4326 copy is kept in sync for web mapping / bbox queries
ALTER TABLE townland_geoms
  ADD COLUMN IF NOT EXISTS geom_4326 geometry(MultiPolygon,4326);

-- Keep geom_4326 synchronized from geom_2157
CREATE OR REPLACE FUNCTION set_townland_geom_4326()
RETURNS trigger AS $$
BEGIN
  NEW.geom_4326 := ST_Transform(NEW.geom_2157, 4326);
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'trg_townland_geoms_set_4326'
  ) THEN
    CREATE TRIGGER trg_townland_geoms_set_4326
    BEFORE INSERT OR UPDATE OF geom_2157 ON townland_geoms
    FOR EACH ROW EXECUTE FUNCTION set_townland_geom_4326();
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS townland_geoms_gix_4326
  ON townland_geoms USING GIST (geom_4326);
