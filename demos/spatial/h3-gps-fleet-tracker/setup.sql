-- ============================================================================
-- H3 GPS Fleet Tracker — Setup Script
-- ============================================================================
-- Creates Delta tables with generated GPS data and region boundaries, then
-- builds H3-indexed views for O(1) spatial joins.
--
--   1. landmarks       — 10 famous world landmarks with known coordinates
--   2. regions         — 5 city boundaries as WKT polygons
--   3. gps_points      — 10,000 GPS pings across 5 cities (deterministic)
--   4. points_h3       — VIEW: gps_points enriched with H3 cell IDs
--   5. region_cells    — VIEW: regions expanded to H3 cell coverage
--
-- Demonstrates:
--   - CREATE DELTA TABLE with schema definition
--   - INSERT INTO ... SELECT FROM generate_series() for data generation
--   - 21 H3 spatial functions (coordinate conversion, grid topology,
--     hierarchy, metrics, polyfill, validation, string conversion)
--   - O(1) spatial join via H3 cell matching (vs O(n) polygon tests)
--   - Known verifiable values at every step
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.spatial_demos
    COMMENT 'H3 spatial indexing and geographic analysis tables';
-- ============================================================================
-- TABLE 1: landmarks — 10 famous world landmarks with known coordinates
-- ============================================================================
-- Each landmark has a well-known lat/lng that produces a deterministic H3 cell.
-- These serve as ground truth for verifying coordinate conversions, grid
-- distances, cell hierarchy, area metrics, and boundary WKT generation.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.spatial_demos.landmarks (
    id INT,
    name VARCHAR,
    city VARCHAR,
    country VARCHAR,
    lat DOUBLE,
    lng DOUBLE
) LOCATION 'h3-gps-fleet-tracker/landmarks';


INSERT INTO {{zone_name}}.spatial_demos.landmarks VALUES
    (1,  'Golden Gate Bridge',     'San Francisco', 'USA',       37.8199, -122.4783),
    (2,  'Statue of Liberty',     'New York',      'USA',       40.6892,  -74.0445),
    (3,  'Eiffel Tower',          'Paris',         'France',    48.8584,    2.2945),
    (4,  'Big Ben',               'London',        'UK',        51.5007,   -0.1246),
    (5,  'Tokyo Tower',           'Tokyo',         'Japan',     35.6586,  139.7454),
    (6,  'Sydney Opera House',    'Sydney',        'Australia', -33.8568,  151.2153),
    (7,  'SF City Hall',          'San Francisco', 'USA',       37.7792, -122.4191),
    (8,  'Empire State Building', 'New York',      'USA',       40.7484,  -73.9857),
    (9,  'Tower of London',       'London',        'UK',        51.5081,   -0.0759),
    (10, 'Shibuya Crossing',      'Tokyo',         'Japan',     35.6595,  139.7004);

-- ============================================================================
-- TABLE 2: regions — 5 city boundaries as WKT polygons
-- ============================================================================
-- Bounding-box polygons for major cities. Each polygon is a closed ring in
-- WKT format suitable for h3_polyfill(). The polygons are intentionally
-- rectangular for deterministic cell counts at each H3 resolution.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.spatial_demos.regions (
    region_id INT,
    region_name VARCHAR,
    country VARCHAR,
    polygon_wkt VARCHAR,
    timezone VARCHAR
) LOCATION 'h3-gps-fleet-tracker/regions';


INSERT INTO {{zone_name}}.spatial_demos.regions VALUES
    (1, 'San Francisco', 'USA',    'POLYGON((-122.52 37.70, -122.35 37.70, -122.35 37.82, -122.52 37.82, -122.52 37.70))', 'America/Los_Angeles'),
    (2, 'Manhattan',     'USA',    'POLYGON((-74.02 40.70, -73.97 40.70, -73.97 40.80, -74.02 40.80, -74.02 40.70))',      'America/New_York'),
    (3, 'Central Paris', 'France', 'POLYGON((2.25 48.83, 2.42 48.83, 2.42 48.90, 2.25 48.90, 2.25 48.83))',               'Europe/Paris'),
    (4, 'Central London','UK',     'POLYGON((-0.20 51.48, 0.05 51.48, 0.05 51.55, -0.20 51.55, -0.20 51.48))',             'Europe/London'),
    (5, 'Central Tokyo', 'Japan',  'POLYGON((139.65 35.63, 139.80 35.63, 139.80 35.73, 139.65 35.73, 139.65 35.63))',      'Asia/Tokyo');

-- ============================================================================
-- TABLE 3: gps_points — 10,000 GPS pings across 5 cities
-- ============================================================================
-- Deterministic point generation using golden-ratio quasi-random distribution.
-- Each city gets 2,000 points uniformly scattered within its bounding box.
-- The formula (id * 0.618033...) % 1.0 produces a low-discrepancy sequence
-- ensuring even coverage without clustering artifacts.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.spatial_demos.gps_points (
    id BIGINT,
    lat DOUBLE,
    lng DOUBLE,
    device_id VARCHAR,
    city VARCHAR
) LOCATION 'h3-gps-fleet-tracker/gps_points';


-- San Francisco: 2,000 points within bounding box
INSERT INTO {{zone_name}}.spatial_demos.gps_points
SELECT
    id,
    37.70 + (0.12 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    -122.52 + (0.17 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'device_sf_' || (id % 50) AS device_id,
    'San Francisco' AS city
FROM generate_series(1, 2000) AS t(id);

-- Manhattan: 2,000 points
INSERT INTO {{zone_name}}.spatial_demos.gps_points
SELECT
    2000 + id,
    40.70 + (0.10 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    -74.02 + (0.05 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'device_nyc_' || (id % 50) AS device_id,
    'Manhattan' AS city
FROM generate_series(1, 2000) AS t(id);

-- Paris: 2,000 points
INSERT INTO {{zone_name}}.spatial_demos.gps_points
SELECT
    4000 + id,
    48.83 + (0.07 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    2.25 + (0.17 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'device_par_' || (id % 50) AS device_id,
    'Central Paris' AS city
FROM generate_series(1, 2000) AS t(id);

-- London: 2,000 points
INSERT INTO {{zone_name}}.spatial_demos.gps_points
SELECT
    6000 + id,
    51.48 + (0.07 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    -0.20 + (0.25 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'device_lon_' || (id % 50) AS device_id,
    'Central London' AS city
FROM generate_series(1, 2000) AS t(id);

-- Tokyo: 2,000 points
INSERT INTO {{zone_name}}.spatial_demos.gps_points
SELECT
    8000 + id,
    35.63 + (0.10 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    139.65 + (0.15 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'device_tky_' || (id % 50) AS device_id,
    'Central Tokyo' AS city
FROM generate_series(1, 2000) AS t(id);

-- ============================================================================
-- VIEW 4: points_h3 — GPS points enriched with H3 cell IDs (resolution 9)
-- ============================================================================
-- Adds an h3_cell column to each GPS point. Resolution 9 gives ~201 m edge
-- hexagons (~105,000 m² area) — appropriate for city-level analysis. The view is lazy so cells
-- are computed on read, not stored.
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.spatial_demos.points_h3 AS
SELECT
    id,
    lat,
    lng,
    device_id,
    city,
    h3_latlng_to_cell(lat, lng, 9) AS h3_cell
FROM {{zone_name}}.spatial_demos.gps_points;
-- ============================================================================
-- VIEW 5: region_cells — Regions expanded to H3 cell coverage
-- ============================================================================
-- Each region polygon is polyfilled at resolution 9, producing one row per
-- H3 cell that covers the region. This enables O(1) spatial joins by
-- matching h3_cell values directly (instead of point-in-polygon tests).
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.spatial_demos.region_cells AS
SELECT
    region_id,
    region_name,
    country,
    timezone,
    UNNEST(h3_polyfill(polygon_wkt, 9)) AS h3_cell
FROM {{zone_name}}.spatial_demos.regions;
