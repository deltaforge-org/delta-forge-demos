-- ============================================================================
-- H3 Point-in-Polygon — Setup Script
-- ============================================================================
-- Creates Delta tables for a ride-share geofencing scenario: 12 pricing zones
-- across 8 world cities and 1,000,000 driver GPS positions. H3 hexagonal
-- indexing converts the O(n×m) point-in-polygon problem into O(1) integer
-- equality joins.
--
--   1. zones             — 12 pricing zones as WKT polygons
--   2. driver_positions  — 1,000,000 GPS pings from drivers
--   3. driver_cells      — VIEW: drivers + H3 cell ID (lazy, computed on read)
--   4. zone_cells        — VIEW: zones expanded to H3 cells via polyfill
--
-- All data is generated inline — no external files needed.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.spatial_demos
    COMMENT 'H3 spatial indexing and geographic analysis tables';
-- ============================================================================
-- TABLE 1: zones — 12 pricing zones across 8 world cities
-- ============================================================================
-- Each zone is a rectangular WKT polygon representing a real-world area:
--   - Airport zones (small, high surcharge): SFO, JFK, CDG, Heathrow, Narita
--   - Downtown zones (medium, moderate surcharge): SF, Manhattan, Paris, London,
--     Tokyo Shibuya, Sydney CBD, LA Downtown
--
-- Coordinates verified against real-world geography (Feb 2026).
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.spatial_demos.zones (
    zone_id INT,
    zone_name VARCHAR,
    zone_type VARCHAR,
    city VARCHAR,
    country VARCHAR,
    polygon_wkt VARCHAR,
    surcharge_pct DOUBLE
) LOCATION 'h3-point-in-polygon/pip_zones';


INSERT INTO {{zone_name}}.spatial_demos.zones VALUES
    -- San Francisco
    (1,  'SFO Airport',      'airport',  'San Francisco', 'USA',
     'POLYGON((-122.40 37.60, -122.36 37.60, -122.36 37.63, -122.40 37.63, -122.40 37.60))',
     25.0),
    (2,  'SF Downtown',      'downtown', 'San Francisco', 'USA',
     'POLYGON((-122.42 37.77, -122.39 37.77, -122.39 37.80, -122.42 37.80, -122.42 37.77))',
     15.0),
    -- New York
    (3,  'JFK Airport',      'airport',  'New York',      'USA',
     'POLYGON((-73.80 40.63, -73.76 40.63, -73.76 40.66, -73.80 40.66, -73.80 40.63))',
     25.0),
    (4,  'Manhattan Core',   'downtown', 'New York',      'USA',
     'POLYGON((-74.00 40.74, -73.97 40.74, -73.97 40.78, -74.00 40.78, -74.00 40.74))',
     15.0),
    -- Paris
    (5,  'CDG Airport',      'airport',  'Paris',         'France',
     'POLYGON((2.53 49.00, 2.57 49.00, 2.57 49.02, 2.53 49.02, 2.53 49.00))',
     20.0),
    (6,  'Paris Centre',     'downtown', 'Paris',         'France',
     'POLYGON((2.33 48.85, 2.37 48.85, 2.37 48.87, 2.33 48.87, 2.33 48.85))',
     10.0),
    -- London
    (7,  'Heathrow Airport', 'airport',  'London',        'UK',
     'POLYGON((-0.49 51.46, -0.44 51.46, -0.44 51.48, -0.49 51.48, -0.49 51.46))',
     20.0),
    (8,  'London City',      'downtown', 'London',        'UK',
     'POLYGON((-0.10 51.50, -0.02 51.50, -0.02 51.52, -0.10 51.52, -0.10 51.50))',
     10.0),
    -- Tokyo
    (9,  'Narita Airport',   'airport',  'Tokyo',         'Japan',
     'POLYGON((140.37 35.76, 140.40 35.76, 140.40 35.78, 140.37 35.78, 140.37 35.76))',
     20.0),
    (10, 'Tokyo Shibuya',    'downtown', 'Tokyo',         'Japan',
     'POLYGON((139.69 35.65, 139.72 35.65, 139.72 35.67, 139.69 35.67, 139.69 35.65))',
     10.0),
    -- Sydney
    (11, 'Sydney CBD',       'downtown', 'Sydney',        'Australia',
     'POLYGON((151.20 -33.88, 151.22 -33.88, 151.22 -33.86, 151.20 -33.86, 151.20 -33.88))',
     10.0),
    -- Los Angeles
    (12, 'LA Downtown',      'downtown', 'Los Angeles',   'USA',
     'POLYGON((-118.26 34.04, -118.24 34.04, -118.24 34.06, -118.26 34.06, -118.26 34.04))',
     10.0);
-- ============================================================================
-- TABLE 2: driver_positions — 1,000,000 GPS pings from ride-share drivers
-- ============================================================================
-- Deterministic generation using golden-ratio quasi-random distribution.
-- Each city cluster produces points uniformly within a bounding box that
-- ENCLOSES its zones (so most — but not all — points fall inside a zone).
--
-- Distribution:
--   San Francisco  150,000 points  (IDs       1 – 150,000)
--   New York       150,000 points  (IDs 150,001 – 300,000)
--   Paris          150,000 points  (IDs 300,001 – 450,000)
--   London         150,000 points  (IDs 450,001 – 600,000)
--   Tokyo          150,000 points  (IDs 600,001 – 750,000)
--   Sydney         100,000 points  (IDs 750,001 – 850,000)
--   Los Angeles    100,000 points  (IDs 850,001 – 950,000)
--   Global scatter  50,000 points  (IDs 950,001 – 1,000,000)
--                 ---------
--   Total:       1,000,000
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.spatial_demos.driver_positions (
    id BIGINT,
    lat DOUBLE,
    lng DOUBLE,
    driver_id VARCHAR,
    city VARCHAR
) LOCATION 'h3-point-in-polygon/pip_driver_positions';


-- San Francisco: 150,000 points (bbox covers SFO Airport + SF Downtown)
-- Lat: 37.58–37.82, Lng: -122.52–-122.34
INSERT INTO {{zone_name}}.spatial_demos.driver_positions
SELECT
    id,
    37.58 + (0.24 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    -122.52 + (0.18 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'driver_sf_' || (id % 200) AS driver_id,
    'San Francisco' AS city
FROM generate_series(1, 150000) AS t(id);

-- New York: 150,000 points (bbox covers JFK Airport + Manhattan Core)
-- Lat: 40.62–40.80, Lng: -74.02–-73.74
INSERT INTO {{zone_name}}.spatial_demos.driver_positions
SELECT
    150000 + id,
    40.62 + (0.18 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    -74.02 + (0.28 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'driver_nyc_' || (id % 200) AS driver_id,
    'New York' AS city
FROM generate_series(1, 150000) AS t(id);

-- Paris: 150,000 points (bbox covers CDG Airport + Paris Centre)
-- Lat: 48.84–49.04, Lng: 2.30–2.60
INSERT INTO {{zone_name}}.spatial_demos.driver_positions
SELECT
    300000 + id,
    48.84 + (0.20 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    2.30 + (0.30 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'driver_par_' || (id % 200) AS driver_id,
    'Paris' AS city
FROM generate_series(1, 150000) AS t(id);

-- London: 150,000 points (bbox covers Heathrow + London City)
-- Lat: 51.44–51.54, Lng: -0.52–0.06
INSERT INTO {{zone_name}}.spatial_demos.driver_positions
SELECT
    450000 + id,
    51.44 + (0.10 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    -0.52 + (0.58 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'driver_lon_' || (id % 200) AS driver_id,
    'London' AS city
FROM generate_series(1, 150000) AS t(id);

-- Tokyo: 150,000 points (bbox covers Narita Airport + Shibuya)
-- Lat: 35.64–35.80, Lng: 139.68–140.42
INSERT INTO {{zone_name}}.spatial_demos.driver_positions
SELECT
    600000 + id,
    35.64 + (0.16 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    139.68 + (0.74 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'driver_tky_' || (id % 200) AS driver_id,
    'Tokyo' AS city
FROM generate_series(1, 150000) AS t(id);

-- Sydney: 100,000 points (bbox covers Sydney CBD area)
-- Lat: -33.90–-33.84, Lng: 151.18–151.24
INSERT INTO {{zone_name}}.spatial_demos.driver_positions
SELECT
    750000 + id,
    -33.90 + (0.06 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    151.18 + (0.06 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'driver_syd_' || (id % 200) AS driver_id,
    'Sydney' AS city
FROM generate_series(1, 100000) AS t(id);

-- Los Angeles: 100,000 points (bbox covers LA Downtown area)
-- Lat: 34.02–34.08, Lng: -118.28–-118.22
INSERT INTO {{zone_name}}.spatial_demos.driver_positions
SELECT
    850000 + id,
    34.02 + (0.06 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    -118.28 + (0.06 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'driver_la_' || (id % 200) AS driver_id,
    'Los Angeles' AS city
FROM generate_series(1, 100000) AS t(id);

-- Global scatter: 50,000 points across the world (outside any zone)
-- Lat: -50 to 60, Lng: -170 to 170
INSERT INTO {{zone_name}}.spatial_demos.driver_positions
SELECT
    950000 + id,
    -50.0 + (110.0 * ((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0)) AS lat,
    -170.0 + (340.0 * ((CAST(id AS DOUBLE) * 0.381966011250105) % 1.0)) AS lng,
    'driver_global_' || (id % 200) AS driver_id,
    'Global' AS city
FROM generate_series(1, 50000) AS t(id);
-- ============================================================================
-- VIEW 3: driver_cells — Drivers enriched with H3 cell IDs (resolution 9)
-- ============================================================================
-- Adds an h3_cell column to each driver position. Resolution 9 gives ~201 m
-- edge hexagons (~105,000 m² area). The view is lazy: cells are computed on
-- read, not stored. This is the "index" side of the point-in-polygon trick.
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.spatial_demos.driver_cells AS
SELECT
    id,
    lat,
    lng,
    driver_id,
    city,
    h3_latlng_to_cell(lat, lng, 9) AS h3_cell
FROM {{zone_name}}.spatial_demos.driver_positions;
-- ============================================================================
-- VIEW 4: zone_cells — Zones expanded to H3 cell coverage
-- ============================================================================
-- Each zone polygon is polyfilled at resolution 9, producing one row per H3
-- cell that covers the zone. JOIN driver_cells.h3_cell = zone_cells.h3_cell
-- gives O(1) point-in-polygon — no geometry math at query time.
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.spatial_demos.zone_cells AS
SELECT
    zone_id,
    zone_name,
    zone_type,
    city,
    country,
    surcharge_pct,
    UNNEST(h3_polyfill(polygon_wkt, 9)) AS h3_cell
FROM {{zone_name}}.spatial_demos.zones;
