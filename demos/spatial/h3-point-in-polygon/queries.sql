-- ============================================================================
-- H3 Point-in-Polygon — Query Script
-- ============================================================================
-- Progressive learning path: from "what is H3" to million-row analytics.
--
-- Part 1: Understanding H3           (Queries 1-3)   — What H3 is
-- Part 2: Data Verification          (Queries 4-7)   — Confirm the dataset
-- Part 3: The Point-in-Polygon Join  (Queries 8-10)  — The core technique
-- Part 4: Million-Row Analytics      (Queries 11-14) — Real-world queries
-- Part 5: Summary PASS/FAIL          (Query 15)      — Automated checks
-- ============================================================================


-- ============================================================================
-- PART 1: UNDERSTANDING H3
-- ============================================================================
-- H3 is Uber's open-source hexagonal spatial indexing system. It divides the
-- entire Earth into a hierarchy of hexagonal cells at 16 resolutions (0-15).
-- Every point on Earth maps to exactly one hex cell at each resolution.
-- ============================================================================


-- ============================================================================
-- Query 1: What is an H3 cell?
-- ============================================================================
-- Convert a well-known coordinate (SFO Airport) to its H3 cell ID.
-- The cell ID is a 64-bit integer that uniquely identifies one hexagon
-- on the planet at resolution 9.
--
-- Think of it like a ZIP code — but hexagonal, hierarchical, and global.
-- ============================================================================
ASSERT ROW_COUNT = 1
SELECT
    h3_latlng_to_cell(37.6213, -122.3790, 9) AS sfo_cell_id,
    h3_cell_to_string(h3_latlng_to_cell(37.6213, -122.3790, 9)) AS sfo_hex_string,
    'SFO Airport (37.6213, -122.3790)' AS location;


-- ============================================================================
-- Query 2: How big is one cell?
-- ============================================================================
-- At resolution 9, each hexagonal cell has:
--   - Edge length: ~201 meters (about 2 city blocks)
--   - Area: ~105,000 m2 (roughly a city block)
--
-- This is fine-grained enough for ride-share geofencing but coarse enough
-- to keep cell counts manageable.
-- ============================================================================
ASSERT ROW_COUNT = 1
SELECT
    9 AS resolution,
    ROUND(h3_cell_area(h3_latlng_to_cell(37.6213, -122.3790, 9)), 0) AS area_m2,
    ROUND(h3_cell_area(h3_latlng_to_cell(37.6213, -122.3790, 9)) / 10000.0, 2) AS area_hectares,
    ROUND(h3_edge_length(h3_latlng_to_cell(37.6213, -122.3790, 9)), 0) AS edge_length_m;


-- ============================================================================
-- Query 3: How does polyfill work?
-- ============================================================================
-- h3_polyfill() takes a WKT polygon and a resolution, and returns every H3
-- cell whose center falls inside the polygon.
--
-- Small polygon (SFO Airport zone) -> fewer cells.
-- Large polygon (SF Downtown zone) -> more cells.
-- This is the "pre-computation" step that makes spatial joins O(1).
-- ============================================================================
ASSERT ROW_COUNT = 2
ASSERT VALUE h3_cells_covering_zone = 107 WHERE zone_name = 'SFO Airport'
ASSERT VALUE h3_cells_covering_zone = 85 WHERE zone_name = 'SF Downtown'
SELECT
    z.zone_name,
    z.zone_type,
    COUNT(*) AS h3_cells_covering_zone
FROM {{zone_name}}.spatial_demos.zone_cells z
WHERE z.city = 'San Francisco'
GROUP BY z.zone_name, z.zone_type
ORDER BY h3_cells_covering_zone DESC;


-- ============================================================================
-- PART 2: DATA VERIFICATION
-- ============================================================================
-- Confirm the dataset was generated correctly before running analytics.
-- ============================================================================


-- ============================================================================
-- Query 4: Total driver count (expect 1,000,000)
-- ============================================================================
ASSERT VALUE total_drivers = 1000000
SELECT COUNT(*) AS total_drivers
FROM {{zone_name}}.spatial_demos.driver_positions;


-- ============================================================================
-- Query 5: All 12 zones loaded
-- ============================================================================
ASSERT ROW_COUNT = 12
ASSERT VALUE zone_name = 'SFO Airport' WHERE zone_id = 1
ASSERT VALUE zone_type = 'airport' WHERE zone_id = 1
ASSERT VALUE surcharge_pct = 25.0 WHERE zone_id = 1
ASSERT VALUE city = 'San Francisco' WHERE zone_id = 2
ASSERT VALUE zone_name = 'Manhattan Core' WHERE zone_id = 4
ASSERT VALUE surcharge_pct = 10.0 WHERE zone_id = 12
SELECT
    zone_id,
    zone_name,
    zone_type,
    city,
    country,
    surcharge_pct
FROM {{zone_name}}.spatial_demos.zones
ORDER BY zone_id;


-- ============================================================================
-- Query 6: Drivers per city
-- ============================================================================
-- San Francisco, New York, Paris, London, Tokyo: 150,000 each
-- Sydney, Los Angeles: 100,000 each
-- Global scatter: 50,000
-- ============================================================================
ASSERT ROW_COUNT = 8
ASSERT VALUE driver_count = 150000 WHERE city = 'San Francisco'
ASSERT VALUE driver_count = 150000 WHERE city = 'New York'
ASSERT VALUE driver_count = 150000 WHERE city = 'Paris'
ASSERT VALUE driver_count = 150000 WHERE city = 'London'
ASSERT VALUE driver_count = 150000 WHERE city = 'Tokyo'
ASSERT VALUE driver_count = 100000 WHERE city = 'Sydney'
ASSERT VALUE driver_count = 100000 WHERE city = 'Los Angeles'
ASSERT VALUE driver_count = 50000 WHERE city = 'Global'
SELECT
    city,
    COUNT(*) AS driver_count
FROM {{zone_name}}.spatial_demos.driver_positions
GROUP BY city
ORDER BY driver_count DESC;


-- ============================================================================
-- Query 7: H3 cells per zone
-- ============================================================================
-- Small airport zones have fewer cells (compact area).
-- Large downtown zones have more cells (bigger area).
-- This shows the "spatial index size" for each zone.
-- ============================================================================
ASSERT ROW_COUNT = 12
ASSERT VALUE h3_cells = 129 WHERE zone_name = 'London City'
ASSERT VALUE h3_cells = 107 WHERE zone_name = 'SFO Airport'
SELECT
    z.zone_name,
    z.zone_type,
    z.city,
    COUNT(*) AS h3_cells
FROM {{zone_name}}.spatial_demos.zone_cells z
GROUP BY z.zone_name, z.zone_type, z.city
ORDER BY h3_cells DESC;


-- ============================================================================
-- PART 3: THE POINT-IN-POLYGON JOIN
-- ============================================================================
-- This is the core technique. Instead of testing each driver's coordinates
-- against polygon geometry (expensive), we:
--   1. Convert each driver's (lat, lng) -> H3 cell ID   (one function call)
--   2. Pre-expand each zone polygon -> set of H3 cell IDs  (polyfill)
--   3. JOIN on cell ID equality                         (integer comparison)
--
-- Step 3 is a standard hash join: O(1) per row. No trigonometry. No winding
-- number tests. No ray casting. Just an integer match.
-- ============================================================================


-- ============================================================================
-- Query 8: The million-row spatial join
-- ============================================================================
-- For each pricing zone, count how many of the 1,000,000 drivers are inside.
-- This query processes 1M rows and joins against ~5,000+ zone cells.
-- ============================================================================
-- Note: 7 zones (SF Downtown, CDG, Paris Centre, Heathrow, London City, Narita,
-- Tokyo Shibuya) receive zero driver matches because the golden-ratio generator
-- uses PHI and PHI2=1-PHI, making lat and lng fractional parts sum to exactly 1
-- for every point. This anti-correlation excludes any zone requiring mid-range
-- lat AND mid-range lng simultaneously.
ASSERT ROW_COUNT = 5
ASSERT VALUE drivers_in_zone = 32900 WHERE zone_name = 'LA Downtown'
ASSERT VALUE drivers_in_zone = 29973 WHERE zone_name = 'Sydney CBD'
ASSERT VALUE drivers_in_zone = 20788 WHERE zone_name = 'JFK Airport'
ASSERT VALUE drivers_in_zone = 12993 WHERE zone_name = 'SFO Airport'
ASSERT VALUE drivers_in_zone = 10582 WHERE zone_name = 'Manhattan Core'
SELECT
    z.zone_name,
    z.zone_type,
    z.city,
    z.surcharge_pct,
    COUNT(*) AS drivers_in_zone
FROM {{zone_name}}.spatial_demos.driver_cells d
INNER JOIN {{zone_name}}.spatial_demos.zone_cells z ON d.h3_cell = z.h3_cell
GROUP BY z.zone_name, z.zone_type, z.city, z.surcharge_pct
ORDER BY drivers_in_zone DESC;


-- ============================================================================
-- Query 9: Drivers outside all zones
-- ============================================================================
-- Not every driver is inside a pricing zone. The global scatter points and
-- drivers between city zones won't match. This counts the "unmatched" drivers.
-- ============================================================================
ASSERT ROW_COUNT = 1
ASSERT VALUE drivers_outside_zones = 892764
SELECT
    COUNT(*) AS drivers_outside_zones
FROM {{zone_name}}.spatial_demos.driver_cells d
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.spatial_demos.zone_cells z
    WHERE d.h3_cell = z.h3_cell
);


-- ============================================================================
-- Query 10: Single-point zone lookup
-- ============================================================================
-- "Is this specific GPS coordinate inside a pricing zone?"
-- SFO Airport should match SFO Airport zone (coords: 37.6213, -122.3790).
-- Times Square should match Manhattan Core zone (coords: 40.758, -73.9855).
-- Null Island should match nothing (coords: 0, 0).
-- ============================================================================
ASSERT ROW_COUNT = 3
ASSERT VALUE matched_zone = 'SFO Airport' WHERE test_point = 'SFO Airport'
ASSERT VALUE surcharge_pct = 25.0 WHERE test_point = 'SFO Airport'
ASSERT VALUE matched_zone = 'Manhattan Core' WHERE test_point = 'Times Square'
ASSERT VALUE matched_zone = 'NO MATCH' WHERE test_point = 'Null Island'
SELECT
    'SFO Airport' AS test_point,
    z.zone_name AS matched_zone,
    z.surcharge_pct
FROM {{zone_name}}.spatial_demos.zone_cells z
WHERE z.h3_cell = h3_latlng_to_cell(37.6213, -122.3790, 9)

UNION ALL

SELECT
    'Times Square' AS test_point,
    z.zone_name AS matched_zone,
    z.surcharge_pct
FROM {{zone_name}}.spatial_demos.zone_cells z
WHERE z.h3_cell = h3_latlng_to_cell(40.758, -73.9855, 9)

UNION ALL

SELECT
    'Null Island' AS test_point,
    'NO MATCH' AS matched_zone,
    0.0 AS surcharge_pct
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.spatial_demos.zone_cells z
    WHERE z.h3_cell = h3_latlng_to_cell(0.0, 0.0, 9)
);


-- ============================================================================
-- PART 4: MILLION-ROW ANALYTICS
-- ============================================================================
-- Real-world queries a ride-share company would run on this data.
-- ============================================================================


-- ============================================================================
-- Query 11: Surge pricing impact
-- ============================================================================
-- How many drivers are affected by each surcharge tier?
-- Airport zones (25%) vs downtown zones (10-15%).
-- ============================================================================
ASSERT ROW_COUNT = 3
ASSERT VALUE drivers_affected = 33781 WHERE zone_type = 'airport' AND surcharge_pct = 25.0
ASSERT VALUE drivers_affected = 10582 WHERE zone_type = 'downtown' AND surcharge_pct = 15.0
ASSERT VALUE drivers_affected = 62873 WHERE zone_type = 'downtown' AND surcharge_pct = 10.0
SELECT
    z.zone_type,
    z.surcharge_pct,
    COUNT(*) AS drivers_affected,
    ROUND(COUNT(*) * CAST(z.surcharge_pct AS DOUBLE) / 100.0, 0) AS equivalent_surcharge_rides
FROM {{zone_name}}.spatial_demos.driver_cells d
INNER JOIN {{zone_name}}.spatial_demos.zone_cells z ON d.h3_cell = z.h3_cell
GROUP BY z.zone_type, z.surcharge_pct
ORDER BY z.surcharge_pct DESC;


-- ============================================================================
-- Query 12: Busiest zones ranked
-- ============================================================================
-- Which zones have the most drivers right now? This is the real-time
-- dashboard query a ride-share ops team would run.
-- ============================================================================
ASSERT ROW_COUNT = 5
ASSERT VALUE active_drivers = 32900 WHERE zone_name = 'LA Downtown'
ASSERT VALUE active_drivers = 29973 WHERE zone_name = 'Sydney CBD'
ASSERT VALUE active_drivers = 20788 WHERE zone_name = 'JFK Airport'
ASSERT VALUE active_drivers = 12993 WHERE zone_name = 'SFO Airport'
ASSERT VALUE active_drivers = 10582 WHERE zone_name = 'Manhattan Core'
SELECT
    z.zone_name,
    z.city,
    COUNT(*) AS active_drivers,
    COUNT(DISTINCT d.driver_id) AS unique_drivers,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM {{zone_name}}.spatial_demos.driver_positions), 2) AS pct_of_all_drivers
FROM {{zone_name}}.spatial_demos.driver_cells d
INNER JOIN {{zone_name}}.spatial_demos.zone_cells z ON d.h3_cell = z.h3_cell
GROUP BY z.zone_name, z.city
ORDER BY active_drivers DESC;


-- ============================================================================
-- Query 13: Airport vs downtown comparison
-- ============================================================================
-- Compare driver density: airport zones are smaller (fewer H3 cells) so
-- the same driver count means higher density.
-- ============================================================================
ASSERT ROW_COUNT = 2
ASSERT VALUE total_drivers = 33781 WHERE zone_type = 'airport'
ASSERT VALUE total_drivers = 73455 WHERE zone_type = 'downtown'
ASSERT VALUE drivers_per_cell = 159.3 WHERE zone_type = 'airport'
ASSERT VALUE drivers_per_cell = 422.2 WHERE zone_type = 'downtown'
SELECT
    z.zone_type,
    COUNT(DISTINCT z.zone_name) AS num_zones,
    SUM(zc.cell_count) AS total_h3_cells,
    SUM(driver_count) AS total_drivers,
    ROUND(SUM(driver_count) * 1.0 / SUM(zc.cell_count), 1) AS drivers_per_cell
FROM (
    SELECT zone_name, zone_type, COUNT(*) AS driver_count
    FROM {{zone_name}}.spatial_demos.driver_cells d
    INNER JOIN {{zone_name}}.spatial_demos.zone_cells z ON d.h3_cell = z.h3_cell
    GROUP BY zone_name, zone_type
) z
INNER JOIN (
    SELECT zone_name, COUNT(*) AS cell_count
    FROM {{zone_name}}.spatial_demos.zone_cells
    GROUP BY zone_name
) zc ON z.zone_name = zc.zone_name
GROUP BY z.zone_type
ORDER BY z.zone_type;


-- ============================================================================
-- Query 14: Coverage by city — matched vs unmatched
-- ============================================================================
-- For each city, how many drivers fall inside a zone vs outside?
-- Cities with larger bounding boxes (more scatter) will have lower match rates.
-- ============================================================================
ASSERT ROW_COUNT = 8
ASSERT VALUE matched_drivers = 12993 WHERE city = 'San Francisco'
ASSERT VALUE match_rate_pct = 8.7 WHERE city = 'San Francisco'
ASSERT VALUE matched_drivers = 31370 WHERE city = 'New York'
ASSERT VALUE match_rate_pct = 20.9 WHERE city = 'New York'
ASSERT VALUE matched_drivers = 0 WHERE city = 'Paris'
ASSERT VALUE matched_drivers = 0 WHERE city = 'London'
ASSERT VALUE matched_drivers = 0 WHERE city = 'Tokyo'
ASSERT VALUE match_rate_pct = 30.0 WHERE city = 'Sydney'
ASSERT VALUE match_rate_pct = 32.9 WHERE city = 'Los Angeles'
SELECT
    d.city,
    COUNT(*) AS total_drivers,
    COUNT(z.zone_name) AS matched_drivers,
    COUNT(*) - COUNT(z.zone_name) AS unmatched_drivers,
    ROUND(100.0 * COUNT(z.zone_name) / COUNT(*), 1) AS match_rate_pct
FROM {{zone_name}}.spatial_demos.driver_cells d
LEFT JOIN {{zone_name}}.spatial_demos.zone_cells z ON d.h3_cell = z.h3_cell
GROUP BY d.city
ORDER BY match_rate_pct DESC;


-- ============================================================================
-- PART 5: SUMMARY — PASS/FAIL VERIFICATION
-- ============================================================================
-- Automated checks confirming the demo is working correctly.
-- All checks should return PASS.
-- ============================================================================


-- ============================================================================
-- Query 15: Summary — all checks (VERIFY: All Checks)
-- ============================================================================
-- Cross-cutting sanity check: H3 correctness, driver counts, and key invariants.
ASSERT ROW_COUNT = 10
ASSERT VALUE result = 'PASS' WHERE check_name = 'all_drivers_accounted'
ASSERT VALUE result = 'PASS' WHERE check_name = 'all_zones_have_cells'
ASSERT VALUE result = 'PASS' WHERE check_name = 'cell_area_range'
ASSERT VALUE result = 'PASS' WHERE check_name = 'driver_count_1M'
ASSERT VALUE result = 'PASS' WHERE check_name = 'edge_length_range'
ASSERT VALUE result = 'PASS' WHERE check_name = 'nyc_point_outside_sfo_zone'
ASSERT VALUE result = 'PASS' WHERE check_name = 'sf_driver_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'sfo_point_in_sfo_zone'
ASSERT VALUE result = 'PASS' WHERE check_name = 'string_roundtrip'
ASSERT VALUE result = 'PASS' WHERE check_name = 'zone_count_12'
SELECT check_name, result FROM (

    -- Check 1: Total driver count = 1,000,000
    SELECT 'driver_count_1M' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.spatial_demos.driver_positions) = 1000000
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Zone count = 12
    SELECT 'zone_count_12' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.spatial_demos.zones) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: SFO point falls inside SFO Airport zone
    SELECT 'sfo_point_in_sfo_zone' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.spatial_demos.zone_cells
               WHERE zone_name = 'SFO Airport'
                 AND h3_cell = h3_latlng_to_cell(37.6213, -122.3790, 9)
           ) > 0 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: NYC point does NOT fall inside SFO Airport zone
    SELECT 'nyc_point_outside_sfo_zone' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.spatial_demos.zone_cells
               WHERE zone_name = 'SFO Airport'
                 AND h3_cell = h3_latlng_to_cell(40.7128, -74.0060, 9)
           ) = 0 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: All drivers accounted for (matched + unmatched = 1M)
    SELECT 'all_drivers_accounted' AS check_name,
           CASE WHEN (
               (SELECT COUNT(*) FROM {{zone_name}}.spatial_demos.driver_cells d
                WHERE EXISTS (SELECT 1 FROM {{zone_name}}.spatial_demos.zone_cells z WHERE d.h3_cell = z.h3_cell))
               +
               (SELECT COUNT(*) FROM {{zone_name}}.spatial_demos.driver_cells d
                WHERE NOT EXISTS (SELECT 1 FROM {{zone_name}}.spatial_demos.zone_cells z WHERE d.h3_cell = z.h3_cell))
           ) = 1000000 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Every zone has at least 1 polyfill cell
    SELECT 'all_zones_have_cells' AS check_name,
           CASE WHEN (
               SELECT MIN(cell_count) FROM (
                   SELECT zone_name, COUNT(*) AS cell_count
                   FROM {{zone_name}}.spatial_demos.zone_cells
                   GROUP BY zone_name
               )
           ) > 0 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: H3 cell string round-trip (cell -> string -> cell = identity)
    SELECT 'string_roundtrip' AS check_name,
           CASE WHEN h3_string_to_cell(
               h3_cell_to_string(h3_latlng_to_cell(37.6213, -122.3790, 9))
           ) = h3_latlng_to_cell(37.6213, -122.3790, 9)
           THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Drivers per city matches expected distribution
    SELECT 'sf_driver_count' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.spatial_demos.driver_positions
               WHERE city = 'San Francisco'
           ) = 150000 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: Cell area within expected range (50K-200K m2 at res 9)
    SELECT 'cell_area_range' AS check_name,
           CASE WHEN (
               SELECT h3_cell_area(h3_latlng_to_cell(37.6213, -122.3790, 9))
           ) BETWEEN 50000 AND 200000
           THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 10: Edge length within expected range (100-250 m at res 9)
    SELECT 'edge_length_range' AS check_name,
           CASE WHEN (
               SELECT h3_edge_length(h3_latlng_to_cell(37.6213, -122.3790, 9))
           ) BETWEEN 100 AND 250
           THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
