-- ============================================================================
-- Iceberg V2 Fleet Telemetry — Queries
-- ============================================================================
-- Demonstrates native Iceberg format-version 2 table reading: schema
-- inference from v2 metadata with enhanced column statistics, manifest-based
-- file discovery, fleet analytics, safety monitoring, and fuel analysis.
-- All queries are read-only.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Total Row Count
-- ============================================================================
-- Verifies that DeltaForge discovered the Parquet data file via the
-- Iceberg v2 manifest chain (metadata.json → manifest list → manifest → file).

ASSERT ROW_COUNT = 450
SELECT * FROM {{zone_name}}.iceberg_demos.fleet_telemetry;


-- ============================================================================
-- Query 2: Schema Inference from Iceberg V2 Metadata
-- ============================================================================
-- The schema comes from the v2 metadata.json. This query exercises all 13
-- columns to prove correct Iceberg→Arrow type mapping including boolean.

ASSERT ROW_COUNT = 450
ASSERT VALUE fleet = 'West-Coast' WHERE vehicle_id = 'VH-0001'
ASSERT VALUE vehicle_type = 'Delivery-Van' WHERE vehicle_id = 'VH-0001'
ASSERT VALUE driver_id = 'DRV-141' WHERE vehicle_id = 'VH-0001'
SELECT
    vehicle_id,
    fleet,
    vehicle_type,
    driver_id,
    latitude,
    longitude,
    speed_mph,
    fuel_level_pct,
    engine_temp_f,
    odometer_miles,
    idle_minutes,
    harsh_braking,
    route_id
FROM {{zone_name}}.iceberg_demos.fleet_telemetry
ORDER BY vehicle_id;


-- ============================================================================
-- Query 3: Per-Fleet Row Counts
-- ============================================================================
-- Three regional fleets with 150 telemetry pings each.

ASSERT ROW_COUNT = 3
ASSERT VALUE ping_count = 150 WHERE fleet = 'East-Coast'
ASSERT VALUE ping_count = 150 WHERE fleet = 'Midwest'
ASSERT VALUE ping_count = 150 WHERE fleet = 'West-Coast'
SELECT
    fleet,
    COUNT(*) AS ping_count
FROM {{zone_name}}.iceberg_demos.fleet_telemetry
GROUP BY fleet
ORDER BY fleet;


-- ============================================================================
-- Query 4: Vehicle Type Distribution
-- ============================================================================
-- Three vehicle types across all fleets.

ASSERT ROW_COUNT = 3
ASSERT VALUE vehicle_count = 132 WHERE vehicle_type = 'Box-Truck'
ASSERT VALUE vehicle_count = 156 WHERE vehicle_type = 'Delivery-Van'
ASSERT VALUE vehicle_count = 162 WHERE vehicle_type = 'Semi-Truck'
SELECT
    vehicle_type,
    COUNT(*) AS vehicle_count
FROM {{zone_name}}.iceberg_demos.fleet_telemetry
GROUP BY vehicle_type
ORDER BY vehicle_type;


-- ============================================================================
-- Query 5: Average Speed by Fleet
-- ============================================================================
-- Floating-point aggregation proving correct numeric handling in v2.

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_speed = 37.27 WHERE fleet = 'East-Coast'
ASSERT VALUE avg_speed = 37.89 WHERE fleet = 'Midwest'
ASSERT VALUE avg_speed = 37.13 WHERE fleet = 'West-Coast'
SELECT
    fleet,
    ROUND(AVG(speed_mph), 2) AS avg_speed
FROM {{zone_name}}.iceberg_demos.fleet_telemetry
GROUP BY fleet
ORDER BY fleet;


-- ============================================================================
-- Query 6: Total Idle Minutes by Fleet
-- ============================================================================
-- Fleet operational efficiency metric. Higher idle = more fuel waste.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_idle = 3355 WHERE fleet = 'East-Coast'
ASSERT VALUE total_idle = 3439 WHERE fleet = 'Midwest'
ASSERT VALUE total_idle = 3751 WHERE fleet = 'West-Coast'
SELECT
    fleet,
    SUM(idle_minutes) AS total_idle
FROM {{zone_name}}.iceberg_demos.fleet_telemetry
GROUP BY fleet
ORDER BY fleet;


-- ============================================================================
-- Query 7: Harsh Braking Events — Safety Analytics
-- ============================================================================
-- Counts harsh braking incidents per fleet. Boolean column aggregation
-- exercises Iceberg v2 boolean type reading.

ASSERT ROW_COUNT = 3
ASSERT VALUE harsh_events = 27 WHERE fleet = 'East-Coast'
ASSERT VALUE harsh_events = 20 WHERE fleet = 'Midwest'
ASSERT VALUE harsh_events = 31 WHERE fleet = 'West-Coast'
SELECT
    fleet,
    SUM(CASE WHEN harsh_braking THEN 1 ELSE 0 END) AS harsh_events
FROM {{zone_name}}.iceberg_demos.fleet_telemetry
GROUP BY fleet
ORDER BY fleet;


-- ============================================================================
-- Query 8: Speeding Vehicles (> 65 mph)
-- ============================================================================
-- Predicate pushdown on integer column. Identifies vehicles exceeding
-- the speed threshold — a key fleet safety compliance metric.

ASSERT ROW_COUNT = 53
SELECT
    vehicle_id,
    fleet,
    vehicle_type,
    speed_mph,
    route_id
FROM {{zone_name}}.iceberg_demos.fleet_telemetry
WHERE speed_mph > 65
ORDER BY speed_mph DESC;


-- ============================================================================
-- Query 9: Low Fuel Alerts (< 20%)
-- ============================================================================
-- Vehicles at risk of running empty. Exercises integer predicate filtering.

ASSERT ROW_COUNT = 74
SELECT
    vehicle_id,
    fleet,
    fuel_level_pct,
    route_id
FROM {{zone_name}}.iceberg_demos.fleet_telemetry
WHERE fuel_level_pct < 20
ORDER BY fuel_level_pct ASC;


-- ============================================================================
-- Query 10: High Engine Temperature (> 220°F)
-- ============================================================================
-- Overheating risk detection. Exercises predicate evaluation on int columns.

ASSERT ROW_COUNT = 157
SELECT
    vehicle_id,
    fleet,
    vehicle_type,
    engine_temp_f,
    speed_mph
FROM {{zone_name}}.iceberg_demos.fleet_telemetry
WHERE engine_temp_f > 220
ORDER BY engine_temp_f DESC;


-- ============================================================================
-- Query 11: Distinct Counts
-- ============================================================================
-- Exercises COUNT(DISTINCT ...) across the full dataset.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_vehicles = 450
ASSERT VALUE distinct_drivers = 98
ASSERT VALUE distinct_routes = 15
SELECT
    COUNT(DISTINCT vehicle_id) AS distinct_vehicles,
    COUNT(DISTINCT driver_id) AS distinct_drivers,
    COUNT(DISTINCT route_id) AS distinct_routes
FROM {{zone_name}}.iceberg_demos.fleet_telemetry;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, grand totals, and key invariants.
-- A user who runs only this query can verify the Iceberg v2 reader works.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 450
ASSERT VALUE fleet_count = 3
ASSERT VALUE total_harsh_braking = 78
ASSERT VALUE speeding_count = 53
ASSERT VALUE low_fuel_count = 74
ASSERT VALUE distinct_vehicles = 450
ASSERT VALUE distinct_routes = 15
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT fleet) AS fleet_count,
    SUM(CASE WHEN harsh_braking THEN 1 ELSE 0 END) AS total_harsh_braking,
    SUM(CASE WHEN speed_mph > 65 THEN 1 ELSE 0 END) AS speeding_count,
    SUM(CASE WHEN fuel_level_pct < 20 THEN 1 ELSE 0 END) AS low_fuel_count,
    COUNT(DISTINCT vehicle_id) AS distinct_vehicles,
    COUNT(DISTINCT route_id) AS distinct_routes
FROM {{zone_name}}.iceberg_demos.fleet_telemetry;
