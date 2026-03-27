-- ============================================================================
-- Iceberg V2 Position Deletes — Queries
-- ============================================================================
-- Validates that Delta Forge correctly applies Iceberg V2 position delete
-- vectors when reading the table. The original dataset has 600 rows; 30 rows
-- from faulty sensor SENSOR-F01 were retracted. Every query must reflect the
-- corrected 570-row dataset — if position deletes are not applied, assertions
-- will fail.
-- ============================================================================


-- ============================================================================
-- Query 1: Post-Delete Row Count
-- ============================================================================
-- The data file has 600 rows but 30 are marked as deleted in the position
-- delete file. The reader must return exactly 570.

ASSERT ROW_COUNT = 570
SELECT * FROM {{zone_name}}.iceberg.cold_chain_readings;


-- ============================================================================
-- Query 2: Faulty Sensor Completely Removed
-- ============================================================================
-- SENSOR-F01 had 30 readings, all deleted. It must not appear at all.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg.cold_chain_readings
WHERE sensor_id = 'SENSOR-F01';


-- ============================================================================
-- Query 3: Per-Route Reading Counts
-- ============================================================================
-- ROUTE-A originally had 150 readings; 30 were from the faulty sensor.
-- After deletes: ROUTE-A = 120, others unchanged at 150.

ASSERT ROW_COUNT = 4
ASSERT VALUE reading_count = 120 WHERE route = 'ROUTE-A'
ASSERT VALUE reading_count = 150 WHERE route = 'ROUTE-B'
ASSERT VALUE reading_count = 150 WHERE route = 'ROUTE-C'
ASSERT VALUE reading_count = 150 WHERE route = 'ROUTE-D'
SELECT
    route,
    COUNT(*) AS reading_count
FROM {{zone_name}}.iceberg.cold_chain_readings
GROUP BY route
ORDER BY route;


-- ============================================================================
-- Query 4: Distinct Sensor Count
-- ============================================================================
-- 20 normal sensors remain. SENSOR-F01 (the 21st) is fully deleted.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_sensors = 20
SELECT
    COUNT(DISTINCT sensor_id) AS distinct_sensors
FROM {{zone_name}}.iceberg.cold_chain_readings;


-- ============================================================================
-- Query 5: Vaccine Type Distribution
-- ============================================================================
-- ROUTE-A carried mRNA-COVID; its count drops from 150 to 120.

ASSERT ROW_COUNT = 4
ASSERT VALUE reading_count = 150 WHERE vaccine_type = 'HPV-9v'
ASSERT VALUE reading_count = 150 WHERE vaccine_type = 'Influenza-Quad'
ASSERT VALUE reading_count = 150 WHERE vaccine_type = 'Tdap'
ASSERT VALUE reading_count = 120 WHERE vaccine_type = 'mRNA-COVID'
SELECT
    vaccine_type,
    COUNT(*) AS reading_count
FROM {{zone_name}}.iceberg.cold_chain_readings
GROUP BY vaccine_type
ORDER BY vaccine_type;


-- ============================================================================
-- Query 6: Temperature Excursions by Route
-- ============================================================================
-- The faulty sensor always read above 8C (all excursions). Removing it
-- reduces ROUTE-A excursions. Other routes are unaffected.

ASSERT ROW_COUNT = 4
ASSERT VALUE excursion_count = 45 WHERE route = 'ROUTE-A'
ASSERT VALUE excursion_count = 45 WHERE route = 'ROUTE-B'
ASSERT VALUE excursion_count = 57 WHERE route = 'ROUTE-C'
ASSERT VALUE excursion_count = 63 WHERE route = 'ROUTE-D'
SELECT
    route,
    SUM(CASE WHEN temp_excursion THEN 1 ELSE 0 END) AS excursion_count
FROM {{zone_name}}.iceberg.cold_chain_readings
GROUP BY route
ORDER BY route;


-- ============================================================================
-- Query 7: Average Temperature by Route
-- ============================================================================
-- With faulty high-temp readings removed, ROUTE-A average drops.

ASSERT ROW_COUNT = 4
ASSERT VALUE avg_temp = -0.24 WHERE route = 'ROUTE-A'
ASSERT VALUE avg_temp = 0.81 WHERE route = 'ROUTE-B'
ASSERT VALUE avg_temp = 0.21 WHERE route = 'ROUTE-C'
ASSERT VALUE avg_temp = -0.29 WHERE route = 'ROUTE-D'
SELECT
    route,
    ROUND(AVG(temperature_c), 2) AS avg_temp
FROM {{zone_name}}.iceberg.cold_chain_readings
GROUP BY route
ORDER BY route;


-- ============================================================================
-- Query 8: Low Battery Alerts (<= 25%)
-- ============================================================================
-- Sensor battery status — unaffected by the delete since faulty sensor
-- battery readings are also removed.

ASSERT ROW_COUNT = 70
SELECT
    reading_id,
    sensor_id,
    route,
    battery_pct
FROM {{zone_name}}.iceberg.cold_chain_readings
WHERE battery_pct <= 25
ORDER BY battery_pct ASC;


-- ============================================================================
-- Query 9: Temperature Range (Overall)
-- ============================================================================
-- With faulty readings removed, the max temp should be within normal range
-- (not the 8.96-14.20 range of the faulty sensor).

ASSERT ROW_COUNT = 1
ASSERT VALUE min_temp = -7.95
ASSERT VALUE max_temp = 7.98
ASSERT VALUE avg_temp = 0.14
SELECT
    ROUND(MIN(temperature_c), 2) AS min_temp,
    ROUND(MAX(temperature_c), 2) AS max_temp,
    ROUND(AVG(temperature_c), 2) AS avg_temp
FROM {{zone_name}}.iceberg.cold_chain_readings;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check covering every key invariant of the position
-- delete scenario. A single query that fails if deletes are not applied.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 570
ASSERT VALUE route_count = 4
ASSERT VALUE sensor_count = 20
ASSERT VALUE faulty_sensor_rows = 0
ASSERT VALUE total_excursions = 210
ASSERT VALUE low_battery_count = 70
ASSERT VALUE vaccine_count = 4
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT route) AS route_count,
    COUNT(DISTINCT sensor_id) AS sensor_count,
    SUM(CASE WHEN sensor_id = 'SENSOR-F01' THEN 1 ELSE 0 END) AS faulty_sensor_rows,
    SUM(CASE WHEN temp_excursion THEN 1 ELSE 0 END) AS total_excursions,
    SUM(CASE WHEN battery_pct <= 25 THEN 1 ELSE 0 END) AS low_battery_count,
    COUNT(DISTINCT vaccine_type) AS vaccine_count
FROM {{zone_name}}.iceberg.cold_chain_readings;
