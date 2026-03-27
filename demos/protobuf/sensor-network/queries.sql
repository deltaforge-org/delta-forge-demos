-- ============================================================================
-- Protobuf IoT Sensor Network — Verification Queries
-- ============================================================================
-- Validates actual field values read from protobuf binary files, not just
-- row counts. Each query checks known values from the seed data across all
-- three facility files, both tables, and key protobuf features.
-- ============================================================================


-- ============================================================================
-- Query 1: SENSOR SUMMARY — row count and known sensor fields
-- ============================================================================
-- Verifies the summary table (one row per sensor, repeated fields joined).

ASSERT ROW_COUNT = 20
ASSERT VALUE sensor_type = 'temperature' WHERE sensor_id = 'TEMP-A001'
ASSERT VALUE location = 'Line-A' WHERE sensor_id = 'TEMP-A001'
ASSERT VALUE status = 'active' WHERE sensor_id = 'TEMP-A001'
ASSERT VALUE sensor_type = 'vibration' WHERE sensor_id = 'VIB-B002'
ASSERT VALUE status = 'maintenance' WHERE sensor_id = 'VIB-B002'
ASSERT VALUE location = 'Line-B' WHERE sensor_id = 'VIB-B002'
ASSERT VALUE sensor_type = 'humidity' WHERE sensor_id = 'HUM-W001'
ASSERT VALUE location = 'Warehouse' WHERE sensor_id = 'HUM-W001'
ASSERT VALUE status = 'offline' WHERE sensor_id = 'TEMP-B002'
SELECT sensor_id, sensor_type, location, status
FROM {{zone_name}}.protobuf_iot.sensor_summary
ORDER BY sensor_id;


-- ============================================================================
-- Query 2: EXPLODED READINGS — specific reading values from each file
-- ============================================================================
-- Verifies that exploded readings carry correct double values and units.
-- Checks one known reading from each of the 3 facility files.

ASSERT ROW_COUNT = 82
ASSERT VALUE reading_value = 22.5 WHERE sensor_id = 'TEMP-A001' AND unit = 'celsius' AND reading_value = 22.5
ASSERT VALUE unit = 'percent' WHERE sensor_id = 'HUM-B002' AND reading_value = 61.5
ASSERT VALUE unit = 'mm_per_s' WHERE sensor_id = 'VIB-W001' AND reading_value = 0.5
SELECT sensor_id, reading_value, unit
FROM {{zone_name}}.protobuf_iot.sensor_readings
ORDER BY sensor_id, reading_value;


-- ============================================================================
-- Query 3: SPECIFIC SENSOR READINGS — verify all 4 readings for TEMP-A001
-- ============================================================================
-- TEMP-A001 has readings: 22.5, 23.1, 22.8, 23.4 (all celsius).

ASSERT ROW_COUNT = 4
ASSERT VALUE min_val = 22.5 WHERE sensor_id = 'TEMP-A001'
ASSERT VALUE max_val = 23.4 WHERE sensor_id = 'TEMP-A001'
SELECT sensor_id,
       MIN(reading_value) AS min_val,
       MAX(reading_value) AS max_val,
       COUNT(*) AS cnt
FROM {{zone_name}}.protobuf_iot.sensor_readings
WHERE sensor_id = 'TEMP-A001'
GROUP BY sensor_id;


-- ============================================================================
-- Query 4: VIBRATION SENSOR VALUES — verify readings from Line-B
-- ============================================================================
-- VIB-B002 (maintenance): 7.8, 8.2, 8.5, 7.9 mm/s — high vibration sensor.

ASSERT ROW_COUNT = 4
ASSERT VALUE min_val = 7.8 WHERE sensor_id = 'VIB-B002'
ASSERT VALUE max_val = 8.5 WHERE sensor_id = 'VIB-B002'
ASSERT VALUE avg_val = 8.1 WHERE sensor_id = 'VIB-B002'
SELECT sensor_id,
       MIN(reading_value) AS min_val,
       MAX(reading_value) AS max_val,
       ROUND(AVG(reading_value), 1) AS avg_val,
       COUNT(*) AS cnt
FROM {{zone_name}}.protobuf_iot.sensor_readings
WHERE sensor_id = 'VIB-B002'
GROUP BY sensor_id;


-- ============================================================================
-- Query 5: WAREHOUSE HUMIDITY — verify exact reading values for HUM-W002
-- ============================================================================
-- HUM-W002 (maintenance): 72.1, 73.5, 71.8, 72.9 percent.

ASSERT ROW_COUNT = 4
ASSERT VALUE min_val = 71.8 WHERE sensor_id = 'HUM-W002'
ASSERT VALUE max_val = 73.5 WHERE sensor_id = 'HUM-W002'
SELECT sensor_id, status,
       MIN(reading_value) AS min_val,
       MAX(reading_value) AS max_val,
       COUNT(*) AS cnt
FROM {{zone_name}}.protobuf_iot.sensor_readings
WHERE sensor_id = 'HUM-W002'
GROUP BY sensor_id, status;


-- ============================================================================
-- Query 6: SENSOR TYPE BREAKDOWN — count and reading ranges per type
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sensor_count = 7 WHERE sensor_type = 'temperature'
ASSERT VALUE sensor_count = 7 WHERE sensor_type = 'humidity'
ASSERT VALUE sensor_count = 6 WHERE sensor_type = 'vibration'
SELECT sensor_type,
       COUNT(DISTINCT sensor_id) AS sensor_count,
       COUNT(*) AS reading_count
FROM {{zone_name}}.protobuf_iot.sensor_readings
GROUP BY sensor_type
ORDER BY sensor_type;


-- ============================================================================
-- Query 7: LOCATION ANALYSIS — sensors and readings per location
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sensor_count = 8 WHERE location = 'Line-A'
ASSERT VALUE sensor_count = 7 WHERE location = 'Line-B'
ASSERT VALUE sensor_count = 5 WHERE location = 'Warehouse'
ASSERT VALUE reading_count = 33 WHERE location = 'Line-A'
ASSERT VALUE reading_count = 29 WHERE location = 'Line-B'
ASSERT VALUE reading_count = 20 WHERE location = 'Warehouse'
SELECT location,
       COUNT(DISTINCT sensor_id) AS sensor_count,
       COUNT(*) AS reading_count
FROM {{zone_name}}.protobuf_iot.sensor_readings
GROUP BY location
ORDER BY location;


-- ============================================================================
-- Query 8: STATUS CHECK — sensor status distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sensor_count = 16 WHERE status = 'active'
ASSERT VALUE sensor_count = 3 WHERE status = 'maintenance'
ASSERT VALUE sensor_count = 1 WHERE status = 'offline'
SELECT status,
       COUNT(DISTINCT sensor_id) AS sensor_count
FROM {{zone_name}}.protobuf_iot.sensor_readings
GROUP BY status
ORDER BY sensor_count DESC;


-- ============================================================================
-- Query 9: FILE SOURCE VERIFICATION — sensors per source file
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sensor_count = 8 WHERE df_file_name LIKE '%factory_floor_a%'
ASSERT VALUE sensor_count = 7 WHERE df_file_name LIKE '%factory_floor_b%'
ASSERT VALUE sensor_count = 5 WHERE df_file_name LIKE '%warehouse%'
SELECT df_file_name,
       COUNT(DISTINCT sensor_id) AS sensor_count
FROM {{zone_name}}.protobuf_iot.sensor_readings
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- Query 10: FIVE-READING SENSOR — HUM-A003 and HUM-B002 have 5 readings
-- ============================================================================
-- Most sensors have 4 readings; HUM-A003 and HUM-B002 each have 5.

ASSERT ROW_COUNT = 2
ASSERT VALUE reading_count = 5 WHERE sensor_id = 'HUM-A003'
ASSERT VALUE reading_count = 5 WHERE sensor_id = 'HUM-B002'
SELECT sensor_id, COUNT(*) AS reading_count
FROM {{zone_name}}.protobuf_iot.sensor_readings
GROUP BY sensor_id
HAVING COUNT(*) = 5
ORDER BY sensor_id;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check covering key data invariants.

ASSERT ROW_COUNT = 8
ASSERT VALUE result = 'PASS' WHERE check_name = 'sensor_count_20'
ASSERT VALUE result = 'PASS' WHERE check_name = 'reading_rows_82'
ASSERT VALUE result = 'PASS' WHERE check_name = 'three_sensor_types'
ASSERT VALUE result = 'PASS' WHERE check_name = 'three_locations'
ASSERT VALUE result = 'PASS' WHERE check_name = 'active_sensors_16'
ASSERT VALUE result = 'PASS' WHERE check_name = 'three_source_files'
ASSERT VALUE result = 'PASS' WHERE check_name = 'timestamps_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_metadata_populated'
SELECT check_name, result FROM (

    -- Check 1: Total sensors = 20
    SELECT 'sensor_count_20' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.protobuf_iot.sensor_summary) = 20
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Exploded reading rows = 82
    SELECT 'reading_rows_82' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.protobuf_iot.sensor_readings) = 82
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 3 distinct sensor types
    SELECT 'three_sensor_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT sensor_type) FROM {{zone_name}}.protobuf_iot.sensor_readings) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: 3 distinct locations
    SELECT 'three_locations' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT location) FROM {{zone_name}}.protobuf_iot.sensor_readings) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: 16 active sensors
    SELECT 'active_sensors_16' AS check_name,
           CASE WHEN (
               SELECT COUNT(DISTINCT sensor_id) FROM {{zone_name}}.protobuf_iot.sensor_readings
               WHERE status = 'active'
           ) = 16 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: 3 source files
    SELECT 'three_source_files' AS check_name,
           CASE WHEN (
               SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.protobuf_iot.sensor_readings
           ) = 3 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Timestamps populated for all sensors
    SELECT 'timestamps_populated' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf_iot.sensor_summary
               WHERE installed_at IS NOT NULL
           ) = 20 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: File metadata populated for all rows
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf_iot.sensor_readings
               WHERE df_file_name IS NOT NULL
           ) = 82 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
