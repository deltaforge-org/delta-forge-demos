-- ============================================================================
-- Protobuf IoT Sensor Network — Verification Queries
-- ============================================================================
-- Each query verifies a specific protobuf feature: nested message flattening,
-- repeated field handling (explode vs. join_comma), timestamp conversion,
-- multi-file reading, and file metadata.
-- ============================================================================


-- ============================================================================
-- 1. SENSOR SUMMARY — 8 + 7 + 5 = 20 sensors across 3 facility files
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT *
FROM {{zone_name}}.protobuf_iot.sensor_summary;


-- ============================================================================
-- 2. EXPLODED READINGS — 33 + 29 + 20 = 82 reading rows
-- ============================================================================
-- Each SensorReading within each Sensor becomes its own row.

ASSERT ROW_COUNT = 82
SELECT *
FROM {{zone_name}}.protobuf_iot.sensor_readings;


-- ============================================================================
-- 3. SENSOR TYPE BREAKDOWN — count and average reading value per type
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
-- 4. LOCATION ANALYSIS — sensors and readings per location
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
-- 5. STATUS CHECK — sensor status distribution
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
-- 6. FILE SOURCE VERIFICATION — 3 distinct source files
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
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check: sensor count, reading count, type distribution,
-- location distribution, status distribution, and file metadata.

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
