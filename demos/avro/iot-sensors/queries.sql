-- ============================================================================
-- Avro IoT Sensors — Verification Queries
-- ============================================================================
-- Each query verifies a specific Avro feature: multi-file reading,
-- file_filter, max_rows, file_metadata, self-describing schema,
-- mixed compression codec support, and v2 column access.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ROW COUNT — 2,500 readings across 5 floor files
-- ============================================================================

ASSERT ROW_COUNT = 2500
SELECT *
FROM {{zone_name}}.avro.all_readings;


-- ============================================================================
-- 2. BROWSE READINGS — See column types (self-describing Avro schema)
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT sensor_id, floor, zone, timestamp, temperature_c, humidity_pct,
       co2_ppm, occupancy
FROM {{zone_name}}.avro.all_readings
LIMIT 20;


-- ============================================================================
-- 3. ROWS PER FILE — 500 rows each across 5 files
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE row_count = 500 WHERE df_file_name LIKE '%floor1%'
ASSERT VALUE row_count = 500 WHERE df_file_name LIKE '%floor2%'
ASSERT VALUE row_count = 500 WHERE df_file_name LIKE '%floor3%'
ASSERT VALUE row_count = 500 WHERE df_file_name LIKE '%floor4%'
ASSERT VALUE row_count = 500 WHERE df_file_name LIKE '%floor5%'
SELECT df_file_name, COUNT(*) AS row_count
FROM {{zone_name}}.avro.all_readings
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. V2 COLUMNS — floor4_only includes battery_pct and firmware_version
-- ============================================================================
-- The v2 Avro files (floors 4–5) contain extra columns not present in v1.
-- Use file_filter (floor4_only table) to access the full v2 schema.
-- ============================================================================

ASSERT ROW_COUNT = 10
SELECT sensor_id, floor, zone, battery_pct, firmware_version
FROM {{zone_name}}.avro.floor4_only
LIMIT 10;


-- ============================================================================
-- 5. FILE FILTER — floor4_only has exactly 500 rows
-- ============================================================================

ASSERT ROW_COUNT = 500
SELECT *
FROM {{zone_name}}.avro.floor4_only;


-- ============================================================================
-- 6. FILE FILTER — floor4_only has all v2 columns populated
-- ============================================================================

ASSERT VALUE battery_non_null = 500
SELECT COUNT(battery_pct) AS battery_non_null
FROM {{zone_name}}.avro.floor4_only;

ASSERT VALUE firmware_non_null = 500
SELECT COUNT(firmware_version) AS firmware_non_null
FROM {{zone_name}}.avro.floor4_only;


-- ============================================================================
-- 7. MAX ROWS — readings_sample is limited (50 rows per file x 5 files)
-- ============================================================================

ASSERT ROW_COUNT = 250
SELECT *
FROM {{zone_name}}.avro.readings_sample;


-- ============================================================================
-- 8. FILE METADATA — All rows have non-NULL df_file_name
-- ============================================================================

ASSERT VALUE metadata_count = 2500
SELECT COUNT(*) AS metadata_count
FROM {{zone_name}}.avro.all_readings
WHERE df_file_name IS NOT NULL;


-- ============================================================================
-- 9. ANALYTICS — Average temperature and humidity by floor/zone
-- ============================================================================

ASSERT ROW_COUNT = 25
SELECT floor, zone,
       ROUND(AVG(CAST(temperature_c AS DOUBLE)), 1) AS avg_temp_c,
       ROUND(AVG(CAST(humidity_pct AS DOUBLE)), 1) AS avg_humidity,
       ROUND(AVG(CAST(co2_ppm AS DOUBLE)), 0) AS avg_co2
FROM {{zone_name}}.avro.all_readings
GROUP BY floor, zone
ORDER BY floor, zone;


-- ============================================================================
-- 10. ANALYTICS — Occupancy rate by floor
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE total_readings = 500 WHERE "floor" = 1
ASSERT VALUE total_readings = 500 WHERE "floor" = 2
ASSERT VALUE total_readings = 500 WHERE "floor" = 3
ASSERT VALUE total_readings = 500 WHERE "floor" = 4
ASSERT VALUE total_readings = 500 WHERE "floor" = 5
ASSERT VALUE occupied_readings = 364 WHERE "floor" = 1
ASSERT VALUE occupancy_pct = 72.8 WHERE "floor" = 1
ASSERT VALUE occupied_readings = 333 WHERE "floor" = 4
ASSERT VALUE occupancy_pct = 66.6 WHERE "floor" = 4
SELECT floor,
       COUNT(*) AS total_readings,
       SUM(CASE WHEN CAST(occupancy AS BOOLEAN) THEN 1 ELSE 0 END) AS occupied_readings,
       ROUND(SUM(CASE WHEN CAST(occupancy AS BOOLEAN) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS occupancy_pct
FROM {{zone_name}}.avro.all_readings
GROUP BY floor
ORDER BY floor;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_count_2500'
ASSERT VALUE result = 'PASS' WHERE check_name = 'five_source_files'
ASSERT VALUE result = 'PASS' WHERE check_name = 'floor4_has_battery'
ASSERT VALUE result = 'PASS' WHERE check_name = 'max_rows_250'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_metadata_populated'
SELECT check_name, result FROM (

    -- Check 1: Total row count = 2,500
    SELECT 'total_count_2500' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.avro.all_readings) = 2500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 5 distinct source files
    SELECT 'five_source_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.avro.all_readings) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: V2 columns — floor4_only has battery_pct for all 500 rows
    SELECT 'floor4_has_battery' AS check_name,
           CASE WHEN (
               SELECT COUNT(battery_pct) FROM {{zone_name}}.avro.floor4_only
           ) = 500 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: V2 columns — floor4_only has firmware_version for all 500 rows
    SELECT 'floor4_has_firmware' AS check_name,
           CASE WHEN (
               SELECT COUNT(firmware_version) FROM {{zone_name}}.avro.floor4_only
           ) = 500 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: File filter — floor4_only has 500 rows
    SELECT 'filter_floor4_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.avro.floor4_only) = 500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Max rows — sample has 250 rows (50 x 5)
    SELECT 'max_rows_250' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.avro.readings_sample) = 250
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: File metadata populated for all rows
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.avro.all_readings WHERE df_file_name IS NOT NULL) = 2500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Self-describing schema — sensor_id column exists
    SELECT 'schema_sensor_id_exists' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'avro' AND table_name = 'all_readings'
               AND column_name = 'sensor_id'
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: Column count — 10 data columns (8 v1 + 2 v2) + 2 metadata = 12
    SELECT 'column_count_12' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'avro' AND table_name = 'all_readings'
           ) = 12 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 10: Sample covers all 5 files
    SELECT 'sample_all_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.avro.readings_sample) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
