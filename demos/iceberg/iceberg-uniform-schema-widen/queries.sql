-- ============================================================================
-- Iceberg UniForm Type Widening — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH TYPE WIDENING
-- ----------------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- When ALTER TABLE CHANGE COLUMN ... TYPE runs, Delta Forge:
--   1. Updates the Delta schema in _delta_log/ (what these queries read)
--   2. Adds a new schema entry to metadata.json's "schemas" array with the
--      widened type (Iceberg V2/V3 track multiple schema versions)
--
-- Supported widenings: INT→BIGINT, FLOAT→DOUBLE. The Iceberg metadata
-- records the type change so that Iceberg-compatible engines can read both
-- old data (original narrow type) and new data (widened type) through the
-- unified metadata chain.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify type widening in metadata with:
--   python3 verify_iceberg_metadata.py <table_data_path>/sensor_readings -v
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — 24 Sensors (Version 1)
-- ============================================================================

ASSERT ROW_COUNT = 24
SELECT * FROM {{zone_name}}.iceberg_demos.sensor_readings ORDER BY sensor_id;


-- ============================================================================
-- Query 1: Baseline Aggregation — Per-Location Averages
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE avg_temp = 18.53 WHERE location = 'basement'
ASSERT VALUE avg_temp = 21.13 WHERE location = 'cleanroom'
ASSERT VALUE avg_temp = 33.05 WHERE location = 'rooftop'
ASSERT VALUE avg_temp = 22.92 WHERE location = 'warehouse'
ASSERT VALUE avg_humidity = 71.85 WHERE location = 'basement'
ASSERT VALUE avg_humidity = 49.83 WHERE location = 'cleanroom'
ASSERT VALUE avg_humidity = 44.55 WHERE location = 'rooftop'
ASSERT VALUE avg_humidity = 54.82 WHERE location = 'warehouse'
SELECT
    location,
    COUNT(*) AS sensor_count,
    ROUND(AVG(temperature), 2) AS avg_temp,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    SUM(reading_count) AS total_readings
FROM {{zone_name}}.iceberg_demos.sensor_readings
GROUP BY location
ORDER BY location;


-- ============================================================================
-- LEARN: Type Widening Step 1 — INT→BIGINT for reading_count (Version 2)
-- ============================================================================
-- IoT sensor counters can exceed 2^31 (2,147,483,647). Widening to BIGINT
-- allows counters up to 2^63. The Iceberg metadata.json gets a new schema
-- entry with the reading_count field type changed from int to long.

ALTER TABLE {{zone_name}}.iceberg_demos.sensor_readings ALTER COLUMN reading_count TYPE BIGINT;


-- ============================================================================
-- Query 2: Verify Column Still Works After Widening
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_sensors = 24
ASSERT VALUE max_reading = 63000
ASSERT VALUE total_readings = 874000
SELECT
    COUNT(*) AS total_sensors,
    MAX(reading_count) AS max_reading,
    SUM(reading_count) AS total_readings
FROM {{zone_name}}.iceberg_demos.sensor_readings;


-- ============================================================================
-- LEARN: Insert Rows With Large Values > 2 Billion (Version 3)
-- ============================================================================
-- These values would overflow a 32-bit INT. With the BIGINT widening,
-- they store correctly.

INSERT INTO {{zone_name}}.iceberg_demos.sensor_readings VALUES
    ('S025', 'rooftop',    2500000000, 33.0, 43.5, 90, '2025-02-01'),
    ('S026', 'basement',   3100000000, 18.6, 71.8, 86, '2025-02-01'),
    ('S027', 'warehouse',  2800000000, 22.9, 55.0, 73, '2025-02-01'),
    ('S028', 'cleanroom',  3500000000, 21.1, 50.2, 87, '2025-02-01');


-- ============================================================================
-- Query 3: Verify BIGINT Values Stored Correctly
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_sensors = 28
ASSERT VALUE max_reading = 3500000000
ASSERT VALUE rows_over_2b = 4
SELECT
    COUNT(*) AS total_sensors,
    MAX(reading_count) AS max_reading,
    COUNT(*) FILTER (WHERE reading_count > 2000000000) AS rows_over_2b
FROM {{zone_name}}.iceberg_demos.sensor_readings;


-- ============================================================================
-- LEARN: Type Widening Step 2 — FLOAT→DOUBLE for temperature (Version 4)
-- ============================================================================
-- Upgrading sensor precision from single-precision (FLOAT, ~7 digits) to
-- double-precision (DOUBLE, ~15 digits). The Iceberg metadata records the
-- type change from float to double.

ALTER TABLE {{zone_name}}.iceberg_demos.sensor_readings ALTER COLUMN temperature TYPE DOUBLE;


-- ============================================================================
-- LEARN: Type Widening Step 3 — FLOAT→DOUBLE for humidity (Version 5)
-- ============================================================================

ALTER TABLE {{zone_name}}.iceberg_demos.sensor_readings ALTER COLUMN humidity TYPE DOUBLE;


-- ============================================================================
-- LEARN: Insert High-Precision Readings (Version 6)
-- ============================================================================
-- These readings have 6+ decimal places. With DOUBLE precision, the values
-- are stored without truncation (FLOAT would lose precision beyond ~7 digits).

INSERT INTO {{zone_name}}.iceberg_demos.sensor_readings VALUES
    ('S029', 'rooftop',    16000, 32.456789, 45.123456, 91, '2025-02-15'),
    ('S030', 'basement',   29000, 18.789012, 72.345678, 88, '2025-02-15'),
    ('S031', 'warehouse',  43000, 22.567890, 55.678901, 75, '2025-02-15'),
    ('S032', 'cleanroom',  54000, 21.234567, 50.890123, 86, '2025-02-15');


-- ============================================================================
-- Query 4: Verify Precision Is Preserved
-- ============================================================================
-- These high-precision values should not be truncated to FLOAT resolution.

ASSERT ROW_COUNT = 4
ASSERT VALUE temperature = 32.456789 WHERE sensor_id = 'S029'
ASSERT VALUE temperature = 18.789012 WHERE sensor_id = 'S030'
ASSERT VALUE temperature = 22.567890 WHERE sensor_id = 'S031'
ASSERT VALUE temperature = 21.234567 WHERE sensor_id = 'S032'
SELECT
    sensor_id,
    temperature,
    humidity
FROM {{zone_name}}.iceberg_demos.sensor_readings
WHERE sensor_id IN ('S029', 'S030', 'S031', 'S032')
ORDER BY sensor_id;


-- ============================================================================
-- Query 5: Final Per-Location Summary
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE sensor_count = 8 WHERE location = 'basement'
ASSERT VALUE sensor_count = 8 WHERE location = 'cleanroom'
ASSERT VALUE sensor_count = 8 WHERE location = 'rooftop'
ASSERT VALUE sensor_count = 8 WHERE location = 'warehouse'
ASSERT VALUE avg_temp = 18.57 WHERE location = 'basement'
ASSERT VALUE avg_temp = 21.14 WHERE location = 'cleanroom'
ASSERT VALUE avg_temp = 32.97 WHERE location = 'rooftop'
ASSERT VALUE avg_temp = 22.87 WHERE location = 'warehouse'
SELECT
    location,
    COUNT(*) AS sensor_count,
    ROUND(AVG(temperature), 2) AS avg_temp,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    MAX(reading_count) AS max_reading
FROM {{zone_name}}.iceberg_demos.sensor_readings
GROUP BY location
ORDER BY location;


-- ============================================================================
-- Query 6: Time Travel — Read Version 1 (Original INT/FLOAT Schema)
-- ============================================================================
-- Reading the pre-widening version returns data with the original types.

ASSERT ROW_COUNT = 24
SELECT
    sensor_id, location, reading_count, temperature, humidity, battery_pct, reading_date
FROM {{zone_name}}.iceberg_demos.sensor_readings VERSION AS OF 1
ORDER BY sensor_id;


-- ============================================================================
-- Query 7: Version History — Type Widening Trail
-- ============================================================================

ASSERT WARNING ROW_COUNT >= 6
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.sensor_readings;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 32
ASSERT VALUE max_reading_count = 3500000000
ASSERT VALUE distinct_locations = 4
ASSERT VALUE bigint_rows = 4
ASSERT VALUE avg_temp = 23.89
ASSERT VALUE avg_humidity = 55.34
SELECT
    COUNT(*) AS total_rows,
    MAX(reading_count) AS max_reading_count,
    COUNT(DISTINCT location) AS distinct_locations,
    COUNT(*) FILTER (WHERE reading_count > 2147483647) AS bigint_rows,
    ROUND(AVG(temperature), 2) AS avg_temp,
    ROUND(AVG(humidity), 2) AS avg_humidity
FROM {{zone_name}}.iceberg_demos.sensor_readings;


-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents the widened types (BIGINT for
-- reading_count, DOUBLE for temperature and humidity).
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sensor_readings_iceberg
USING ICEBERG
LOCATION '{{data_path}}/sensor_readings';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sensor_readings_iceberg TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg_demos.sensor_readings_iceberg;


-- ============================================================================
-- Iceberg Verify 1: Row Count — 32 Sensors (24 Original + 4 BIGINT + 4 DOUBLE)
-- ============================================================================

ASSERT ROW_COUNT = 32
SELECT * FROM {{zone_name}}.iceberg_demos.sensor_readings_iceberg ORDER BY sensor_id;


-- ============================================================================
-- Iceberg Verify 2: BIGINT Values Readable Through Iceberg
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_sensors = 32
ASSERT VALUE max_reading = 3500000000
ASSERT VALUE bigint_rows = 4
SELECT
    COUNT(*) AS total_sensors,
    MAX(reading_count) AS max_reading,
    COUNT(*) FILTER (WHERE reading_count > 2147483647) AS bigint_rows
FROM {{zone_name}}.iceberg_demos.sensor_readings_iceberg;


-- ============================================================================
-- Iceberg Verify 3: Averages Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE avg_temp = 23.89
ASSERT VALUE avg_humidity = 55.34
SELECT
    ROUND(AVG(temperature), 2) AS avg_temp,
    ROUND(AVG(humidity), 2) AS avg_humidity
FROM {{zone_name}}.iceberg_demos.sensor_readings_iceberg;
