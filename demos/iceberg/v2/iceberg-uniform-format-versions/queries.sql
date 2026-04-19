-- ============================================================================
-- Iceberg UniForm Format Versions — Queries
-- ============================================================================
-- HOW UNIFORM FORMAT VERSIONS WORK
-- ---------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata is generated automatically by the post-commit hook and is
-- never read by Delta Forge.
--
-- The `delta.universalFormat.icebergVersion` property controls which
-- Iceberg spec version the metadata conforms to:
--   V1 — single schema, basic partition specs
--   V2 — schema evolution array, sequence numbers (default)
--   V3 — nanosecond timestamps, deletion vectors, named references
--
-- Since queries go through Delta, all three tables behave identically.
-- The differences only matter to external Iceberg engines reading the
-- generated metadata.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running, verify each table's Iceberg format version:
--   python3 verify_iceberg_metadata.py <table_data_path>/sensors_v1 -v
--   python3 verify_iceberg_metadata.py <table_data_path>/sensors_v2 -v
--   python3 verify_iceberg_metadata.py <table_data_path>/sensors_v3 -v
-- ============================================================================
-- ============================================================================
-- EXPLORE: Verify Format Version Properties
-- ============================================================================
-- Each table should show its configured icebergVersion in TBLPROPERTIES.

ASSERT WARNING ROW_COUNT >= 2
SHOW TBLPROPERTIES {{zone_name}}.iceberg_demos.sensors_v1;

ASSERT WARNING ROW_COUNT >= 2
SHOW TBLPROPERTIES {{zone_name}}.iceberg_demos.sensors_v2;

ASSERT WARNING ROW_COUNT >= 2
SHOW TBLPROPERTIES {{zone_name}}.iceberg_demos.sensors_v3;
-- ============================================================================
-- Query 1: Row Count Parity — All Three Tables
-- ============================================================================
-- All tables have identical data: 12 readings.

ASSERT ROW_COUNT = 12
SELECT * FROM {{zone_name}}.iceberg_demos.sensors_v1 ORDER BY id;

ASSERT ROW_COUNT = 12
SELECT * FROM {{zone_name}}.iceberg_demos.sensors_v2 ORDER BY id;

ASSERT ROW_COUNT = 12
SELECT * FROM {{zone_name}}.iceberg_demos.sensors_v3 ORDER BY id;
-- ============================================================================
-- Query 2: Per-Location Aggregation — V1
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_temp = 23.08 WHERE location = 'Lab-A'
ASSERT VALUE avg_temp = 21.13 WHERE location = 'Lab-B'
ASSERT VALUE avg_temp = 22.43 WHERE location = 'Lab-C'
SELECT
    location,
    ROUND(AVG(temperature), 2) AS avg_temp,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    COUNT(*) AS readings
FROM {{zone_name}}.iceberg_demos.sensors_v1
GROUP BY location
ORDER BY location;
-- ============================================================================
-- Query 3: Per-Location Aggregation — V2
-- ============================================================================
-- Identical results prove V2 metadata is consistent.

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_temp = 23.08 WHERE location = 'Lab-A'
ASSERT VALUE avg_temp = 21.13 WHERE location = 'Lab-B'
ASSERT VALUE avg_temp = 22.43 WHERE location = 'Lab-C'
SELECT
    location,
    ROUND(AVG(temperature), 2) AS avg_temp,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    COUNT(*) AS readings
FROM {{zone_name}}.iceberg_demos.sensors_v2
GROUP BY location
ORDER BY location;
-- ============================================================================
-- Query 4: Per-Location Aggregation — V3
-- ============================================================================
-- V3 produces the same results — format version does not affect data.

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_temp = 23.08 WHERE location = 'Lab-A'
ASSERT VALUE avg_temp = 21.13 WHERE location = 'Lab-B'
ASSERT VALUE avg_temp = 22.43 WHERE location = 'Lab-C'
SELECT
    location,
    ROUND(AVG(temperature), 2) AS avg_temp,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    COUNT(*) AS readings
FROM {{zone_name}}.iceberg_demos.sensors_v3
GROUP BY location
ORDER BY location;
-- ============================================================================
-- Query 5: Status Distribution — Consistent Across All Versions
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 9 WHERE status = 'normal'
ASSERT VALUE cnt = 2 WHERE status = 'warning'
ASSERT VALUE cnt = 1 WHERE status = 'critical'
SELECT
    status,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.sensors_v1
GROUP BY status
ORDER BY status;

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 9 WHERE status = 'normal'
ASSERT VALUE cnt = 2 WHERE status = 'warning'
ASSERT VALUE cnt = 1 WHERE status = 'critical'
SELECT
    status,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.sensors_v2
GROUP BY status
ORDER BY status;

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 9 WHERE status = 'normal'
ASSERT VALUE cnt = 2 WHERE status = 'warning'
ASSERT VALUE cnt = 1 WHERE status = 'critical'
SELECT
    status,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.sensors_v3
GROUP BY status
ORDER BY status;
-- ============================================================================
-- LEARN: DML on V3 — UPDATE Critical Readings
-- ============================================================================
-- Correct the critical reading (Lab-B SENS-004 was too hot). Generates
-- Iceberg V3 snapshot with sequence number tracking.

UPDATE {{zone_name}}.iceberg_demos.sensors_v3
SET temperature = 22.0,
    status = 'corrected'
WHERE status = 'critical';
-- ============================================================================
-- Query 6: V3 Post-Update — Verify Correction
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE temperature = 22.0 WHERE sensor_id = 'SENS-004'
SELECT
    sensor_id,
    location,
    temperature,
    status
FROM {{zone_name}}.iceberg_demos.sensors_v3
WHERE id = 8;
-- ============================================================================
-- Query 7: V3 Time Travel — Before vs After Correction
-- ============================================================================
-- V3 snapshots support the same time travel as V1/V2.

ASSERT ROW_COUNT = 1
ASSERT VALUE old_temp = 26.3
ASSERT VALUE new_temp = 22.0
ASSERT VALUE old_status = 'critical'
ASSERT VALUE new_status = 'corrected'
SELECT
    old.temperature AS old_temp,
    curr.temperature AS new_temp,
    old.status AS old_status,
    curr.status AS new_status
FROM {{zone_name}}.iceberg_demos.sensors_v3 curr
JOIN {{zone_name}}.iceberg_demos.sensors_v3 VERSION AS OF 1 old
    ON curr.id = old.id
WHERE curr.id = 8;
-- ============================================================================
-- LEARN: Schema Evolution on V2 — Add calibration_offset
-- ============================================================================
-- V2's schema array tracks multiple schema versions. V1 only supports
-- a single schema (last-writer-wins). Both produce valid Iceberg metadata.

ALTER TABLE {{zone_name}}.iceberg_demos.sensors_v2 ADD COLUMN calibration_offset DOUBLE;

UPDATE {{zone_name}}.iceberg_demos.sensors_v2
SET calibration_offset = CASE
    WHEN location = 'Lab-A' THEN 0.5
    WHEN location = 'Lab-B' THEN -0.3
    ELSE 0.2
END;
-- ============================================================================
-- Query 8: V2 With Evolved Schema — Calibrated Temperature
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE calibrated_avg = 23.58 WHERE location = 'Lab-A'
ASSERT VALUE calibrated_avg = 20.83 WHERE location = 'Lab-B'
ASSERT VALUE calibrated_avg = 22.63 WHERE location = 'Lab-C'
SELECT
    location,
    ROUND(AVG(temperature + calibration_offset), 2) AS calibrated_avg
FROM {{zone_name}}.iceberg_demos.sensors_v2
GROUP BY location
ORDER BY location;
-- ============================================================================
-- Query 9: Grand Totals Across All Versions
-- ============================================================================
-- Proves data integrity is maintained regardless of format version.

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_avg_temp = 22.21
ASSERT VALUE v2_avg_temp = 22.21
ASSERT VALUE v3_avg_temp = 21.85
ASSERT VALUE v1_total_humidity = 571.50
SELECT
    ROUND((SELECT AVG(temperature) FROM {{zone_name}}.iceberg_demos.sensors_v1), 2) AS v1_avg_temp,
    ROUND((SELECT AVG(temperature) FROM {{zone_name}}.iceberg_demos.sensors_v2), 2) AS v2_avg_temp,
    ROUND((SELECT AVG(temperature) FROM {{zone_name}}.iceberg_demos.sensors_v3), 2) AS v3_avg_temp,
    ROUND((SELECT SUM(humidity) FROM {{zone_name}}.iceberg_demos.sensors_v1), 2) AS v1_total_humidity;
-- ============================================================================
-- Query 10: Version History Comparison
-- ============================================================================
-- V1 and V2 have 1 version (initial INSERT). V3 has 2 (INSERT + UPDATE).

ASSERT WARNING ROW_COUNT >= 1
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.sensors_v1;

ASSERT WARNING ROW_COUNT >= 1
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.sensors_v2;

ASSERT WARNING ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.sensors_v3;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-version parity check. V1 and V2 unchanged; V3 has one corrected row.

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_rows = 12
ASSERT VALUE v2_rows = 12
ASSERT VALUE v3_rows = 12
ASSERT VALUE v1_critical = 1
ASSERT VALUE v3_critical = 0
ASSERT VALUE v3_corrected = 1
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.sensors_v1) AS v1_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.sensors_v2) AS v2_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.sensors_v3) AS v3_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.sensors_v1 WHERE status = 'critical') AS v1_critical,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.sensors_v3 WHERE status = 'critical') AS v3_critical,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.sensors_v3 WHERE status = 'corrected') AS v3_corrected;
-- ============================================================================
-- Iceberg Read-Back: Register V1 as External Iceberg Table
-- ============================================================================
-- Register V1's physical location as an external Iceberg table and detect
-- its schema through the Iceberg metadata chain.

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v1_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sensors_v1_iceberg
USING ICEBERG
LOCATION '{{data_subdir}}/sensors_v1';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sensors_v1_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Read-Back: Register V2 as External Iceberg Table
-- ============================================================================
-- Register V2's physical location as an external Iceberg table.

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v2_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sensors_v2_iceberg
USING ICEBERG
LOCATION '{{data_subdir}}/sensors_v2';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sensors_v2_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Read-Back: Register V3 as External Iceberg Table
-- ============================================================================
-- Register V3's physical location as an external Iceberg table.

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v3_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sensors_v3_iceberg
USING ICEBERG
LOCATION '{{data_subdir}}/sensors_v3';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sensors_v3_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Verify 1: Row Counts — All Three Format Versions
-- ============================================================================

ASSERT ROW_COUNT = 12
SELECT * FROM {{zone_name}}.iceberg_demos.sensors_v1_iceberg ORDER BY id;

ASSERT ROW_COUNT = 12
SELECT * FROM {{zone_name}}.iceberg_demos.sensors_v2_iceberg ORDER BY id;

ASSERT ROW_COUNT = 12
SELECT * FROM {{zone_name}}.iceberg_demos.sensors_v3_iceberg ORDER BY id;
-- ============================================================================
-- Iceberg Verify 2: V1 — Per-Location Averages Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_temp = 23.08 WHERE location = 'Lab-A'
ASSERT VALUE avg_temp = 21.13 WHERE location = 'Lab-B'
ASSERT VALUE avg_temp = 22.43 WHERE location = 'Lab-C'
SELECT
    location,
    ROUND(AVG(temperature), 2) AS avg_temp
FROM {{zone_name}}.iceberg_demos.sensors_v1_iceberg
GROUP BY location
ORDER BY location;
-- ============================================================================
-- Iceberg Verify 3: V3 — Corrected Reading Visible
-- ============================================================================
-- V3 had the critical reading updated to 'corrected'. The Iceberg reader
-- should see the corrected state, not the original.

ASSERT ROW_COUNT = 1
ASSERT VALUE temperature = 22.0
ASSERT VALUE status = 'corrected'
SELECT
    temperature,
    status
FROM {{zone_name}}.iceberg_demos.sensors_v3_iceberg
WHERE id = 8;
-- ============================================================================
-- Iceberg Verify 4: Cross-Version Parity
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_avg = 22.21
ASSERT VALUE v3_avg = 21.85
ASSERT VALUE v1_total_humidity = 571.50
SELECT
    ROUND((SELECT AVG(temperature) FROM {{zone_name}}.iceberg_demos.sensors_v1_iceberg), 2) AS v1_avg,
    ROUND((SELECT AVG(temperature) FROM {{zone_name}}.iceberg_demos.sensors_v3_iceberg), 2) AS v3_avg,
    ROUND((SELECT SUM(humidity) FROM {{zone_name}}.iceberg_demos.sensors_v1_iceberg), 2) AS v1_total_humidity;
