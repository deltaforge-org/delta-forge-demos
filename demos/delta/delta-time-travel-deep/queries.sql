-- ============================================================================
-- Delta Time Travel — Version History Deep Dive — Educational Queries
-- ============================================================================
-- WHAT: Time travel lets you query any historical version of a Delta table
--       using SELECT ... FROM table VERSION AS OF N.
-- WHY:  Without time travel, answering "what did this data look like last
--       Tuesday?" requires maintaining separate snapshots or expensive backup
--       systems. Delta gives you this for free — every commit is a queryable
--       snapshot.
-- HOW:  Each DML operation (INSERT, UPDATE, DELETE) creates a new version in
--       the _delta_log. The log records which Parquet files to add and remove.
--       To read version N, Delta replays the log up to commit N and reads
--       only the files that were active at that point.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Current State (Version 3 — Latest)
-- ============================================================================
-- The sensor_readings table went through 5 Delta versions (0-4):
--   V0: CREATE (empty table)
--   V1: INSERT 40 initial readings (10 per location: lab-a, lab-b, warehouse, field)
--   V2: UPDATE — Calibrated lab-a sensors (reading += 2.5)
--   V3: DELETE — Removed 5 faulty field sensors (impossible readings like 99.9, -40.0)
--   V4: INSERT — Added 15 new sensor readings
--
-- Let's see the current state:

-- Verify all 4 locations are present and sensor counts reflect version history
ASSERT VALUE sensors = 13 WHERE location = 'lab-a'
ASSERT VALUE sensors = 13 WHERE location = 'lab-b'
ASSERT VALUE sensors = 14 WHERE location = 'warehouse'
ASSERT VALUE sensors = 10 WHERE location = 'field'
ASSERT ROW_COUNT = 4
SELECT location,
       COUNT(*) AS sensors,
       ROUND(MIN(reading), 1) AS min_reading,
       ROUND(MAX(reading), 1) AS max_reading,
       ROUND(AVG(reading), 1) AS avg_reading
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY location
ORDER BY location;


-- ============================================================================
-- LEARN: Querying a Historical Version (V0 — Before Any Changes)
-- ============================================================================
-- VERSION AS OF 1 shows the table exactly as it was after the initial INSERT.
-- At V1, lab-a sensors had their original uncalibrated readings, and the
-- faulty field sensors (ids 36-40) still existed.

-- Non-deterministic: ROUND(AVG(DOUBLE), 1) may vary ±0.1 due to platform-specific rounding at x.x5 boundary
ASSERT WARNING VALUE avg_reading BETWEEN 22.8 AND 23.1 WHERE location = 'lab-a'
ASSERT ROW_COUNT = 4
SELECT location,
       COUNT(*) AS sensors,
       ROUND(AVG(reading), 1) AS avg_reading
FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 1
GROUP BY location
ORDER BY location;


-- ============================================================================
-- LEARN: Comparing Versions — Before vs. After Calibration
-- ============================================================================
-- One of time travel's most powerful uses: comparing the same data across
-- versions. Here we look at sensor S001 before calibration (V1: 22.5)
-- and after (V2+: 25.0). The delta of +2.5 confirms the calibration offset.

-- Verify S001 reading before calibration was 22.5 and after is 25.0
ASSERT VALUE reading = 22.5 WHERE version_label = 'Before calibration (V1)'
ASSERT VALUE reading = 25.0 WHERE version_label = 'After calibration (V2+)'
ASSERT ROW_COUNT = 2
SELECT 'Before calibration (V1)' AS version_label,
       sensor_id, reading
FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 1
WHERE sensor_id = 'S001'
UNION ALL
SELECT 'After calibration (V2+)',
       sensor_id, reading
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'S001';


-- ============================================================================
-- LEARN: Recovering Deleted Data via Time Travel
-- ============================================================================
-- At V2, the faulty field sensors still existed (they were deleted in V3).
-- Time travel lets you inspect what was deleted without needing a backup.
-- These sensors had impossible readings that warranted removal:

-- Verify the impossible readings are visible via time travel (id=36 had reading=99.9 celsius)
ASSERT VALUE reading = 99.9 WHERE id = 36
ASSERT ROW_COUNT = 5
SELECT id, sensor_id, reading, location
FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 2
WHERE id IN (36, 37, 38, 39, 40)
ORDER BY id;


-- ============================================================================
-- LEARN: Version Progression — Row Count Over Time
-- ============================================================================
-- Each version represents an atomic snapshot. The row count tells the story:
--   V0 (delta v1): 40 (initial load)
--   V1 (delta v2): 40 (calibration changed values, not row count)
--   V2 (delta v3): 35 (5 faulty sensors deleted)
--   V3 (delta v4): 50 (15 new sensors added)

-- Verify each version's row count matches the expected progression
ASSERT VALUE row_count = 40 WHERE version = 'V0'
ASSERT VALUE row_count = 40 WHERE version = 'V1'
ASSERT VALUE row_count = 35 WHERE version = 'V2'
ASSERT VALUE row_count = 50 WHERE version = 'V3'
ASSERT ROW_COUNT = 4
SELECT 'V0' AS version,
       (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 1) AS row_count
UNION ALL
SELECT 'V1',
       (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 2)
UNION ALL
SELECT 'V2',
       (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 3)
UNION ALL
SELECT 'V3',
       (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 4);


-- ============================================================================
-- EXPLORE: New Sensors Added in V3
-- ============================================================================
-- V4 added 15 new sensors (ids 41-55) across all 4 locations.
-- These only appear in V4, not in earlier versions.

ASSERT ROW_COUNT = 15
SELECT id, sensor_id, reading, location, recorded_at
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE id > 40
ORDER BY location, id;


-- ============================================================================
-- EXPLORE: Field Sensors — Before and After Cleanup
-- ============================================================================
-- At V2, the field location had 10 sensors (5 good + 5 faulty).
-- After V3's DELETE, only 5 remain. V4 added 5 more replacements.

-- Verify field sensor counts: 10 at V2, 10 after delete + new additions
ASSERT VALUE field_sensors = 10 WHERE state = 'V2 (before delete)'
ASSERT VALUE field_sensors = 10 WHERE state = 'Current (after delete + new)'
ASSERT ROW_COUNT = 2
SELECT 'V2 (before delete)' AS state,
       COUNT(*) AS field_sensors
FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 2
WHERE location = 'field'
UNION ALL
SELECT 'Current (after delete + new)',
       COUNT(*)
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE location = 'field';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify current row count is 50
ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.delta_demos.sensor_readings;

-- Verify version 1 (initial insert) had 40 rows
ASSERT VALUE v1_row_count = 40
SELECT COUNT(*) AS v1_row_count FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 1;

-- Verify S001 reading was calibrated to 25.0
ASSERT VALUE reading = 25.0
SELECT reading FROM {{zone_name}}.delta_demos.sensor_readings WHERE sensor_id = 'S001';

-- Verify faulty sensors (ids 36-40) were deleted
ASSERT VALUE deleted_count = 0
SELECT COUNT(*) AS deleted_count FROM {{zone_name}}.delta_demos.sensor_readings WHERE id IN (36, 37, 38, 39, 40);

-- Verify 15 new sensors were added (ids > 40)
ASSERT VALUE new_sensors = 15
SELECT COUNT(*) AS new_sensors FROM {{zone_name}}.delta_demos.sensor_readings WHERE id > 40;

-- Verify version 2 (after calibration) still has 10 field sensors (before delete)
ASSERT VALUE v2_field_count = 10
SELECT COUNT(*) AS v2_field_count FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 2 WHERE location = 'field';

-- Verify 4 distinct locations exist
ASSERT VALUE location_count = 4
SELECT COUNT(DISTINCT location) AS location_count FROM {{zone_name}}.delta_demos.sensor_readings;

-- Verify version 4 (final state) is queryable
ASSERT VALUE v4_row_count = 50
SELECT COUNT(*) AS v4_row_count FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 4;
