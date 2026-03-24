-- ============================================================================
-- Delta MERGE — Idempotent Pipeline (Timestamp Guards) — Educational Queries
-- ============================================================================
-- WHAT: MERGE INTO with a timestamp guard that makes the operation safe to
--       re-run any number of times (idempotent). Only newer source rows
--       overwrite target rows; stale source data is silently skipped.
-- WHY:  Production pipelines fail and retry. If a MERGE is not idempotent,
--       retries can corrupt data — double-counting, overwriting newer values
--       with stale ones, or inserting duplicates. The timestamp guard pattern
--       ensures the MERGE converges to the same state regardless of how
--       many times it runs.
-- HOW:  The ON clause matches on sensor_id + metric (the natural key — one
--       row per sensor per metric). The WHEN MATCHED clause adds
--       AND source.recorded_at > target.recorded_at so only strictly newer
--       readings apply. WHEN NOT MATCHED inserts new sensors. On the second
--       run, all source timestamps are now <= target, so zero rows are
--       affected.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Existing Sensor Readings (Target Table)
-- ============================================================================
-- The fact table has 25 rows: 5 sensors x 5 metrics each.
-- This is a "current state" table — one row per sensor_id+metric.
-- All recorded_at timestamps are '2024-01-01 12:00:00' (the most recent
-- hourly reading). All ingested at '2024-01-01 13:00:00'.

ASSERT ROW_COUNT = 25
SELECT sensor_id, location, metric, value, recorded_at, ingested_at
FROM {{zone_name}}.delta_demos.sensor_readings
ORDER BY sensor_id, metric;


-- ============================================================================
-- PREVIEW: Incoming Batch with Classification
-- ============================================================================
-- The batch has 20 rows. We classify each one by what SHOULD happen:
--   - NEWER (10 rows): recorded_at = 13:00 > target 12:00 → will UPDATE
--   - STALE (5 rows):  recorded_at = 10:00 < target 12:00 → will be SKIPPED
--   - NEW (5 rows):    sensor_id+metric not in target → will INSERT

ASSERT ROW_COUNT = 20
SELECT b.sensor_id,
       b.metric,
       b.value,
       b.recorded_at AS batch_recorded_at,
       b.batch_id,
       CASE
           WHEN r.sensor_id IS NULL THEN 'NEW (insert)'
           WHEN b.recorded_at > r.recorded_at THEN 'NEWER (update)'
           ELSE 'STALE (skip)'
       END AS expected_action
FROM {{zone_name}}.delta_demos.sensor_batch b
LEFT JOIN {{zone_name}}.delta_demos.sensor_readings r
    ON b.sensor_id = r.sensor_id AND b.metric = r.metric
ORDER BY expected_action, b.sensor_id, b.metric;


-- ============================================================================
-- MERGE: First Run — Apply Batch with Timestamp Guard
-- ============================================================================
-- This MERGE is the core of the idempotent pattern:
--
--   ON: Match on sensor_id + metric (the natural key for current-state readings)
--
--   WHEN MATCHED AND source.recorded_at > target.recorded_at:
--       Only update if the source reading is strictly NEWER. This is the
--       timestamp guard — it prevents stale data from overwriting fresh data.
--       10 rows pass (13:00 > 12:00). 5 stale rows fail (10:00 < 12:00).
--
--   WHEN NOT MATCHED:
--       Insert new sensors (TEMP-06, TEMP-07, TEMP-08). 5 rows inserted.
--
-- Result: 15 rows affected (10 updated + 5 inserted). 5 stale rows silently
-- skipped because they match on ON but fail the timestamp guard predicate.

ASSERT ROW_COUNT = 15
MERGE INTO {{zone_name}}.delta_demos.sensor_readings AS target
USING {{zone_name}}.delta_demos.sensor_batch AS source
ON target.sensor_id = source.sensor_id AND target.metric = source.metric
WHEN MATCHED AND source.recorded_at > target.recorded_at THEN
    UPDATE SET value = source.value,
               recorded_at = source.recorded_at,
               ingested_at = source.batch_id
WHEN NOT MATCHED THEN
    INSERT (sensor_id, location, metric, value, recorded_at, ingested_at)
    VALUES (source.sensor_id, source.location, source.metric, source.value,
            source.recorded_at, source.batch_id);


-- ============================================================================
-- EXPLORE: Post-First-MERGE State
-- ============================================================================
-- After the first MERGE:
--   - 25 original rows + 5 new sensor rows = 30 total
--   - 10 rows updated  (ingested_at changed from '2024-01-01 13:00:00' to 'BATCH-2024-001')
--   - 5 rows inserted  (new sensors TEMP-06/07/08, ingested_at = 'BATCH-2024-001')
--   - 15 rows unchanged (original ingested_at = '2024-01-01 13:00:00')

ASSERT ROW_COUNT = 30
SELECT sensor_id, location, metric, value, recorded_at, ingested_at
FROM {{zone_name}}.delta_demos.sensor_readings
ORDER BY sensor_id, metric;


-- ============================================================================
-- LEARN: Verify Stale Data Was Rejected
-- ============================================================================
-- The 5 stale batch rows targeted these sensor_id+metric combos with
-- recorded_at = '2024-01-01 10:00:00':
--   TEMP-01/pressure, TEMP-02/humidity, TEMP-03/pressure,
--   TEMP-04/airflow, TEMP-05/humidity
--
-- The timestamp guard (source.recorded_at > target.recorded_at) rejected
-- all 5 because 10:00 < 12:00. These rows should still have their
-- ORIGINAL values and ingested_at = '2024-01-01 13:00:00'.

ASSERT ROW_COUNT = 5
SELECT sensor_id, metric, value, recorded_at, ingested_at
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE (sensor_id = 'TEMP-01' AND metric = 'pressure')
   OR (sensor_id = 'TEMP-02' AND metric = 'humidity')
   OR (sensor_id = 'TEMP-03' AND metric = 'pressure')
   OR (sensor_id = 'TEMP-04' AND metric = 'airflow')
   OR (sensor_id = 'TEMP-05' AND metric = 'humidity')
ORDER BY sensor_id;


-- ============================================================================
-- MERGE: Second Run — Idempotent Re-run (Same Batch)
-- ============================================================================
-- Run the EXACT SAME MERGE again. This is the idempotency test.
--
-- What happens:
--   - The 10 "newer" source rows: source.recorded_at (13:00) is now NOT >
--     target.recorded_at (also 13:00, updated by the first MERGE).
--     Guard fails: 13:00 > 13:00 is FALSE. No updates.
--   - The 5 "new" sensors (TEMP-06/07/08): now exist in the target, so they
--     match on ON. They fall into WHEN MATCHED, not WHEN NOT MATCHED.
--     Guard fails: 13:00 > 13:00 is FALSE. No updates.
--   - The 5 stale rows: still stale, still blocked (10:00 > 12:00 is FALSE).
--
-- Result: 0 rows affected. The MERGE is a complete no-op.
-- This is idempotency — proof that re-running the pipeline is safe.

ASSERT ROW_COUNT = 0
MERGE INTO {{zone_name}}.delta_demos.sensor_readings AS target
USING {{zone_name}}.delta_demos.sensor_batch AS source
ON target.sensor_id = source.sensor_id AND target.metric = source.metric
WHEN MATCHED AND source.recorded_at > target.recorded_at THEN
    UPDATE SET value = source.value,
               recorded_at = source.recorded_at,
               ingested_at = source.batch_id
WHEN NOT MATCHED THEN
    INSERT (sensor_id, location, metric, value, recorded_at, ingested_at)
    VALUES (source.sensor_id, source.location, source.metric, source.value,
            source.recorded_at, source.batch_id);


-- ============================================================================
-- EXPLORE: Verify State Unchanged After Idempotent Re-run
-- ============================================================================
-- The table must be in exactly the same state as after the first MERGE.
-- Still 30 rows. Same distribution: 10 updated, 5 inserted, 15 unchanged.

ASSERT ROW_COUNT = 30
SELECT sensor_id, location, metric, value, recorded_at, ingested_at
FROM {{zone_name}}.delta_demos.sensor_readings
ORDER BY sensor_id, metric;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 25 original + 5 new sensors = 30
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.sensor_readings;

-- Verify batch_updated_rows: 10 existing-sensor rows updated by the batch
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE ingested_at = 'BATCH-2024-001'
  AND sensor_id IN ('TEMP-01', 'TEMP-02', 'TEMP-03', 'TEMP-04', 'TEMP-05');

-- Verify batch_inserted_rows: 5 new sensor rows from TEMP-06, TEMP-07, TEMP-08
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id IN ('TEMP-06', 'TEMP-07', 'TEMP-08');

-- Verify original_unchanged_rows: 15 rows still have original ingested_at
ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE ingested_at = '2024-01-01 13:00:00';

-- Verify no_duplicates: each sensor_id+metric combo appears exactly once
ASSERT VALUE duplicate_count = 0
SELECT COUNT(*) - COUNT(DISTINCT sensor_id || '|' || metric) AS duplicate_count
FROM {{zone_name}}.delta_demos.sensor_readings;

-- Verify stale_temp01_pressure_rejected: still original value 1013.8 (not stale 1012.0)
ASSERT VALUE value = 1013.8
SELECT value FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-01' AND metric = 'pressure';

-- Verify stale_temp02_humidity_rejected: still original value 45.2 (not stale 44.0)
ASSERT VALUE value = 45.2
SELECT value FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-02' AND metric = 'humidity';

-- Verify stale_temp03_pressure_rejected: still original value 1014.2 (not stale 1013.0)
ASSERT VALUE value = 1014.2
SELECT value FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-03' AND metric = 'pressure';

-- Verify stale_temp04_airflow_rejected: still original value 3.0 (not stale 2.5)
ASSERT VALUE value = 3.0
SELECT value FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-04' AND metric = 'airflow';

-- Verify stale_temp05_humidity_rejected: still original value 50.0 (not stale 48.0)
ASSERT VALUE value = 50.0
SELECT value FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-05' AND metric = 'humidity';

-- Verify newer_temp01_temperature_updated: updated to 23.5 from batch
ASSERT VALUE value = 23.5
SELECT value FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-01' AND metric = 'temperature';

-- Verify newer_temp03_humidity_updated: updated to 57.3 from batch
ASSERT VALUE value = 57.3
SELECT value FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-03' AND metric = 'humidity';

-- Verify new_sensor_cold_storage: TEMP-06 Cold Storage readings inserted
ASSERT VALUE cnt = 2
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-06' AND location = 'Cold Storage';

-- Verify new_sensor_rooftop: TEMP-07 Rooftop HVAC readings inserted
ASSERT VALUE cnt = 2
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-07' AND location = 'Rooftop HVAC';

-- Verify new_sensor_lobby: TEMP-08 Lobby reading inserted
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE sensor_id = 'TEMP-08' AND location = 'Lobby';
