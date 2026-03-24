-- ============================================================================
-- Delta Auto-Optimize — Interactive Write & Observe Queries
-- ============================================================================
-- WHAT: Auto-optimize automatically compacts small files during writes
-- WHY:  Frequent small INSERTs create many tiny Parquet files, degrading read
--       performance due to file-open overhead and metadata bloat
-- HOW:  SET AUTO OPTIMIZE enables delta.autoOptimize.autoCompact, which
--       triggers a post-write compaction job when the small file count
--       exceeds the threshold (lowered to 3 for this demo)
--
-- This demo walks through 6 incremental INSERT batches (batches 2-7), an
-- UPDATE for quality flagging, and SELECT queries to verify data integrity.
-- Auto-optimize compacts files behind the scenes after writes that push
-- the small file count past the threshold.
-- ============================================================================


-- ============================================================================
-- CHECK: Baseline state after setup (batch 1 already loaded)
-- ============================================================================
-- Setup inserted 10 temperature readings (batch 1) and enabled auto-optimize
-- via SET AUTO OPTIMIZE ON. Let's confirm the starting state.

-- Verify baseline: 10 temperature readings in batch 1
ASSERT VALUE baseline_count = 10
SELECT COUNT(*) AS baseline_count FROM {{zone_name}}.delta_demos.iot_readings;

-- Verify auto-optimize is enabled on this table
ASSERT ROW_COUNT = 5
SELECT * FROM (DESCRIBE AUTO OPTIMIZE {{zone_name}}.delta_demos.iot_readings);


-- ============================================================================
-- BATCH 2: Humidity readings (10 rows) — 1 extreme value (> 90%)
-- ============================================================================
-- Each INSERT creates new data files. With autoCompact enabled and a low
-- threshold of 3 files, compaction will trigger once we accumulate enough
-- small files.

INSERT INTO {{zone_name}}.delta_demos.iot_readings VALUES
    (11, 'DEV-001', 'humidity', 55.0, 'percent', 'good', 2, '2025-01-15 09:00:00'),
    (12, 'DEV-002', 'humidity', 62.3, 'percent', 'good', 2, '2025-01-15 09:01:00'),
    (13, 'DEV-003', 'humidity', 48.7, 'percent', 'good', 2, '2025-01-15 09:02:00'),
    (14, 'DEV-004', 'humidity', 70.1, 'percent', 'good', 2, '2025-01-15 09:03:00'),
    (15, 'DEV-005', 'humidity', 93.5, 'percent', 'good', 2, '2025-01-15 09:04:00'),
    (16, 'DEV-006', 'humidity', 58.2, 'percent', 'good', 2, '2025-01-15 09:05:00'),
    (17, 'DEV-007', 'humidity', 65.0, 'percent', 'good', 2, '2025-01-15 09:06:00'),
    (18, 'DEV-008', 'humidity', 72.4, 'percent', 'good', 2, '2025-01-15 09:07:00'),
    (19, 'DEV-009', 'humidity', 44.9, 'percent', 'good', 2, '2025-01-15 09:08:00'),
    (20, 'DEV-010', 'humidity', 60.0, 'percent', 'good', 2, '2025-01-15 09:09:00');


-- Observe: we now have 2 batches (20 rows).
ASSERT ROW_COUNT = 2
SELECT batch_id,
       metric,
       COUNT(*) AS row_count
FROM {{zone_name}}.delta_demos.iot_readings
GROUP BY batch_id, metric
ORDER BY batch_id;


-- ============================================================================
-- BATCH 3: Pressure readings (10 rows) — 1 extreme value (> 1050 hPa)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.iot_readings VALUES
    (21, 'DEV-001', 'pressure', 1013.2, 'hPa', 'good', 3, '2025-01-15 10:00:00'),
    (22, 'DEV-002', 'pressure', 1010.5, 'hPa', 'good', 3, '2025-01-15 10:01:00'),
    (23, 'DEV-003', 'pressure', 1015.8, 'hPa', 'good', 3, '2025-01-15 10:02:00'),
    (24, 'DEV-004', 'pressure', 1008.3, 'hPa', 'good', 3, '2025-01-15 10:03:00'),
    (25, 'DEV-005', 'pressure', 1055.0, 'hPa', 'good', 3, '2025-01-15 10:04:00'),
    (26, 'DEV-006', 'pressure', 1012.1, 'hPa', 'good', 3, '2025-01-15 10:05:00'),
    (27, 'DEV-007', 'pressure', 1018.9, 'hPa', 'good', 3, '2025-01-15 10:06:00'),
    (28, 'DEV-008', 'pressure', 1009.7, 'hPa', 'good', 3, '2025-01-15 10:07:00'),
    (29, 'DEV-009', 'pressure', 1014.4, 'hPa', 'good', 3, '2025-01-15 10:08:00'),
    (30, 'DEV-010', 'pressure', 1011.6, 'hPa', 'good', 3, '2025-01-15 10:09:00');


-- ============================================================================
-- BATCH 4: Wind speed readings (10 rows) — 1 extreme value (> 55 km/h)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.iot_readings VALUES
    (31, 'DEV-001', 'wind_speed', 12.3, 'km/h', 'good', 4, '2025-01-15 11:00:00'),
    (32, 'DEV-002', 'wind_speed', 8.7,  'km/h', 'good', 4, '2025-01-15 11:01:00'),
    (33, 'DEV-003', 'wind_speed', 15.1, 'km/h', 'good', 4, '2025-01-15 11:02:00'),
    (34, 'DEV-004', 'wind_speed', 22.0, 'km/h', 'good', 4, '2025-01-15 11:03:00'),
    (35, 'DEV-005', 'wind_speed', 58.4, 'km/h', 'good', 4, '2025-01-15 11:04:00'),
    (36, 'DEV-006', 'wind_speed', 10.2, 'km/h', 'good', 4, '2025-01-15 11:05:00'),
    (37, 'DEV-007', 'wind_speed', 18.5, 'km/h', 'good', 4, '2025-01-15 11:06:00'),
    (38, 'DEV-008', 'wind_speed', 25.8, 'km/h', 'good', 4, '2025-01-15 11:07:00'),
    (39, 'DEV-009', 'wind_speed', 7.3,  'km/h', 'good', 4, '2025-01-15 11:08:00'),
    (40, 'DEV-010', 'wind_speed', 14.0, 'km/h', 'good', 4, '2025-01-15 11:09:00');


-- Midpoint check: 4 batches loaded (40 rows). Each device has 4 readings
-- across 4 distinct metrics (temperature, humidity, pressure, wind_speed).
ASSERT ROW_COUNT = 10
ASSERT VALUE total_readings = 4 WHERE device_id = 'DEV-001'
ASSERT VALUE distinct_metrics = 4 WHERE device_id = 'DEV-001'
SELECT device_id,
       COUNT(*) AS total_readings,
       COUNT(DISTINCT metric) AS distinct_metrics
FROM {{zone_name}}.delta_demos.iot_readings
GROUP BY device_id
ORDER BY device_id;


-- ============================================================================
-- BATCH 5: Light readings (10 rows) — 1 extreme value (> 900 lux)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.iot_readings VALUES
    (41, 'DEV-001', 'light', 320.0, 'lux', 'good', 5, '2025-01-15 12:00:00'),
    (42, 'DEV-002', 'light', 450.5, 'lux', 'good', 5, '2025-01-15 12:01:00'),
    (43, 'DEV-003', 'light', 280.0, 'lux', 'good', 5, '2025-01-15 12:02:00'),
    (44, 'DEV-004', 'light', 510.3, 'lux', 'good', 5, '2025-01-15 12:03:00'),
    (45, 'DEV-005', 'light', 950.0, 'lux', 'good', 5, '2025-01-15 12:04:00'),
    (46, 'DEV-006', 'light', 375.2, 'lux', 'good', 5, '2025-01-15 12:05:00'),
    (47, 'DEV-007', 'light', 420.0, 'lux', 'good', 5, '2025-01-15 12:06:00'),
    (48, 'DEV-008', 'light', 290.8, 'lux', 'good', 5, '2025-01-15 12:07:00'),
    (49, 'DEV-009', 'light', 530.0, 'lux', 'good', 5, '2025-01-15 12:08:00'),
    (50, 'DEV-010', 'light', 410.0, 'lux', 'good', 5, '2025-01-15 12:09:00');


-- ============================================================================
-- BATCH 6: Noise readings (10 rows) — 1 extreme value (> 85 dB)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.iot_readings VALUES
    (51, 'DEV-001', 'noise', 42.0, 'dB', 'good', 6, '2025-01-15 13:00:00'),
    (52, 'DEV-002', 'noise', 55.3, 'dB', 'good', 6, '2025-01-15 13:01:00'),
    (53, 'DEV-003', 'noise', 38.7, 'dB', 'good', 6, '2025-01-15 13:02:00'),
    (54, 'DEV-004', 'noise', 61.0, 'dB', 'good', 6, '2025-01-15 13:03:00'),
    (55, 'DEV-005', 'noise', 92.0, 'dB', 'good', 6, '2025-01-15 13:04:00'),
    (56, 'DEV-006', 'noise', 47.5, 'dB', 'good', 6, '2025-01-15 13:05:00'),
    (57, 'DEV-007', 'noise', 50.0, 'dB', 'good', 6, '2025-01-15 13:06:00'),
    (58, 'DEV-008', 'noise', 65.2, 'dB', 'good', 6, '2025-01-15 13:07:00'),
    (59, 'DEV-009', 'noise', 35.0, 'dB', 'good', 6, '2025-01-15 13:08:00'),
    (60, 'DEV-010', 'noise', 44.8, 'dB', 'good', 6, '2025-01-15 13:09:00');


-- ============================================================================
-- BATCH 7: Vibration readings (10 rows) — 1 extreme value (> 8.0 mm/s)
-- ============================================================================
-- This is the final batch. After this INSERT, all 70 rows are loaded across
-- 7 metric types. Auto-optimize has been compacting files after each write
-- that pushed small file count past the threshold of 3.

INSERT INTO {{zone_name}}.delta_demos.iot_readings VALUES
    (61, 'DEV-001', 'vibration', 2.1, 'mm/s', 'good', 7, '2025-01-15 14:00:00'),
    (62, 'DEV-002', 'vibration', 3.5, 'mm/s', 'good', 7, '2025-01-15 14:01:00'),
    (63, 'DEV-003', 'vibration', 1.8, 'mm/s', 'good', 7, '2025-01-15 14:02:00'),
    (64, 'DEV-004', 'vibration', 4.2, 'mm/s', 'good', 7, '2025-01-15 14:03:00'),
    (65, 'DEV-005', 'vibration', 9.5, 'mm/s', 'good', 7, '2025-01-15 14:04:00'),
    (66, 'DEV-006', 'vibration', 2.7, 'mm/s', 'good', 7, '2025-01-15 14:05:00'),
    (67, 'DEV-007', 'vibration', 3.0, 'mm/s', 'good', 7, '2025-01-15 14:06:00'),
    (68, 'DEV-008', 'vibration', 5.1, 'mm/s', 'good', 7, '2025-01-15 14:07:00'),
    (69, 'DEV-009', 'vibration', 1.5, 'mm/s', 'good', 7, '2025-01-15 14:08:00'),
    (70, 'DEV-010', 'vibration', 2.9, 'mm/s', 'good', 7, '2025-01-15 14:09:00');


-- All 7 batches loaded. Verify the batch distribution.
ASSERT ROW_COUNT = 7
SELECT batch_id,
       metric,
       COUNT(*) AS row_count,
       MIN(recorded_at) AS first_reading,
       MAX(recorded_at) AS last_reading
FROM {{zone_name}}.delta_demos.iot_readings
GROUP BY batch_id, metric
ORDER BY batch_id;


-- ============================================================================
-- UPDATE: Flag 8 rows with extreme values as poor quality
-- ============================================================================
-- This UPDATE also generates new data files (Delta is copy-on-write).
-- With auto-optimize enabled, the rewritten files are compacted automatically.
--
-- Extreme thresholds:
--   temperature > 45   -> ids 5, 8           (2 rows)
--   humidity > 90      -> id 15              (1 row)
--   pressure > 1050    -> id 25              (1 row)
--   wind_speed > 55    -> id 35              (1 row)
--   light > 900        -> id 45              (1 row)
--   noise > 85         -> id 55              (1 row)
--   vibration > 8.0    -> id 65              (1 row)
-- Total: 8 rows updated to quality='poor'

UPDATE {{zone_name}}.delta_demos.iot_readings
SET quality = 'poor'
WHERE (metric = 'temperature' AND value > 45.0)
   OR (metric = 'humidity'    AND value > 90.0)
   OR (metric = 'pressure'    AND value > 1050.0)
   OR (metric = 'wind_speed'  AND value > 55.0)
   OR (metric = 'light'       AND value > 900.0)
   OR (metric = 'noise'       AND value > 85.0)
   OR (metric = 'vibration'   AND value > 8.0);


-- ============================================================================
-- EXPLORE: Identifying Extreme Readings
-- ============================================================================
-- Confirm the 8 rows that were flagged as 'poor' quality.
-- DEV-005 has an extreme reading in every metric (ids 5, 15, 25, 35, 45, 55, 65)
-- plus DEV-008 has extreme temperature (id 8).

ASSERT ROW_COUNT = 8
SELECT id, device_id, metric, value, unit, quality
FROM {{zone_name}}.delta_demos.iot_readings
WHERE quality = 'poor'
ORDER BY metric, id;


-- ============================================================================
-- LEARN: How Auto-Optimize Works
-- ============================================================================
-- When delta.autoOptimize.autoCompact is enabled, a post-write compaction
-- job runs after each INSERT/UPDATE/DELETE. It checks if the number of small
-- files (< 64 MB) exceeds the threshold (spark.databricks.delta.autoCompact
-- .minNumFiles, set to 3 for this demo). If so, OPTIMIZE runs automatically,
-- merging small files into larger ones — eliminating the small files problem
-- without manual intervention.

-- Verify DEV-005 has 7 poor-quality readings (one extreme per metric)
ASSERT VALUE poor_readings = 7
SELECT COUNT(*) FILTER (WHERE quality = 'poor') AS poor_readings
FROM {{zone_name}}.delta_demos.iot_readings WHERE device_id = 'DEV-005';

ASSERT ROW_COUNT = 10
ASSERT VALUE total_readings = 7 WHERE device_id = 'DEV-001'
ASSERT VALUE distinct_metrics = 7 WHERE device_id = 'DEV-001'
ASSERT VALUE poor_readings = 7 WHERE device_id = 'DEV-005'
ASSERT VALUE poor_readings = 1 WHERE device_id = 'DEV-008'
SELECT device_id,
       COUNT(*) AS total_readings,
       COUNT(DISTINCT metric) AS distinct_metrics,
       COUNT(*) FILTER (WHERE quality = 'poor') AS poor_readings
FROM {{zone_name}}.delta_demos.iot_readings
GROUP BY device_id
ORDER BY device_id;


-- ============================================================================
-- LEARN: Metric Summary Statistics
-- ============================================================================
-- Aggregating across metrics shows the range and average for each sensor type.
-- In a real system, these statistics help set thresholds for quality flagging.

ASSERT ROW_COUNT = 7
ASSERT VALUE min_value = 19.8 WHERE metric = 'temperature'
ASSERT VALUE avg_value = 28.1 WHERE metric = 'temperature'
ASSERT VALUE max_value = 50.1 WHERE metric = 'temperature'
ASSERT VALUE outliers = 2 WHERE metric = 'temperature'
ASSERT VALUE outliers = 1 WHERE metric = 'humidity'
SELECT metric,
       unit,
       COUNT(*) AS readings,
       ROUND(MIN(value), 2) AS min_value,
       ROUND(AVG(value), 2) AS avg_value,
       ROUND(MAX(value), 2) AS max_value,
       COUNT(*) FILTER (WHERE quality = 'poor') AS outliers
FROM {{zone_name}}.delta_demos.iot_readings
GROUP BY metric, unit
ORDER BY metric;


-- ============================================================================
-- EXPLORE: Browse All IoT Readings
-- ============================================================================

ASSERT ROW_COUNT = 70
SELECT id, device_id, metric, value, unit, quality, batch_id, recorded_at
FROM {{zone_name}}.delta_demos.iot_readings
ORDER BY batch_id, id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Summary verification ensuring all 7 batches loaded correctly with proper
-- quality flags applied.

-- Verify total row count
ASSERT ROW_COUNT = 70
SELECT * FROM {{zone_name}}.delta_demos.iot_readings;

-- Verify metric count
ASSERT VALUE metric_count = 7
SELECT COUNT(DISTINCT metric) AS metric_count FROM {{zone_name}}.delta_demos.iot_readings;

-- Verify batch count
ASSERT VALUE batch_count = 7
SELECT COUNT(DISTINCT batch_id) AS batch_count FROM {{zone_name}}.delta_demos.iot_readings;

-- Verify poor quality count
ASSERT VALUE poor_quality_count = 8
SELECT COUNT(*) AS poor_quality_count FROM {{zone_name}}.delta_demos.iot_readings WHERE quality = 'poor';

-- Verify good quality count
ASSERT VALUE good_quality_count = 62
SELECT COUNT(*) AS good_quality_count FROM {{zone_name}}.delta_demos.iot_readings WHERE quality = 'good';

-- Verify device count
ASSERT VALUE device_count = 10
SELECT COUNT(DISTINCT device_id) AS device_count FROM {{zone_name}}.delta_demos.iot_readings;

-- Verify temperature count
ASSERT VALUE temperature_count = 10
SELECT COUNT(*) AS temperature_count FROM {{zone_name}}.delta_demos.iot_readings WHERE metric = 'temperature';

-- Verify average temperature value
ASSERT VALUE avg_temp = 28.1
SELECT ROUND(AVG(value), 1) AS avg_temp FROM {{zone_name}}.delta_demos.iot_readings WHERE metric = 'temperature';
