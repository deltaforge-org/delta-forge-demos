-- ============================================================================
-- Delta RESTORE + VACUUM — Incident Response Lifecycle — Educational Queries
-- ============================================================================
-- WHAT: Demonstrates the full incident response lifecycle using RESTORE and
--       VACUUM on a cold-storage IoT monitoring table.
-- WHY:  When bad data enters a Delta table (e.g., faulty sensor batch), RESTORE
--       rolls back to the last known-good version. VACUUM then permanently
--       removes the orphaned files, preventing time travel back to the bad state.
-- HOW:  V0=CREATE, V1=INSERT (setup), V2=calibration UPDATE, V3=bad import,
--       V4=RESTORE TO VERSION 2, V5=VACUUM, then resume normal operations.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — room summary after initial 30 readings (V1)
-- ============================================================================
-- After setup, 30 readings exist across 3 rooms. Each room has 10 readings
-- from 2 sensors over 5 time slots. All readings are within normal operating
-- ranges for each room type.

ASSERT VALUE reading_count = 10 WHERE room = 'room_a'
ASSERT VALUE reading_count = 10 WHERE room = 'room_b'
ASSERT VALUE reading_count = 10 WHERE room = 'room_c'
ASSERT VALUE avg_temp = -18.07 WHERE room = 'room_a'
ASSERT VALUE avg_temp = 3.95 WHERE room = 'room_b'
ASSERT VALUE avg_temp = 21.95 WHERE room = 'room_c'
ASSERT ROW_COUNT = 3
SELECT room,
       COUNT(*) AS reading_count,
       ROUND(AVG(temperature), 2) AS avg_temp,
       ROUND(AVG(humidity), 2) AS avg_humidity,
       ROUND(MIN(temperature), 2) AS min_temp,
       ROUND(MAX(temperature), 2) AS max_temp
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY room
ORDER BY room;


-- ============================================================================
-- V2: Calibration update — adjust room_a sensors (+0.3°C, +1.0% humidity)
-- ============================================================================
-- A technician recalibrates the cold-storage sensors. All room_a readings
-- get a +0.3°C temperature correction and +1.0% humidity correction.
-- This is a legitimate operation we want to preserve through the incident.

UPDATE {{zone_name}}.delta_demos.sensor_readings
SET temperature = temperature + 0.3, humidity = humidity + 1.0
WHERE room = 'room_a';


-- ============================================================================
-- EXPLORE: Verify calibration — room_a temperatures after adjustment
-- ============================================================================
-- Room A readings now show the +0.3°C offset. For example, id=1 moved from
-- -18.2 to -17.9. The calibration is correct and should survive the restore.

ASSERT VALUE avg_temp = -17.77 WHERE room = 'room_a'
ASSERT VALUE avg_humidity = 46.0 WHERE room = 'room_a'
ASSERT ROW_COUNT = 3
SELECT room,
       COUNT(*) AS reading_count,
       ROUND(AVG(temperature), 2) AS avg_temp,
       ROUND(AVG(humidity), 2) AS avg_humidity,
       ROUND(MIN(temperature), 2) AS min_temp,
       ROUND(MAX(temperature), 2) AS max_temp
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY room
ORDER BY room;


-- ============================================================================
-- EXPLORE: Verify specific calibrated values in room_a
-- ============================================================================
-- Spot-check individual sensor readings to confirm the calibration offset.

ASSERT VALUE temperature = -17.9 WHERE id = 1
ASSERT VALUE temperature = -18.0 WHERE id = 6
ASSERT ROW_COUNT = 10
SELECT id, sensor_id, temperature, humidity, reading_time
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE room = 'room_a'
ORDER BY id;


-- ============================================================================
-- V3: Bad import — faulty sensor batch sends extreme readings
-- ============================================================================
-- A malfunctioning sensor batch pushes 10 readings with wildly incorrect
-- temperature and humidity values. These show up as 'alert' status because
-- automated monitoring flags them. In reality, the sensors were faulty —
-- the rooms never actually reached these temperatures.

INSERT INTO {{zone_name}}.delta_demos.sensor_readings VALUES
    (31, 'S01', 'room_a', 25.0,  95.0, '2025-06-01 09:15', 'alert'),
    (32, 'S02', 'room_a', 28.0,  92.0, '2025-06-01 09:15', 'alert'),
    (33, 'S03', 'room_b', 35.0,  88.0, '2025-06-01 09:15', 'alert'),
    (34, 'S04', 'room_b', 32.0,  90.0, '2025-06-01 09:15', 'alert'),
    (35, 'S05', 'room_c', 45.0,  85.0, '2025-06-01 09:15', 'alert'),
    (36, 'S06', 'room_c', 42.0,  87.0, '2025-06-01 09:15', 'alert'),
    (37, 'S01', 'room_a', 26.0,  94.0, '2025-06-01 09:30', 'alert'),
    (38, 'S03', 'room_b', 33.0,  89.0, '2025-06-01 09:30', 'alert'),
    (39, 'S05', 'room_c', 44.0,  86.0, '2025-06-01 09:30', 'alert'),
    (40, 'S06', 'room_c', 43.0,  88.0, '2025-06-01 09:30', 'alert');


-- ============================================================================
-- EXPLORE: Assess the damage — alert readings skew room averages
-- ============================================================================
-- The 10 faulty readings have polluted the data. Room A shows an average
-- near -7.59°C instead of -17.77°C. The alert readings need to be removed,
-- but simply deleting them would create version V4 without preserving the
-- clean V2 state. RESTORE is the correct approach.

ASSERT VALUE total_rows = 40
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_rows
FROM {{zone_name}}.delta_demos.sensor_readings;

ASSERT VALUE alert_count = 10
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS alert_count
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE status = 'alert';

ASSERT VALUE avg_temp = 35.3
ASSERT VALUE avg_humidity = 89.4
ASSERT ROW_COUNT = 1
SELECT ROUND(AVG(temperature), 2) AS avg_temp,
       ROUND(AVG(humidity), 2) AS avg_humidity
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE status = 'alert';


-- ============================================================================
-- EXPLORE: Room-level impact — false alerts per room
-- ============================================================================
-- Each room received false alert readings. Room C was hit worst with 4 alerts.
-- The faulty temperatures are completely outside normal operating ranges:
-- Room A should be ~-18°C but shows 25-28°C alerts, Room B should be ~4°C
-- but shows 32-35°C, Room C should be ~22°C but shows 42-45°C.

ASSERT VALUE alert_count = 3 WHERE room = 'room_a'
ASSERT VALUE alert_count = 3 WHERE room = 'room_b'
ASSERT VALUE alert_count = 4 WHERE room = 'room_c'
ASSERT ROW_COUNT = 3
SELECT room,
       COUNT(*) AS total_count,
       SUM(CASE WHEN status = 'alert' THEN 1 ELSE 0 END) AS alert_count,
       ROUND(AVG(temperature), 2) AS avg_temp
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY room
ORDER BY room;


-- ============================================================================
-- EXPLORE: Pre-restore inspection — verify V2 is the correct target
-- ============================================================================
-- Before restoring, inspect Version 2 via time travel to confirm it has the
-- calibrated data (30 rows, no alerts, room_a temps adjusted by +0.3°C).
-- This is the last known-good version before the bad import.

ASSERT VALUE row_count = 30
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS row_count
FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 2;

ASSERT VALUE alert_count = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS alert_count
FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 2
WHERE status = 'alert';

ASSERT VALUE avg_temp = -17.77 WHERE room = 'room_a'
ASSERT ROW_COUNT = 3
SELECT room,
       ROUND(AVG(temperature), 2) AS avg_temp,
       ROUND(AVG(humidity), 2) AS avg_humidity
FROM {{zone_name}}.delta_demos.sensor_readings VERSION AS OF 2
GROUP BY room
ORDER BY room;


-- ============================================================================
-- V4: RESTORE TO VERSION 2 — undo the bad import, keep calibration
-- ============================================================================
-- RESTORE creates a new commit (V4) whose content matches V2. The bad import
-- (V3) still exists in the Delta log but is no longer the active version.
-- This is a metadata-only operation — it writes a new log entry pointing to
-- the V2 file set. No data files are copied or moved.

RESTORE {{zone_name}}.delta_demos.sensor_readings TO VERSION 2;


-- ============================================================================
-- LEARN: Post-restore verification — data matches V2 exactly
-- ============================================================================
-- After RESTORE, the table has 30 rows with no alerts. The calibration from
-- V2 is intact (room_a temperatures still show the +0.3°C offset). The bad
-- import data from V3 is no longer visible in the current version.

ASSERT VALUE total_rows = 30
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_rows
FROM {{zone_name}}.delta_demos.sensor_readings;

ASSERT VALUE alert_count = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS alert_count
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE status = 'alert';

ASSERT VALUE avg_temp = -17.77 WHERE room = 'room_a'
ASSERT VALUE avg_humidity = 46.0 WHERE room = 'room_a'
ASSERT VALUE avg_temp = 3.95 WHERE room = 'room_b'
ASSERT VALUE avg_temp = 21.95 WHERE room = 'room_c'
ASSERT ROW_COUNT = 3
SELECT room,
       COUNT(*) AS reading_count,
       ROUND(AVG(temperature), 2) AS avg_temp,
       ROUND(AVG(humidity), 2) AS avg_humidity
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY room
ORDER BY room;


-- ============================================================================
-- LEARN: Calibration survived the restore — spot-check room_a values
-- ============================================================================
-- The +0.3°C calibration offset applied in V2 is still present. RESTORE TO
-- VERSION 2 brought back the exact state after calibration, not the original
-- uncalibrated readings from V1.

ASSERT VALUE temperature = -17.9 WHERE id = 1
ASSERT VALUE temperature = -18.0 WHERE id = 6
ASSERT ROW_COUNT = 10
SELECT id, sensor_id, temperature, humidity
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE room = 'room_a'
ORDER BY id;


-- ============================================================================
-- V5: VACUUM — permanently remove orphaned files from the bad import
-- ============================================================================
-- RESTORE undid the bad import logically, but the Parquet files containing
-- the faulty readings still exist on disk. VACUUM removes these orphaned
-- files, reclaiming storage and — importantly — preventing anyone from using
-- time travel to access the corrupted V3 data.

VACUUM {{zone_name}}.delta_demos.sensor_readings;


-- ============================================================================
-- LEARN: Post-VACUUM verification — data is unchanged
-- ============================================================================
-- VACUUM only affects physical files, not the logical table state. All 30
-- rows remain intact with identical values. The only difference is that
-- time travel to V3 (the bad import) will now fail because those files
-- have been permanently deleted.

ASSERT VALUE total_rows = 30
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_rows
FROM {{zone_name}}.delta_demos.sensor_readings;

ASSERT VALUE avg_temp = -17.77 WHERE room = 'room_a'
ASSERT VALUE avg_humidity = 46.0 WHERE room = 'room_a'
ASSERT ROW_COUNT = 3
SELECT room,
       COUNT(*) AS reading_count,
       ROUND(AVG(temperature), 2) AS avg_temp,
       ROUND(AVG(humidity), 2) AS avg_humidity
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY room
ORDER BY room;


-- ============================================================================
-- RESUME: Insert 5 new good readings — normal operations continue
-- ============================================================================
-- With the incident resolved and orphaned files cleaned up, the facility
-- resumes collecting sensor data. These 5 readings are from the next time
-- slot and all show normal values within expected ranges.

INSERT INTO {{zone_name}}.delta_demos.sensor_readings VALUES
    (31, 'S01', 'room_a', -17.8, 46.2, '2025-06-01 09:15', 'normal'),
    (32, 'S03', 'room_b',   4.1, 60.0, '2025-06-01 09:15', 'normal'),
    (33, 'S05', 'room_c',  22.0, 35.1, '2025-06-01 09:15', 'normal'),
    (34, 'S02', 'room_a', -18.0, 46.0, '2025-06-01 09:15', 'normal'),
    (35, 'S04', 'room_b',   3.9, 60.3, '2025-06-01 09:15', 'normal');


-- ============================================================================
-- LEARN: Final state — 35 rows, all normal, no trace of the incident
-- ============================================================================
-- The table now has 35 rows: 30 from the restored V2 state plus 5 new
-- readings. All statuses are 'normal'. The calibration offset is preserved.
-- The faulty sensor batch data is completely gone from both the current
-- version and the physical files.

ASSERT VALUE total_rows = 35
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_rows
FROM {{zone_name}}.delta_demos.sensor_readings;

ASSERT VALUE alert_count = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS alert_count
FROM {{zone_name}}.delta_demos.sensor_readings
WHERE status = 'alert';

ASSERT VALUE reading_count = 12 WHERE room = 'room_a'
ASSERT VALUE reading_count = 12 WHERE room = 'room_b'
ASSERT VALUE reading_count = 11 WHERE room = 'room_c'
ASSERT VALUE avg_temp = -17.79 WHERE room = 'room_a'
ASSERT VALUE avg_temp = 3.96 WHERE room = 'room_b'
ASSERT VALUE avg_temp = 21.95 WHERE room = 'room_c'
ASSERT ROW_COUNT = 3
SELECT room,
       COUNT(*) AS reading_count,
       ROUND(AVG(temperature), 2) AS avg_temp,
       ROUND(AVG(humidity), 2) AS avg_humidity,
       ROUND(MIN(temperature), 2) AS min_temp,
       ROUND(MAX(temperature), 2) AS max_temp
FROM {{zone_name}}.delta_demos.sensor_readings
GROUP BY room
ORDER BY room;


-- ============================================================================
-- VERIFY: All checks — cross-cutting verification of the incident lifecycle
-- ============================================================================
-- Final comprehensive check: correct total, correct per-room distribution,
-- all readings normal, calibration intact, no orphaned alert data.

ASSERT VALUE total_rows = 35
ASSERT VALUE distinct_sensors = 6
ASSERT VALUE all_normal = 35
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT sensor_id) AS distinct_sensors,
       SUM(CASE WHEN status = 'normal' THEN 1 ELSE 0 END) AS all_normal
FROM {{zone_name}}.delta_demos.sensor_readings;
