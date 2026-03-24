-- ============================================================================
-- Delta MERGE — Idempotent Pipeline (Timestamp Guards) — Setup Script
-- ============================================================================
-- Creates the target and source tables for the idempotent MERGE demo.
--
-- Tables:
--   1. sensor_readings — 25 latest IoT sensor readings (target/fact table)
--        Each sensor_id+metric pair has exactly ONE row (the latest reading).
--   2. sensor_batch    — 20 staged readings (source/incoming batch)
--
-- The batch contains:
--   - 10 rows that UPDATE existing readings (newer recorded_at: 13:00 > 12:00)
--   - 5 rows that are STALE (older recorded_at: 10:00 < 12:00 — timestamp guard rejects)
--   - 5 rows that are NEW sensors (TEMP-06, TEMP-07, TEMP-08 — inserted)
--
-- First MERGE:  15 rows affected (10 updated + 5 inserted, 5 stale skipped)
-- Second MERGE: 0 rows affected (idempotent — safe to re-run)
-- Final count:  25 + 5 new = 30
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: sensor_readings — 25 latest IoT sensor readings (target)
-- ============================================================================
-- 5 sensors (TEMP-01 through TEMP-05), each reporting multiple metrics.
-- This is a "current state" table: one row per sensor_id+metric (the latest
-- reading). All recorded_at timestamps are '2024-01-01 12:00:00' (the most
-- recent hourly reading). All ingested at '2024-01-01 13:00:00'.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sensor_readings (
    sensor_id   VARCHAR,
    location    VARCHAR,
    metric      VARCHAR,
    value       DOUBLE,
    recorded_at VARCHAR,
    ingested_at VARCHAR
) LOCATION '{{data_path}}/sensor_readings';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.sensor_readings TO USER {{current_user}};

INSERT INTO {{zone_name}}.delta_demos.sensor_readings VALUES
    -- TEMP-01: Server Room A (5 metrics)
    ('TEMP-01', 'Server Room A', 'temperature',   22.4,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-01', 'Server Room A', 'humidity',       43.0,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-01', 'Server Room A', 'pressure',       1013.8,  '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-01', 'Server Room A', 'airflow',        2.4,     '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-01', 'Server Room A', 'power_draw',     340.5,   '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    -- TEMP-02: Server Room B (5 metrics)
    ('TEMP-02', 'Server Room B', 'temperature',   21.9,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-02', 'Server Room B', 'humidity',       45.2,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-02', 'Server Room B', 'pressure',       1013.5,  '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-02', 'Server Room B', 'airflow',        2.1,     '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-02', 'Server Room B', 'power_draw',     312.0,   '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    -- TEMP-03: Warehouse (5 metrics)
    ('TEMP-03', 'Warehouse',     'temperature',   18.5,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-03', 'Warehouse',     'humidity',       56.1,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-03', 'Warehouse',     'pressure',       1014.2,  '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-03', 'Warehouse',     'airflow',        1.8,     '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-03', 'Warehouse',     'power_draw',     185.0,   '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    -- TEMP-04: Office Floor 3 (5 metrics)
    ('TEMP-04', 'Office Floor 3','temperature',   24.7,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-04', 'Office Floor 3','humidity',       38.5,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-04', 'Office Floor 3','pressure',       1012.9,  '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-04', 'Office Floor 3','airflow',        3.0,     '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-04', 'Office Floor 3','power_draw',     420.0,   '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    -- TEMP-05: Lab (5 metrics)
    ('TEMP-05', 'Lab',           'temperature',   20.5,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-05', 'Lab',           'humidity',       50.0,    '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-05', 'Lab',           'pressure',       1015.3,  '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-05', 'Lab',           'airflow',        2.7,     '2024-01-01 12:00:00', '2024-01-01 13:00:00'),
    ('TEMP-05', 'Lab',           'power_draw',     275.0,   '2024-01-01 12:00:00', '2024-01-01 13:00:00');


-- ============================================================================
-- TABLE 2: sensor_batch — 20 staged readings (source/incoming batch)
-- ============================================================================
-- 10 NEWER rows:  same sensor_id+metric as existing, recorded_at = 13:00 (> 12:00)
-- 5  STALE rows:  same sensor_id+metric as existing, recorded_at = 10:00 (< 12:00)
-- 5  NEW rows:    sensors TEMP-06, TEMP-07, TEMP-08 (not in target — will INSERT)
-- All rows have batch_id = 'BATCH-2024-001'
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sensor_batch (
    sensor_id   VARCHAR,
    location    VARCHAR,
    metric      VARCHAR,
    value       DOUBLE,
    recorded_at VARCHAR,
    batch_id    VARCHAR
) LOCATION '{{data_path}}/sensor_batch';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.sensor_batch TO USER {{current_user}};

INSERT INTO {{zone_name}}.delta_demos.sensor_batch VALUES
    -- 10 NEWER readings (recorded_at = '2024-01-01 13:00:00' > target 12:00)
    -- These will UPDATE existing rows via the timestamp guard
    ('TEMP-01', 'Server Room A', 'temperature', 23.5,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-01', 'Server Room A', 'humidity',    44.2,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-02', 'Server Room B', 'temperature', 22.7,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-02', 'Server Room B', 'pressure',    1014.0,  '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-03', 'Warehouse',     'temperature', 19.4,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-03', 'Warehouse',     'humidity',    57.3,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-04', 'Office Floor 3','temperature', 25.1,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-04', 'Office Floor 3','humidity',    39.0,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-05', 'Lab',           'temperature', 20.9,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-05', 'Lab',           'pressure',    1015.8,  '2024-01-01 13:00:00', 'BATCH-2024-001'),
    -- 5 STALE readings (recorded_at = '2024-01-01 10:00:00' < target 12:00)
    -- These match ON sensor_id+metric but FAIL the timestamp guard — skipped
    ('TEMP-01', 'Server Room A', 'pressure',    1012.0,  '2024-01-01 10:00:00', 'BATCH-2024-001'),
    ('TEMP-02', 'Server Room B', 'humidity',    44.0,    '2024-01-01 10:00:00', 'BATCH-2024-001'),
    ('TEMP-03', 'Warehouse',     'pressure',    1013.0,  '2024-01-01 10:00:00', 'BATCH-2024-001'),
    ('TEMP-04', 'Office Floor 3','airflow',     2.5,     '2024-01-01 10:00:00', 'BATCH-2024-001'),
    ('TEMP-05', 'Lab',           'humidity',    48.0,    '2024-01-01 10:00:00', 'BATCH-2024-001'),
    -- 5 NEW sensor readings (TEMP-06, TEMP-07, TEMP-08 — not in target)
    -- These will INSERT via WHEN NOT MATCHED
    ('TEMP-06', 'Cold Storage',  'temperature', 4.2,     '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-06', 'Cold Storage',  'humidity',    62.0,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-07', 'Rooftop HVAC',  'temperature', 27.8,    '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-07', 'Rooftop HVAC',  'pressure',    1018.5,  '2024-01-01 13:00:00', 'BATCH-2024-001'),
    ('TEMP-08', 'Lobby',         'temperature', 22.0,    '2024-01-01 13:00:00', 'BATCH-2024-001');
