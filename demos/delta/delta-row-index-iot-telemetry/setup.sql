-- ============================================================================
-- Industrial IoT Telemetry — Composite Row-Level Index — Setup Script
-- ============================================================================
-- Sensor telemetry from 8 plant sensors (4 vibration, 4 temperature),
-- 10 readings each, across two ingest batches (morning, afternoon).
-- The composite index on (sensor_id, reading_time) accelerates queries
-- that filter on sensor_id, or on sensor_id together with reading_time.
--
-- Tables created:
--   1. sensor_telemetry — 80 readings (8 sensors × 10 readings)
--
-- Operations performed:
--   1. CREATE DELTA TABLE
--   2. INSERT batch 1 — 40 morning readings
--   3. INSERT batch 2 — 40 afternoon readings
--
-- The composite CREATE INDEX statement lives in queries.sql so the
-- leftmost-prefix rule is taught alongside the queries it governs.
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sensor_telemetry (
    sensor_id     VARCHAR,
    reading_time  VARCHAR,
    sensor_kind   VARCHAR,
    line_id       VARCHAR,
    value         DOUBLE,
    unit          VARCHAR,
    quality       VARCHAR
) LOCATION 'sensor_telemetry';


-- Batch 1 — 40 morning readings (08:00..12:30)
INSERT INTO {{zone_name}}.delta_demos.sensor_telemetry VALUES
    ('VIB-A01','2026-04-15T08:00:00','vibration','line_a',  0.42,'mm_s','good'),
    ('VIB-A01','2026-04-15T08:30:00','vibration','line_a',  0.45,'mm_s','good'),
    ('VIB-A01','2026-04-15T09:00:00','vibration','line_a',  0.51,'mm_s','good'),
    ('VIB-A01','2026-04-15T09:30:00','vibration','line_a',  0.49,'mm_s','good'),
    ('VIB-A01','2026-04-15T10:00:00','vibration','line_a',  0.58,'mm_s','warn'),
    ('VIB-A02','2026-04-15T08:00:00','vibration','line_a',  0.31,'mm_s','good'),
    ('VIB-A02','2026-04-15T08:30:00','vibration','line_a',  0.33,'mm_s','good'),
    ('VIB-A02','2026-04-15T09:00:00','vibration','line_a',  0.29,'mm_s','good'),
    ('VIB-A02','2026-04-15T09:30:00','vibration','line_a',  0.34,'mm_s','good'),
    ('VIB-A02','2026-04-15T10:00:00','vibration','line_a',  0.36,'mm_s','good'),
    ('VIB-B03','2026-04-15T08:00:00','vibration','line_b',  0.62,'mm_s','warn'),
    ('VIB-B03','2026-04-15T08:30:00','vibration','line_b',  0.71,'mm_s','warn'),
    ('VIB-B03','2026-04-15T09:00:00','vibration','line_b',  0.68,'mm_s','warn'),
    ('VIB-B03','2026-04-15T09:30:00','vibration','line_b',  0.74,'mm_s','warn'),
    ('VIB-B03','2026-04-15T10:00:00','vibration','line_b',  0.81,'mm_s','alarm'),
    ('VIB-B04','2026-04-15T08:00:00','vibration','line_b',  0.40,'mm_s','good'),
    ('VIB-B04','2026-04-15T08:30:00','vibration','line_b',  0.43,'mm_s','good'),
    ('VIB-B04','2026-04-15T09:00:00','vibration','line_b',  0.45,'mm_s','good'),
    ('VIB-B04','2026-04-15T09:30:00','vibration','line_b',  0.47,'mm_s','good'),
    ('VIB-B04','2026-04-15T10:00:00','vibration','line_b',  0.50,'mm_s','good'),
    ('TMP-A05','2026-04-15T08:00:00','temperature','line_a', 64.2,'celsius','good'),
    ('TMP-A05','2026-04-15T08:30:00','temperature','line_a', 65.1,'celsius','good'),
    ('TMP-A05','2026-04-15T09:00:00','temperature','line_a', 66.4,'celsius','good'),
    ('TMP-A05','2026-04-15T09:30:00','temperature','line_a', 67.0,'celsius','good'),
    ('TMP-A05','2026-04-15T10:00:00','temperature','line_a', 68.3,'celsius','good'),
    ('TMP-A06','2026-04-15T08:00:00','temperature','line_a', 71.5,'celsius','good'),
    ('TMP-A06','2026-04-15T08:30:00','temperature','line_a', 72.0,'celsius','good'),
    ('TMP-A06','2026-04-15T09:00:00','temperature','line_a', 72.8,'celsius','good'),
    ('TMP-A06','2026-04-15T09:30:00','temperature','line_a', 74.1,'celsius','warn'),
    ('TMP-A06','2026-04-15T10:00:00','temperature','line_a', 75.4,'celsius','warn'),
    ('TMP-B07','2026-04-15T08:00:00','temperature','line_b', 58.9,'celsius','good'),
    ('TMP-B07','2026-04-15T08:30:00','temperature','line_b', 59.4,'celsius','good'),
    ('TMP-B07','2026-04-15T09:00:00','temperature','line_b', 60.1,'celsius','good'),
    ('TMP-B07','2026-04-15T09:30:00','temperature','line_b', 60.8,'celsius','good'),
    ('TMP-B07','2026-04-15T10:00:00','temperature','line_b', 61.5,'celsius','good'),
    ('TMP-B08','2026-04-15T08:00:00','temperature','line_b', 80.2,'celsius','warn'),
    ('TMP-B08','2026-04-15T08:30:00','temperature','line_b', 81.6,'celsius','warn'),
    ('TMP-B08','2026-04-15T09:00:00','temperature','line_b', 83.4,'celsius','alarm'),
    ('TMP-B08','2026-04-15T09:30:00','temperature','line_b', 82.7,'celsius','alarm'),
    ('TMP-B08','2026-04-15T10:00:00','temperature','line_b', 84.1,'celsius','alarm');


-- Batch 2 — 40 afternoon readings (12:30..17:00)
INSERT INTO {{zone_name}}.delta_demos.sensor_telemetry VALUES
    ('VIB-A01','2026-04-15T12:30:00','vibration','line_a',  0.55,'mm_s','warn'),
    ('VIB-A01','2026-04-15T13:30:00','vibration','line_a',  0.52,'mm_s','warn'),
    ('VIB-A01','2026-04-15T14:30:00','vibration','line_a',  0.48,'mm_s','good'),
    ('VIB-A01','2026-04-15T15:30:00','vibration','line_a',  0.46,'mm_s','good'),
    ('VIB-A01','2026-04-15T16:30:00','vibration','line_a',  0.44,'mm_s','good'),
    ('VIB-A02','2026-04-15T12:30:00','vibration','line_a',  0.32,'mm_s','good'),
    ('VIB-A02','2026-04-15T13:30:00','vibration','line_a',  0.30,'mm_s','good'),
    ('VIB-A02','2026-04-15T14:30:00','vibration','line_a',  0.28,'mm_s','good'),
    ('VIB-A02','2026-04-15T15:30:00','vibration','line_a',  0.27,'mm_s','good'),
    ('VIB-A02','2026-04-15T16:30:00','vibration','line_a',  0.31,'mm_s','good'),
    ('VIB-B03','2026-04-15T12:30:00','vibration','line_b',  0.78,'mm_s','warn'),
    ('VIB-B03','2026-04-15T13:30:00','vibration','line_b',  0.72,'mm_s','warn'),
    ('VIB-B03','2026-04-15T14:30:00','vibration','line_b',  0.65,'mm_s','warn'),
    ('VIB-B03','2026-04-15T15:30:00','vibration','line_b',  0.59,'mm_s','warn'),
    ('VIB-B03','2026-04-15T16:30:00','vibration','line_b',  0.55,'mm_s','warn'),
    ('VIB-B04','2026-04-15T12:30:00','vibration','line_b',  0.49,'mm_s','good'),
    ('VIB-B04','2026-04-15T13:30:00','vibration','line_b',  0.46,'mm_s','good'),
    ('VIB-B04','2026-04-15T14:30:00','vibration','line_b',  0.44,'mm_s','good'),
    ('VIB-B04','2026-04-15T15:30:00','vibration','line_b',  0.42,'mm_s','good'),
    ('VIB-B04','2026-04-15T16:30:00','vibration','line_b',  0.40,'mm_s','good'),
    ('TMP-A05','2026-04-15T12:30:00','temperature','line_a', 69.1,'celsius','good'),
    ('TMP-A05','2026-04-15T13:30:00','temperature','line_a', 70.0,'celsius','good'),
    ('TMP-A05','2026-04-15T14:30:00','temperature','line_a', 70.8,'celsius','good'),
    ('TMP-A05','2026-04-15T15:30:00','temperature','line_a', 71.2,'celsius','good'),
    ('TMP-A05','2026-04-15T16:30:00','temperature','line_a', 70.5,'celsius','good'),
    ('TMP-A06','2026-04-15T12:30:00','temperature','line_a', 76.2,'celsius','warn'),
    ('TMP-A06','2026-04-15T13:30:00','temperature','line_a', 77.0,'celsius','warn'),
    ('TMP-A06','2026-04-15T14:30:00','temperature','line_a', 78.1,'celsius','warn'),
    ('TMP-A06','2026-04-15T15:30:00','temperature','line_a', 76.5,'celsius','warn'),
    ('TMP-A06','2026-04-15T16:30:00','temperature','line_a', 74.8,'celsius','warn'),
    ('TMP-B07','2026-04-15T12:30:00','temperature','line_b', 62.0,'celsius','good'),
    ('TMP-B07','2026-04-15T13:30:00','temperature','line_b', 62.8,'celsius','good'),
    ('TMP-B07','2026-04-15T14:30:00','temperature','line_b', 63.4,'celsius','good'),
    ('TMP-B07','2026-04-15T15:30:00','temperature','line_b', 63.0,'celsius','good'),
    ('TMP-B07','2026-04-15T16:30:00','temperature','line_b', 62.5,'celsius','good'),
    ('TMP-B08','2026-04-15T12:30:00','temperature','line_b', 85.0,'celsius','alarm'),
    ('TMP-B08','2026-04-15T13:30:00','temperature','line_b', 86.3,'celsius','alarm'),
    ('TMP-B08','2026-04-15T14:30:00','temperature','line_b', 84.7,'celsius','alarm'),
    ('TMP-B08','2026-04-15T15:30:00','temperature','line_b', 83.2,'celsius','alarm'),
    ('TMP-B08','2026-04-15T16:30:00','temperature','line_b', 81.9,'celsius','warn');
