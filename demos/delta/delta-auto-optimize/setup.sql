-- ============================================================================
-- Delta Auto-Optimize — Automatic Compaction & Write Optimization — Setup
-- ============================================================================
-- An IoT platform ingests sensor data in small batches. Each batch represents
-- a different metric type. Without auto-optimize, each small INSERT creates a
-- tiny file, leading to the "small files problem". By enabling optimizeWrite
-- and autoCompact table properties, Delta automatically coalesces small writes
-- into optimally-sized files.
--
-- This setup creates the table with auto-optimize TBLPROPERTIES and inserts
-- one initial batch of temperature data. The remaining 6 batches, UPDATE, and
-- analytical queries are in queries.sql so you can run them interactively and
-- observe auto-optimize compaction after each write.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- CREATE TABLE with auto-optimize TBLPROPERTIES
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.iot_readings (
    id          INT,
    device_id   VARCHAR,
    metric      VARCHAR,
    value       DOUBLE,
    unit        VARCHAR,
    quality     VARCHAR,
    batch_id    INT,
    recorded_at VARCHAR
) LOCATION '{{data_path}}/iot_readings'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.iot_readings TO USER {{current_user}};


-- ============================================================================
-- BATCH 1: Temperature readings (10 rows) — baseline data
-- ============================================================================
-- This initial batch seeds the table. The remaining 6 batches are in
-- queries.sql so you can observe auto-optimize compacting after each write.
INSERT INTO {{zone_name}}.delta_demos.iot_readings VALUES
    (1,  'DEV-001', 'temperature', 22.5,  'celsius', 'good', 1, '2025-01-15 08:00:00'),
    (2,  'DEV-002', 'temperature', 23.1,  'celsius', 'good', 1, '2025-01-15 08:01:00'),
    (3,  'DEV-003', 'temperature', 19.8,  'celsius', 'good', 1, '2025-01-15 08:02:00'),
    (4,  'DEV-004', 'temperature', 25.0,  'celsius', 'good', 1, '2025-01-15 08:03:00'),
    (5,  'DEV-005', 'temperature', 48.2,  'celsius', 'good', 1, '2025-01-15 08:04:00'),
    (6,  'DEV-006', 'temperature', 21.3,  'celsius', 'good', 1, '2025-01-15 08:05:00'),
    (7,  'DEV-007', 'temperature', 24.7,  'celsius', 'good', 1, '2025-01-15 08:06:00'),
    (8,  'DEV-008', 'temperature', 50.1,  'celsius', 'good', 1, '2025-01-15 08:07:00'),
    (9,  'DEV-009', 'temperature', 20.0,  'celsius', 'good', 1, '2025-01-15 08:08:00'),
    (10, 'DEV-010', 'temperature', 26.3,  'celsius', 'good', 1, '2025-01-15 08:09:00');
