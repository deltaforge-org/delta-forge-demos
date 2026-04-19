-- ============================================================================
-- Iceberg UniForm Format Versions — Setup
-- ============================================================================
-- Creates three Delta tables with identical data but different Iceberg
-- format versions: V1, V2, and V3. Each table produces Iceberg metadata
-- at its configured version, demonstrating format differences.
--
-- Dataset: 12 sensor readings per table across 3 locations (Lab-A, Lab-B,
-- Lab-C) with columns: id, sensor_id, location, temperature, humidity,
-- reading_time, status.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2a: V1 table — Iceberg format version 1
-- V1: Single schema, no sequence numbers, basic partition specs.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sensors_v1 (
    id           INT,
    sensor_id    VARCHAR,
    location     VARCHAR,
    temperature  DOUBLE,
    humidity     DOUBLE,
    reading_time VARCHAR,
    status       VARCHAR
) LOCATION '{{data_subdir}}/sensors_v1'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '1',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sensors_v1 TO USER {{current_user}};

-- STEP 2b: V2 table — Iceberg format version 2 (default)
-- V2: Schema evolution array, sequence numbers, row-level deletes.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sensors_v2 (
    id           INT,
    sensor_id    VARCHAR,
    location     VARCHAR,
    temperature  DOUBLE,
    humidity     DOUBLE,
    reading_time VARCHAR,
    status       VARCHAR
) LOCATION '{{data_subdir}}/sensors_v2'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sensors_v2 TO USER {{current_user}};

-- STEP 2c: V3 table — Iceberg format version 3
-- V3: Nanosecond timestamps, deletion vectors, named references.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sensors_v3 (
    id           INT,
    sensor_id    VARCHAR,
    location     VARCHAR,
    temperature  DOUBLE,
    humidity     DOUBLE,
    reading_time VARCHAR,
    status       VARCHAR
) LOCATION '{{data_subdir}}/sensors_v3'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '3',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sensors_v3 TO USER {{current_user}};

-- STEP 3: Seed identical data into all three tables
INSERT INTO {{zone_name}}.iceberg_demos.sensors_v1 VALUES
    (1,  'SENS-001', 'Lab-A', 22.5,  45.0, '2024-06-01 08:00:00', 'normal'),
    (2,  'SENS-002', 'Lab-A', 23.1,  43.5, '2024-06-01 08:15:00', 'normal'),
    (3,  'SENS-003', 'Lab-A', 24.8,  41.0, '2024-06-01 08:30:00', 'warning'),
    (4,  'SENS-004', 'Lab-A', 21.9,  46.2, '2024-06-01 08:45:00', 'normal'),
    (5,  'SENS-001', 'Lab-B', 19.4,  55.0, '2024-06-01 08:00:00', 'normal'),
    (6,  'SENS-002', 'Lab-B', 20.1,  53.8, '2024-06-01 08:15:00', 'normal'),
    (7,  'SENS-003', 'Lab-B', 18.7,  58.2, '2024-06-01 08:30:00', 'normal'),
    (8,  'SENS-004', 'Lab-B', 26.3,  40.1, '2024-06-01 08:45:00', 'critical'),
    (9,  'SENS-001', 'Lab-C', 21.0,  50.0, '2024-06-01 08:00:00', 'normal'),
    (10, 'SENS-002', 'Lab-C', 22.4,  48.5, '2024-06-01 08:15:00', 'normal'),
    (11, 'SENS-003', 'Lab-C', 25.5,  39.0, '2024-06-01 08:30:00', 'warning'),
    (12, 'SENS-004', 'Lab-C', 20.8,  51.2, '2024-06-01 08:45:00', 'normal');

INSERT INTO {{zone_name}}.iceberg_demos.sensors_v2 VALUES
    (1,  'SENS-001', 'Lab-A', 22.5,  45.0, '2024-06-01 08:00:00', 'normal'),
    (2,  'SENS-002', 'Lab-A', 23.1,  43.5, '2024-06-01 08:15:00', 'normal'),
    (3,  'SENS-003', 'Lab-A', 24.8,  41.0, '2024-06-01 08:30:00', 'warning'),
    (4,  'SENS-004', 'Lab-A', 21.9,  46.2, '2024-06-01 08:45:00', 'normal'),
    (5,  'SENS-001', 'Lab-B', 19.4,  55.0, '2024-06-01 08:00:00', 'normal'),
    (6,  'SENS-002', 'Lab-B', 20.1,  53.8, '2024-06-01 08:15:00', 'normal'),
    (7,  'SENS-003', 'Lab-B', 18.7,  58.2, '2024-06-01 08:30:00', 'normal'),
    (8,  'SENS-004', 'Lab-B', 26.3,  40.1, '2024-06-01 08:45:00', 'critical'),
    (9,  'SENS-001', 'Lab-C', 21.0,  50.0, '2024-06-01 08:00:00', 'normal'),
    (10, 'SENS-002', 'Lab-C', 22.4,  48.5, '2024-06-01 08:15:00', 'normal'),
    (11, 'SENS-003', 'Lab-C', 25.5,  39.0, '2024-06-01 08:30:00', 'warning'),
    (12, 'SENS-004', 'Lab-C', 20.8,  51.2, '2024-06-01 08:45:00', 'normal');

INSERT INTO {{zone_name}}.iceberg_demos.sensors_v3 VALUES
    (1,  'SENS-001', 'Lab-A', 22.5,  45.0, '2024-06-01 08:00:00', 'normal'),
    (2,  'SENS-002', 'Lab-A', 23.1,  43.5, '2024-06-01 08:15:00', 'normal'),
    (3,  'SENS-003', 'Lab-A', 24.8,  41.0, '2024-06-01 08:30:00', 'warning'),
    (4,  'SENS-004', 'Lab-A', 21.9,  46.2, '2024-06-01 08:45:00', 'normal'),
    (5,  'SENS-001', 'Lab-B', 19.4,  55.0, '2024-06-01 08:00:00', 'normal'),
    (6,  'SENS-002', 'Lab-B', 20.1,  53.8, '2024-06-01 08:15:00', 'normal'),
    (7,  'SENS-003', 'Lab-B', 18.7,  58.2, '2024-06-01 08:30:00', 'normal'),
    (8,  'SENS-004', 'Lab-B', 26.3,  40.1, '2024-06-01 08:45:00', 'critical'),
    (9,  'SENS-001', 'Lab-C', 21.0,  50.0, '2024-06-01 08:00:00', 'normal'),
    (10, 'SENS-002', 'Lab-C', 22.4,  48.5, '2024-06-01 08:15:00', 'normal'),
    (11, 'SENS-003', 'Lab-C', 25.5,  39.0, '2024-06-01 08:30:00', 'warning'),
    (12, 'SENS-004', 'Lab-C', 20.8,  51.2, '2024-06-01 08:45:00', 'normal');
