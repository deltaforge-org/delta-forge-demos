-- ============================================================================
-- Delta Flexible Ingestion — JSON to Typed Columns — Setup Script
-- ============================================================================
-- Demonstrates the "schema promotion" pattern: start with JSON metadata,
-- then promote frequently-queried fields to typed columns.
--
-- Tables created:
--   1. sensor_telemetry — 20 IoT sensor readings with JSON metadata
--
-- Operations performed:
--   1. CREATE DELTA TABLE with 7 columns (including JSON metadata)
--   2. INSERT — 10 readings (08:00–09:00 batch B001)
--   3. INSERT — 10 readings (10:00–11:00 batch B002)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: sensor_telemetry — 20 readings from 5 factory sensors
-- ============================================================================
-- Sensors: TEMP-01, TEMP-02 (temperature), PRESS-01, PRESS-02 (pressure),
--          HUM-01 (humidity)
-- The metadata column stores a JSON string with location, firmware, batch,
-- and optional alert flag — fields that vary between sensor types.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sensor_telemetry (
    id             INT,
    sensor_id      VARCHAR,
    reading_type   VARCHAR,
    value          DOUBLE,
    unit           VARCHAR,
    metadata       VARCHAR,
    recorded_at    VARCHAR
) LOCATION '{{data_path}}/sensor_telemetry';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.sensor_telemetry TO USER {{current_user}};

-- STEP 2: Insert batch B001 — 08:00 and 09:00 readings (10 rows)
INSERT INTO {{zone_name}}.delta_demos.sensor_telemetry VALUES
    (1,  'TEMP-01',  'temperature', 24.4, 'celsius', '{"location":"floor_1_east","firmware":"v2.1","batch":"B001"}',                '2024-09-10 08:00:00'),
    (2,  'TEMP-02',  'temperature', 18.3, 'celsius', '{"location":"floor_1_west","firmware":"v2.1","batch":"B001"}',                '2024-09-10 08:00:00'),
    (3,  'PRESS-01', 'pressure',    35.5, 'psi',     '{"location":"boiler_room","firmware":"v3.0","batch":"B001"}',                 '2024-09-10 08:00:00'),
    (4,  'PRESS-02', 'pressure',    34.5, 'psi',     '{"location":"compressor","firmware":"v3.0","batch":"B001"}',                  '2024-09-10 08:00:00'),
    (5,  'HUM-01',   'humidity',    62.1, 'percent', '{"location":"clean_room","firmware":"v1.8","batch":"B001"}',                  '2024-09-10 08:00:00'),
    (6,  'TEMP-01',  'temperature', 24.8, 'celsius', '{"location":"floor_1_east","firmware":"v2.1","batch":"B001"}',                '2024-09-10 09:00:00'),
    (7,  'TEMP-02',  'temperature', 26.9, 'celsius', '{"location":"floor_1_west","firmware":"v2.1","batch":"B001","alert":true}',   '2024-09-10 09:00:00'),
    (8,  'PRESS-01', 'pressure',    31.7, 'psi',     '{"location":"boiler_room","firmware":"v3.0","batch":"B001"}',                 '2024-09-10 09:00:00'),
    (9,  'PRESS-02', 'pressure',    38.4, 'psi',     '{"location":"compressor","firmware":"v3.0","batch":"B001"}',                  '2024-09-10 09:00:00'),
    (10, 'HUM-01',   'humidity',    40.9, 'percent', '{"location":"clean_room","firmware":"v1.8","batch":"B001"}',                  '2024-09-10 09:00:00');


-- ============================================================================
-- STEP 3: Insert batch B002 — 10:00 and 11:00 readings (10 rows)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.sensor_telemetry
SELECT * FROM (VALUES
    (11, 'TEMP-01',  'temperature', 20.2, 'celsius', '{"location":"floor_1_east","firmware":"v2.1","batch":"B002"}',                '2024-09-10 10:00:00'),
    (12, 'TEMP-02',  'temperature', 23.1, 'celsius', '{"location":"floor_1_west","firmware":"v2.1","batch":"B002"}',                '2024-09-10 10:00:00'),
    (13, 'PRESS-01', 'pressure',    30.5, 'psi',     '{"location":"boiler_room","firmware":"v3.0","batch":"B002"}',                 '2024-09-10 10:00:00'),
    (14, 'PRESS-02', 'pressure',    34.0, 'psi',     '{"location":"compressor","firmware":"v3.0","batch":"B002"}',                  '2024-09-10 10:00:00'),
    (15, 'HUM-01',   'humidity',    59.5, 'percent', '{"location":"clean_room","firmware":"v1.8","batch":"B002"}',                  '2024-09-10 10:00:00'),
    (16, 'TEMP-01',  'temperature', 23.4, 'celsius', '{"location":"floor_1_east","firmware":"v2.1","batch":"B002"}',                '2024-09-10 11:00:00'),
    (17, 'TEMP-02',  'temperature', 20.2, 'celsius', '{"location":"floor_1_west","firmware":"v2.1","batch":"B002"}',                '2024-09-10 11:00:00'),
    (18, 'PRESS-01', 'pressure',    41.8, 'psi',     '{"location":"boiler_room","firmware":"v3.0","batch":"B002"}',                 '2024-09-10 11:00:00'),
    (19, 'PRESS-02', 'pressure',    46.2, 'psi',     '{"location":"compressor","firmware":"v3.0","batch":"B002","alert":true}',     '2024-09-10 11:00:00'),
    (20, 'HUM-01',   'humidity',    40.2, 'percent', '{"location":"clean_room","firmware":"v1.8","batch":"B002"}',                  '2024-09-10 11:00:00')
) AS t(id, sensor_id, reading_type, value, unit, metadata, recorded_at);
