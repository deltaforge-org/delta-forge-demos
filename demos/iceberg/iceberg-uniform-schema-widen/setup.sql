-- ============================================================================
-- Iceberg UniForm Type Widening — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table and seeds 24 IoT sensor readings
-- across 4 locations (rooftop, basement, warehouse, cleanroom), 6 per
-- location. Type widening (INT→BIGINT, FLOAT→DOUBLE) happens in queries.sql
-- to demonstrate how both Delta and Iceberg metadata track type changes.
--
-- Dataset: 24 sensors with columns:
-- sensor_id, location, reading_count, temperature, humidity, battery_pct,
-- reading_date.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm and column mapping
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sensor_readings (
    sensor_id      VARCHAR,
    location       VARCHAR,
    reading_count  INT,
    temperature    FLOAT,
    humidity       FLOAT,
    battery_pct    INT,
    reading_date   VARCHAR
) LOCATION '{{data_path}}/sensor_readings'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id',
    'delta.enableTypeWidening' = 'true'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sensor_readings TO USER {{current_user}};

-- STEP 3: Seed 24 sensors (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.sensor_readings VALUES
    ('S001', 'rooftop',    15000, 32.5, 45.2, 88, '2025-01-15'),
    ('S002', 'rooftop',    22000, 34.1, 42.8, 76, '2025-01-15'),
    ('S003', 'rooftop',    18500, 30.8, 48.5, 92, '2025-01-16'),
    ('S004', 'rooftop',    27000, 36.2, 40.1, 65, '2025-01-16'),
    ('S005', 'rooftop',    12000, 31.0, 46.7, 95, '2025-01-17'),
    ('S006', 'rooftop',    19500, 33.7, 44.0, 81, '2025-01-17'),
    ('S007', 'basement',   31000, 18.3, 72.1, 90, '2025-01-15'),
    ('S008', 'basement',   25000, 19.0, 70.5, 84, '2025-01-15'),
    ('S009', 'basement',   28000, 17.5, 74.8, 87, '2025-01-16'),
    ('S010', 'basement',   35000, 18.8, 71.2, 78, '2025-01-16'),
    ('S011', 'basement',   20000, 19.5, 69.0, 93, '2025-01-17'),
    ('S012', 'basement',   33000, 18.1, 73.5, 82, '2025-01-17'),
    ('S013', 'warehouse',  42000, 22.4, 55.3, 70, '2025-01-15'),
    ('S014', 'warehouse',  38000, 23.1, 53.8, 68, '2025-01-15'),
    ('S015', 'warehouse',  45000, 21.7, 57.2, 74, '2025-01-16'),
    ('S016', 'warehouse',  50000, 24.0, 52.0, 62, '2025-01-16'),
    ('S017', 'warehouse',  36000, 22.8, 56.1, 77, '2025-01-17'),
    ('S018', 'warehouse',  41000, 23.5, 54.5, 71, '2025-01-17'),
    ('S019', 'cleanroom',  55000, 21.0, 50.0, 85, '2025-01-15'),
    ('S020', 'cleanroom',  60000, 21.2, 49.8, 80, '2025-01-15'),
    ('S021', 'cleanroom',  48000, 20.8, 50.5, 89, '2025-01-16'),
    ('S022', 'cleanroom',  52000, 21.5, 49.2, 83, '2025-01-16'),
    ('S023', 'cleanroom',  58000, 20.5, 51.0, 91, '2025-01-17'),
    ('S024', 'cleanroom',  63000, 21.8, 48.5, 79, '2025-01-17');
