-- ============================================================================
-- Delta Z-ORDER — Multi-Column Data Layout Optimization — Setup Script
-- ============================================================================
-- Prepares IoT sensor telemetry data with multiple batch inserts to create
-- file fragmentation. The OPTIMIZE ZORDER BY command is in queries.sql so
-- users can observe the before/after effects interactively.
--
-- Tables created:
--   1. sensor_telemetry — 80 sensor readings across 3 batches
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE
--   3. INSERT batch 1 — 30 temperature/humidity from us-east, eu-west
--   5. INSERT batch 2 — 25 pressure/wind from us-west, ap-south
--   6. INSERT batch 3 — 25 mixed sensors from all regions
--   7. UPDATE — flag 8 low-quality readings (quality_score < 50 → 0)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: sensor_telemetry — IoT sensor readings with multi-dimensional access
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sensor_telemetry (
    id              INT,
    device_id       VARCHAR,
    sensor_type     VARCHAR,
    reading         DOUBLE,
    unit            VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    region          VARCHAR,
    quality_score   INT,
    recorded_date   VARCHAR
) LOCATION 'sensor_telemetry';


-- ============================================================================
-- STEP 2: Batch 1 — 30 rows: temperature & humidity from us-east, eu-west
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.sensor_telemetry VALUES
    (1,  'DEV-001', 'temperature', 22.5,  'celsius', 40.7128, -74.0060, 'us-east', 95, '2025-03-01'),
    (2,  'DEV-002', 'temperature', 18.3,  'celsius', 42.3601, -71.0589, 'us-east', 88, '2025-03-01'),
    (3,  'DEV-003', 'temperature', 25.1,  'celsius', 33.7490, -84.3880, 'us-east', 92, '2025-03-02'),
    (4,  'DEV-004', 'temperature', 15.8,  'celsius', 51.5074, -0.1278,  'eu-west', 90, '2025-03-02'),
    (5,  'DEV-005', 'temperature', 19.6,  'celsius', 48.8566, 2.3522,   'eu-west', 85, '2025-03-03'),
    (6,  'DEV-006', 'temperature', 21.0,  'celsius', 52.5200, 13.4050,  'eu-west', 30, '2025-03-03'),
    (7,  'DEV-007', 'temperature', 23.4,  'celsius', 38.9072, -77.0369, 'us-east', 78, '2025-03-04'),
    (8,  'DEV-008', 'temperature', 17.2,  'celsius', 41.8781, -87.6298, 'us-east', 82, '2025-03-04'),
    (9,  'DEV-009', 'humidity',    65.0,  'percent', 40.7128, -74.0060, 'us-east', 91, '2025-03-01'),
    (10, 'DEV-010', 'humidity',    72.3,  'percent', 42.3601, -71.0589, 'us-east', 87, '2025-03-01'),
    (11, 'DEV-011', 'humidity',    58.5,  'percent', 33.7490, -84.3880, 'us-east', 25, '2025-03-02'),
    (12, 'DEV-012', 'humidity',    80.1,  'percent', 51.5074, -0.1278,  'eu-west', 93, '2025-03-02'),
    (13, 'DEV-013', 'humidity',    55.7,  'percent', 48.8566, 2.3522,   'eu-west', 89, '2025-03-03'),
    (14, 'DEV-014', 'humidity',    68.9,  'percent', 52.5200, 13.4050,  'eu-west', 76, '2025-03-03'),
    (15, 'DEV-015', 'humidity',    61.2,  'percent', 38.9072, -77.0369, 'us-east', 84, '2025-03-04'),
    (16, 'DEV-016', 'temperature', 20.5,  'celsius', 40.4168, -3.7038,  'eu-west', 72, '2025-03-05'),
    (17, 'DEV-017', 'temperature', 24.8,  'celsius', 39.9042, -75.1698, 'us-east', 96, '2025-03-05'),
    (18, 'DEV-018', 'humidity',    70.6,  'percent', 41.8781, -87.6298, 'us-east', 80, '2025-03-04'),
    (19, 'DEV-019', 'humidity',    63.4,  'percent', 40.4168, -3.7038,  'eu-west', 35, '2025-03-05'),
    (20, 'DEV-020', 'humidity',    77.8,  'percent', 39.9042, -75.1698, 'us-east', 94, '2025-03-05'),
    (21, 'DEV-021', 'temperature', 16.9,  'celsius', 53.3498, -6.2603,  'eu-west', 86, '2025-03-06'),
    (22, 'DEV-022', 'temperature', 26.3,  'celsius', 25.7617, -80.1918, 'us-east', 79, '2025-03-06'),
    (23, 'DEV-023', 'humidity',    59.1,  'percent', 53.3498, -6.2603,  'eu-west', 81, '2025-03-06'),
    (24, 'DEV-024', 'humidity',    74.5,  'percent', 25.7617, -80.1918, 'us-east', 90, '2025-03-06'),
    (25, 'DEV-025', 'temperature', 13.7,  'celsius', 50.1109, 8.6821,   'eu-west', 20, '2025-03-07'),
    (26, 'DEV-026', 'temperature', 28.0,  'celsius', 29.7604, -95.3698, 'us-east', 97, '2025-03-07'),
    (27, 'DEV-027', 'humidity',    66.2,  'percent', 50.1109, 8.6821,   'eu-west', 83, '2025-03-07'),
    (28, 'DEV-028', 'humidity',    71.0,  'percent', 29.7604, -95.3698, 'us-east', 88, '2025-03-07'),
    (29, 'DEV-029', 'temperature', 19.0,  'celsius', 45.4642, 9.1900,   'eu-west', 75, '2025-03-08'),
    (30, 'DEV-030', 'humidity',    62.8,  'percent', 45.4642, 9.1900,   'eu-west', 10, '2025-03-08');


-- ============================================================================
-- STEP 3: Batch 2 — 25 rows: pressure & wind from us-west, ap-south
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.sensor_telemetry
SELECT * FROM (VALUES
    (31, 'DEV-031', 'pressure',  1013.2, 'hPa',  37.7749, -122.4194, 'us-west',  92, '2025-03-01'),
    (32, 'DEV-032', 'pressure',  1010.5, 'hPa',  34.0522, -118.2437, 'us-west',  88, '2025-03-01'),
    (33, 'DEV-033', 'pressure',  1008.8, 'hPa',  47.6062, -122.3321, 'us-west',  85, '2025-03-02'),
    (34, 'DEV-034', 'pressure',  1015.1, 'hPa',  19.0760, 72.8777,   'ap-south', 90, '2025-03-02'),
    (35, 'DEV-035', 'pressure',  1011.7, 'hPa',  28.6139, 77.2090,   'ap-south', 71, '2025-03-03'),
    (36, 'DEV-036', 'pressure',  1009.3, 'hPa',  13.0827, 80.2707,   'ap-south', 87, '2025-03-03'),
    (37, 'DEV-037', 'wind',      12.5,   'km/h', 37.7749, -122.4194, 'us-west',  93, '2025-03-01'),
    (38, 'DEV-038', 'wind',      8.3,    'km/h', 34.0522, -118.2437, 'us-west',  86, '2025-03-01'),
    (39, 'DEV-039', 'wind',      15.7,   'km/h', 47.6062, -122.3321, 'us-west',  45, '2025-03-02'),
    (40, 'DEV-040', 'wind',      22.1,   'km/h', 19.0760, 72.8777,   'ap-south', 91, '2025-03-02'),
    (41, 'DEV-041', 'wind',      18.4,   'km/h', 28.6139, 77.2090,   'ap-south', 80, '2025-03-03'),
    (42, 'DEV-042', 'wind',      10.9,   'km/h', 13.0827, 80.2707,   'ap-south', 77, '2025-03-03'),
    (43, 'DEV-043', 'pressure',  1014.6, 'hPa',  45.5152, -122.6784, 'us-west',  94, '2025-03-04'),
    (44, 'DEV-044', 'pressure',  1007.2, 'hPa',  1.3521,  103.8198,  'ap-south', 82, '2025-03-04'),
    (45, 'DEV-045', 'wind',      14.0,   'km/h', 45.5152, -122.6784, 'us-west',  89, '2025-03-04'),
    (46, 'DEV-046', 'wind',      25.6,   'km/h', 1.3521,  103.8198,  'ap-south', 95, '2025-03-04'),
    (47, 'DEV-047', 'pressure',  1012.0, 'hPa',  32.7157, -117.1611, 'us-west',  81, '2025-03-05'),
    (48, 'DEV-048', 'pressure',  1016.4, 'hPa',  22.5726, 88.3639,   'ap-south', 76, '2025-03-05'),
    (49, 'DEV-049', 'wind',      9.8,    'km/h', 32.7157, -117.1611, 'us-west',  84, '2025-03-05'),
    (50, 'DEV-050', 'wind',      20.3,   'km/h', 22.5726, 88.3639,   'ap-south', 79, '2025-03-05'),
    (51, 'DEV-051', 'pressure',  1010.9, 'hPa',  36.1699, -115.1398, 'us-west',  90, '2025-03-06'),
    (52, 'DEV-052', 'pressure',  1013.8, 'hPa',  12.9716, 77.5946,   'ap-south', 15, '2025-03-06'),
    (53, 'DEV-053', 'wind',      16.2,   'km/h', 36.1699, -115.1398, 'us-west',  87, '2025-03-06'),
    (54, 'DEV-054', 'wind',      11.5,   'km/h', 12.9716, 77.5946,   'ap-south', 73, '2025-03-06'),
    (55, 'DEV-055', 'pressure',  1011.3, 'hPa',  33.4484, -112.0740, 'us-west',  91, '2025-03-07')
) AS t(id, device_id, sensor_type, reading, unit, latitude, longitude, region, quality_score, recorded_date);


-- ============================================================================
-- STEP 4: Batch 3 — 25 rows: mixed sensor types from all regions
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.sensor_telemetry
SELECT * FROM (VALUES
    (56, 'DEV-056', 'temperature', 27.2,   'celsius', 40.7128, -74.0060, 'us-east',  98, '2025-03-08'),
    (57, 'DEV-057', 'temperature', 14.5,   'celsius', 51.5074, -0.1278,  'eu-west',  83, '2025-03-08'),
    (58, 'DEV-058', 'temperature', 30.1,   'celsius', 37.7749, -122.4194,'us-west',  91, '2025-03-07'),
    (59, 'DEV-059', 'temperature', 33.5,   'celsius', 19.0760, 72.8777,  'ap-south', 89, '2025-03-07'),
    (60, 'DEV-060', 'humidity',    75.0,    'percent', 42.3601, -71.0589, 'us-east',  86, '2025-03-08'),
    (61, 'DEV-061', 'humidity',    82.3,    'percent', 48.8566, 2.3522,   'eu-west',  92, '2025-03-08'),
    (62, 'DEV-062', 'humidity',    53.6,    'percent', 34.0522, -118.2437,'us-west',  78, '2025-03-07'),
    (63, 'DEV-063', 'humidity',    88.9,    'percent', 28.6139, 77.2090,  'ap-south', 85, '2025-03-07'),
    (64, 'DEV-064', 'pressure',   1012.5,  'hPa',     33.7490, -84.3880, 'us-east',  93, '2025-03-08'),
    (65, 'DEV-065', 'pressure',   1009.7,  'hPa',     52.5200, 13.4050,  'eu-west',  80, '2025-03-08'),
    (66, 'DEV-066', 'pressure',   1014.0,  'hPa',     47.6062, -122.3321,'us-west',  96, '2025-03-07'),
    (67, 'DEV-067', 'pressure',   1017.3,  'hPa',     13.0827, 80.2707,  'ap-south', 88, '2025-03-07'),
    (68, 'DEV-068', 'wind',       19.5,    'km/h',    38.9072, -77.0369, 'us-east',  90, '2025-03-08'),
    (69, 'DEV-069', 'wind',       7.8,     'km/h',    53.3498, -6.2603,  'eu-west',  74, '2025-03-08'),
    (70, 'DEV-070', 'wind',       23.4,    'km/h',    45.5152, -122.6784,'us-west',  82, '2025-03-07'),
    (71, 'DEV-071', 'wind',       30.2,    'km/h',    1.3521,  103.8198, 'ap-south', 97, '2025-03-07'),
    (72, 'DEV-072', 'temperature', 21.7,   'celsius', 41.8781, -87.6298, 'us-east',  85, '2025-03-09'),
    (73, 'DEV-073', 'humidity',    67.4,    'percent', 50.1109, 8.6821,   'eu-west',  81, '2025-03-09'),
    (74, 'DEV-074', 'pressure',   1015.8,  'hPa',     32.7157, -117.1611,'us-west',  94, '2025-03-09'),
    (75, 'DEV-075', 'wind',       13.6,    'km/h',    22.5726, 88.3639,  'ap-south', 70, '2025-03-09'),
    (76, 'DEV-076', 'temperature', 29.3,   'celsius', 25.7617, -80.1918, 'us-east',  77, '2025-03-09'),
    (77, 'DEV-077', 'humidity',    60.5,    'percent', 40.4168, -3.7038,  'eu-west',  83, '2025-03-09'),
    (78, 'DEV-078', 'pressure',   1018.1,  'hPa',     36.1699, -115.1398,'us-west',  99, '2025-03-09'),
    (79, 'DEV-079', 'wind',       26.8,    'km/h',    12.9716, 77.5946,  'ap-south', 92, '2025-03-09'),
    (80, 'DEV-080', 'temperature', 11.9,   'celsius', 29.7604, -95.3698, 'us-east',  38, '2025-03-10')
) AS t(id, device_id, sensor_type, reading, unit, latitude, longitude, region, quality_score, recorded_date);


-- ============================================================================
-- STEP 5: UPDATE — flag low-quality readings for recalibration
-- ============================================================================
-- Readings with quality_score < 50 indicate sensor malfunction.
-- Set quality_score = 0 to flag them for recalibration.
-- Affected rows (quality_score < 50):
--   id=6(30), id=11(25), id=19(35), id=25(20),
--   id=30(10), id=39(45), id=52(15), id=80(38) = 8 rows
UPDATE {{zone_name}}.delta_demos.sensor_telemetry
SET quality_score = 0
WHERE quality_score < 50;
