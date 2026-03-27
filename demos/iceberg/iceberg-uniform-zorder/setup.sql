-- ============================================================================
-- Iceberg UniForm Z-ORDER Spatial Optimization — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table and seeds it with 36 geospatial
-- delivery tracking records across 6 US cities with realistic lat/lon
-- coordinates. Additional batches are inserted in queries.sql to create
-- file fragmentation, then OPTIMIZE ZORDER BY (latitude, longitude)
-- reorganizes the data for spatial locality.
--
-- Schema: delivery_id INT, driver_id VARCHAR, latitude DOUBLE,
--         longitude DOUBLE, delivery_status VARCHAR, package_weight DOUBLE,
--         delivery_fee DOUBLE, delivery_date VARCHAR, city VARCHAR
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create Delta table with UniForm enabled
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.delivery_tracking (
    delivery_id       INT,
    driver_id         VARCHAR,
    latitude          DOUBLE,
    longitude         DOUBLE,
    delivery_status   VARCHAR,
    package_weight    DOUBLE,
    delivery_fee      DOUBLE,
    delivery_date     VARCHAR,
    city              VARCHAR
) LOCATION '{{data_path}}/delivery_tracking'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.delivery_tracking TO USER {{current_user}};

-- STEP 3: Seed 36 deliveries across 6 cities (Batch 1)
INSERT INTO {{zone_name}}.iceberg_demos.delivery_tracking VALUES
    (1,  'DRV-101', 40.7128,  -74.0060,  'delivered',  2.5,  8.99,  '2025-02-01', 'New York'),
    (2,  'DRV-102', 40.7580,  -73.9855,  'delivered',  1.2,  6.50,  '2025-02-01', 'New York'),
    (3,  'DRV-103', 40.6892,  -74.0445,  'in_transit', 5.8,  12.99, '2025-02-01', 'New York'),
    (4,  'DRV-104', 40.7282,  -73.7949,  'delivered',  3.1,  9.50,  '2025-02-01', 'New York'),
    (5,  'DRV-105', 40.7484,  -73.9967,  'pending',    0.8,  5.99,  '2025-02-01', 'New York'),
    (6,  'DRV-106', 40.6501,  -73.9496,  'delivered',  4.2,  11.50, '2025-02-01', 'New York'),
    (7,  'DRV-201', 34.0522,  -118.2437, 'delivered',  3.0,  9.99,  '2025-02-01', 'Los Angeles'),
    (8,  'DRV-202', 34.0195,  -118.4912, 'in_transit', 7.5,  15.99, '2025-02-01', 'Los Angeles'),
    (9,  'DRV-203', 34.0689,  -118.4452, 'delivered',  2.2,  7.50,  '2025-02-01', 'Los Angeles'),
    (10, 'DRV-204', 33.9425,  -118.4081, 'delivered',  4.8,  13.50, '2025-02-02', 'Los Angeles'),
    (11, 'DRV-205', 34.1478,  -118.1445, 'pending',    1.5,  6.99,  '2025-02-02', 'Los Angeles'),
    (12, 'DRV-206', 34.0259,  -118.7798, 'delivered',  6.0,  14.50, '2025-02-02', 'Los Angeles'),
    (13, 'DRV-301', 41.8781,  -87.6298,  'delivered',  2.8,  8.50,  '2025-02-02', 'Chicago'),
    (14, 'DRV-302', 41.8827,  -87.6233,  'delivered',  1.0,  5.99,  '2025-02-02', 'Chicago'),
    (15, 'DRV-303', 41.9742,  -87.9073,  'in_transit', 5.2,  12.50, '2025-02-02', 'Chicago'),
    (16, 'DRV-304', 41.7508,  -87.6316,  'delivered',  3.5,  10.50, '2025-02-02', 'Chicago'),
    (17, 'DRV-305', 41.8500,  -87.6500,  'delivered',  2.0,  7.99,  '2025-02-03', 'Chicago'),
    (18, 'DRV-306', 41.9200,  -87.7000,  'pending',    4.0,  11.00, '2025-02-03', 'Chicago'),
    (19, 'DRV-401', 29.7604,  -95.3698,  'delivered',  3.3,  9.50,  '2025-02-03', 'Houston'),
    (20, 'DRV-402', 29.7866,  -95.3909,  'in_transit', 6.1,  14.99, '2025-02-03', 'Houston'),
    (21, 'DRV-403', 29.6516,  -95.2780,  'delivered',  2.0,  7.50,  '2025-02-03', 'Houston'),
    (22, 'DRV-404', 29.7355,  -95.3591,  'delivered',  4.5,  12.00, '2025-02-03', 'Houston'),
    (23, 'DRV-405', 29.8174,  -95.4018,  'pending',    1.8,  6.50,  '2025-02-03', 'Houston'),
    (24, 'DRV-406', 29.6997,  -95.3174,  'delivered',  5.0,  13.99, '2025-02-04', 'Houston'),
    (25, 'DRV-501', 33.4484,  -112.0740, 'delivered',  2.5,  8.50,  '2025-02-04', 'Phoenix'),
    (26, 'DRV-502', 33.5092,  -112.0480, 'delivered',  3.8,  10.99, '2025-02-04', 'Phoenix'),
    (27, 'DRV-503', 33.4152,  -111.8315, 'in_transit', 5.5,  13.50, '2025-02-04', 'Phoenix'),
    (28, 'DRV-504', 33.3062,  -111.8413, 'delivered',  1.9,  7.99,  '2025-02-04', 'Phoenix'),
    (29, 'DRV-505', 33.5722,  -112.0891, 'pending',    2.8,  8.99,  '2025-02-04', 'Phoenix'),
    (30, 'DRV-506', 33.3942,  -112.1401, 'delivered',  4.2,  11.50, '2025-02-05', 'Phoenix'),
    (31, 'DRV-601', 39.9526,  -75.1652,  'delivered',  2.3,  8.50,  '2025-02-05', 'Philadelphia'),
    (32, 'DRV-602', 39.9656,  -75.1810,  'delivered',  1.5,  6.99,  '2025-02-05', 'Philadelphia'),
    (33, 'DRV-603', 40.0379,  -75.1355,  'in_transit', 4.8,  12.50, '2025-02-05', 'Philadelphia'),
    (34, 'DRV-604', 39.9237,  -75.1719,  'delivered',  3.0,  9.50,  '2025-02-05', 'Philadelphia'),
    (35, 'DRV-605', 39.9816,  -75.1565,  'pending',    1.2,  5.99,  '2025-02-05', 'Philadelphia'),
    (36, 'DRV-606', 39.9400,  -75.2100,  'delivered',  5.5,  14.50, '2025-02-05', 'Philadelphia');
