-- ============================================================================
-- Iceberg V3 UniForm — Supply Chain Inventory MERGE Sync — Setup
-- ============================================================================
-- Creates a Delta table with UniForm V3 tracking warehouse inventory.
-- Seeds 30 SKUs (10 per warehouse) across 3 categories. MERGE operations
-- happen in queries.sql to simulate inventory sync from supplier feeds.
--
-- Dataset: 30 rows, 3 warehouses (WH-EAST, WH-WEST, WH-CENTRAL),
-- 10 SKUs per warehouse, 3 categories (Electronics, Office, Accessories).
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm V3
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.warehouse_inventory (
    item_id        INT,
    warehouse      VARCHAR,
    sku            VARCHAR,
    category       VARCHAR,
    product_name   VARCHAR,
    quantity       INT,
    unit_price     DOUBLE,
    last_received  VARCHAR
) LOCATION '{{data_path}}/warehouse_inventory'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '3',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.warehouse_inventory TO USER {{current_user}};

-- STEP 3: Seed 30 inventory records (10 per warehouse)
INSERT INTO {{zone_name}}.iceberg_demos.warehouse_inventory VALUES
    (1,  'WH-EAST',    'SKU-1001', 'Electronics', 'Wireless Mouse',        150, 24.99,  '2024-01-05'),
    (2,  'WH-EAST',    'SKU-1002', 'Electronics', 'USB-C Hub',             80,  39.99,  '2024-01-05'),
    (3,  'WH-EAST',    'SKU-1003', 'Office',      'Ergonomic Chair',       25,  299.99, '2024-01-10'),
    (4,  'WH-EAST',    'SKU-1004', 'Office',      'Standing Desk',         15,  549.99, '2024-01-10'),
    (5,  'WH-EAST',    'SKU-1005', 'Accessories', 'Monitor Stand',         60,  79.99,  '2024-01-12'),
    (6,  'WH-EAST',    'SKU-1006', 'Accessories', 'Laptop Sleeve 15in',    200, 29.99,  '2024-01-12'),
    (7,  'WH-EAST',    'SKU-1007', 'Electronics', 'Mechanical Keyboard',   90,  89.99,  '2024-01-15'),
    (8,  'WH-EAST',    'SKU-1008', 'Office',      'Whiteboard 4x6',        12,  149.99, '2024-01-15'),
    (9,  'WH-EAST',    'SKU-1009', 'Accessories', 'Webcam HD',             110, 59.99,  '2024-01-18'),
    (10, 'WH-EAST',    'SKU-1010', 'Electronics', 'Docking Station',       45,  179.99, '2024-01-18'),
    (11, 'WH-WEST',    'SKU-1001', 'Electronics', 'Wireless Mouse',        180, 24.99,  '2024-01-06'),
    (12, 'WH-WEST',    'SKU-1002', 'Electronics', 'USB-C Hub',             100, 39.99,  '2024-01-06'),
    (13, 'WH-WEST',    'SKU-1003', 'Office',      'Ergonomic Chair',       30,  299.99, '2024-01-11'),
    (14, 'WH-WEST',    'SKU-1004', 'Office',      'Standing Desk',         20,  549.99, '2024-01-11'),
    (15, 'WH-WEST',    'SKU-1005', 'Accessories', 'Monitor Stand',         75,  79.99,  '2024-01-13'),
    (16, 'WH-WEST',    'SKU-1006', 'Accessories', 'Laptop Sleeve 15in',    250, 29.99,  '2024-01-13'),
    (17, 'WH-WEST',    'SKU-1007', 'Electronics', 'Mechanical Keyboard',   120, 89.99,  '2024-01-16'),
    (18, 'WH-WEST',    'SKU-1008', 'Office',      'Whiteboard 4x6',        18,  149.99, '2024-01-16'),
    (19, 'WH-WEST',    'SKU-1009', 'Accessories', 'Webcam HD',             130, 59.99,  '2024-01-19'),
    (20, 'WH-WEST',    'SKU-1010', 'Electronics', 'Docking Station',       55,  179.99, '2024-01-19'),
    (21, 'WH-CENTRAL', 'SKU-1001', 'Electronics', 'Wireless Mouse',        100, 24.99,  '2024-01-07'),
    (22, 'WH-CENTRAL', 'SKU-1002', 'Electronics', 'USB-C Hub',             65,  39.99,  '2024-01-07'),
    (23, 'WH-CENTRAL', 'SKU-1003', 'Office',      'Ergonomic Chair',       20,  299.99, '2024-01-12'),
    (24, 'WH-CENTRAL', 'SKU-1004', 'Office',      'Standing Desk',         10,  549.99, '2024-01-12'),
    (25, 'WH-CENTRAL', 'SKU-1005', 'Accessories', 'Monitor Stand',         40,  79.99,  '2024-01-14'),
    (26, 'WH-CENTRAL', 'SKU-1006', 'Accessories', 'Laptop Sleeve 15in',    175, 29.99,  '2024-01-14'),
    (27, 'WH-CENTRAL', 'SKU-1007', 'Electronics', 'Mechanical Keyboard',   70,  89.99,  '2024-01-17'),
    (28, 'WH-CENTRAL', 'SKU-1008', 'Office',      'Whiteboard 4x6',        8,   149.99, '2024-01-17'),
    (29, 'WH-CENTRAL', 'SKU-1009', 'Accessories', 'Webcam HD',             85,  59.99,  '2024-01-20'),
    (30, 'WH-CENTRAL', 'SKU-1010', 'Electronics', 'Docking Station',       35,  179.99, '2024-01-20');
