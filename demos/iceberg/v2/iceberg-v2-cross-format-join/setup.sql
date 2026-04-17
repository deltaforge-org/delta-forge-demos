-- ============================================================================
-- Iceberg Cross-Format Join — Retail Store Analytics — Setup
-- ============================================================================
-- Creates two tables backed by different formats:
--   1. Delta table with UniForm V2 (sales transactions — 40 rows)
--   2. CSV external table (store locations — 10 rows)
-- The queries.sql demonstrates JOINs between these formats, then registers
-- an Iceberg external table over the Delta data to prove cross-format
-- interop through 3 different access paths.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Retail analytics cross-format demo';

-- STEP 2: CSV external table — store locations
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.stores
USING CSV
LOCATION '{{data_path}}/stores.csv'
OPTIONS (
    header = 'true'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.stores TO USER {{current_user}};

-- STEP 3: Delta table with UniForm V2 — sales transactions
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sales (
    txn_id        INT,
    store_id      VARCHAR,
    product_id    VARCHAR,
    category      VARCHAR,
    product_name  VARCHAR,
    quantity      INT,
    unit_price    DOUBLE,
    sale_date     VARCHAR
) LOCATION '{{data_path}}/sales'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sales TO USER {{current_user}};

-- STEP 4: Seed 40 sales transactions (4 per store)
INSERT INTO {{zone_name}}.iceberg_demos.sales VALUES
    (1,  'S001', 'P100', 'Shoes',       'Running Pro X',   3,  129.99, '2024-01-15'),
    (2,  'S001', 'P101', 'Apparel',     'Winter Jacket',   2,  199.99, '2024-01-15'),
    (3,  'S001', 'P102', 'Accessories', 'Sport Watch',     1,  349.99, '2024-01-16'),
    (4,  'S001', 'P103', 'Shoes',       'Trail Runner',    4,  109.99, '2024-01-17'),
    (5,  'S002', 'P100', 'Shoes',       'Running Pro X',   2,  129.99, '2024-01-15'),
    (6,  'S002', 'P104', 'Apparel',     'Fleece Vest',     5,  79.99,  '2024-01-16'),
    (7,  'S002', 'P105', 'Accessories', 'Gym Bag',         3,  59.99,  '2024-01-17'),
    (8,  'S002', 'P101', 'Apparel',     'Winter Jacket',   1,  199.99, '2024-01-18'),
    (9,  'S003', 'P100', 'Shoes',       'Running Pro X',   6,  129.99, '2024-01-15'),
    (10, 'S003', 'P106', 'Shoes',       'Casual Slip-On',  4,  69.99,  '2024-01-16'),
    (11, 'S003', 'P102', 'Accessories', 'Sport Watch',     2,  349.99, '2024-01-17'),
    (12, 'S003', 'P104', 'Apparel',     'Fleece Vest',     3,  79.99,  '2024-01-18'),
    (13, 'S004', 'P103', 'Shoes',       'Trail Runner',    5,  109.99, '2024-01-15'),
    (14, 'S004', 'P101', 'Apparel',     'Winter Jacket',   3,  199.99, '2024-01-16'),
    (15, 'S004', 'P107', 'Accessories', 'Sunglasses',      8,  89.99,  '2024-01-17'),
    (16, 'S004', 'P100', 'Shoes',       'Running Pro X',   2,  129.99, '2024-01-18'),
    (17, 'S005', 'P102', 'Accessories', 'Sport Watch',     1,  349.99, '2024-01-15'),
    (18, 'S005', 'P106', 'Shoes',       'Casual Slip-On',  3,  69.99,  '2024-01-16'),
    (19, 'S005', 'P104', 'Apparel',     'Fleece Vest',     4,  79.99,  '2024-01-17'),
    (20, 'S005', 'P105', 'Accessories', 'Gym Bag',         2,  59.99,  '2024-01-18'),
    (21, 'S006', 'P100', 'Shoes',       'Running Pro X',   3,  129.99, '2024-01-15'),
    (22, 'S006', 'P107', 'Accessories', 'Sunglasses',      6,  89.99,  '2024-01-16'),
    (23, 'S006', 'P101', 'Apparel',     'Winter Jacket',   2,  199.99, '2024-01-17'),
    (24, 'S006', 'P103', 'Shoes',       'Trail Runner',    3,  109.99, '2024-01-18'),
    (25, 'S007', 'P102', 'Accessories', 'Sport Watch',     2,  349.99, '2024-01-15'),
    (26, 'S007', 'P100', 'Shoes',       'Running Pro X',   4,  129.99, '2024-01-16'),
    (27, 'S007', 'P104', 'Apparel',     'Fleece Vest',     6,  79.99,  '2024-01-17'),
    (28, 'S007', 'P106', 'Shoes',       'Casual Slip-On',  2,  69.99,  '2024-01-18'),
    (29, 'S008', 'P103', 'Shoes',       'Trail Runner',    3,  109.99, '2024-01-15'),
    (30, 'S008', 'P105', 'Accessories', 'Gym Bag',         4,  59.99,  '2024-01-16'),
    (31, 'S008', 'P101', 'Apparel',     'Winter Jacket',   2,  199.99, '2024-01-17'),
    (32, 'S008', 'P107', 'Accessories', 'Sunglasses',      5,  89.99,  '2024-01-18'),
    (33, 'S009', 'P100', 'Shoes',       'Running Pro X',   2,  129.99, '2024-01-15'),
    (34, 'S009', 'P106', 'Shoes',       'Casual Slip-On',  5,  69.99,  '2024-01-16'),
    (35, 'S009', 'P102', 'Accessories', 'Sport Watch',     1,  349.99, '2024-01-17'),
    (36, 'S009', 'P104', 'Apparel',     'Fleece Vest',     3,  79.99,  '2024-01-18'),
    (37, 'S010', 'P103', 'Shoes',       'Trail Runner',    2,  109.99, '2024-01-15'),
    (38, 'S010', 'P107', 'Accessories', 'Sunglasses',      4,  89.99,  '2024-01-16'),
    (39, 'S010', 'P105', 'Accessories', 'Gym Bag',         3,  59.99,  '2024-01-17'),
    (40, 'S010', 'P101', 'Apparel',     'Winter Jacket',   1,  199.99, '2024-01-18');
