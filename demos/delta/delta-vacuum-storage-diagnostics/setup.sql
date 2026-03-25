-- ============================================================================
-- Delta VACUUM Storage Diagnostics — Measuring Cleanup Impact — Setup Script
-- ============================================================================
-- Creates a product inventory table and inserts 30 products across 5
-- categories. All mutations (price updates, deletions, new arrivals) happen
-- in queries.sql so you can observe DESCRIBE DETAIL metrics at each stage.
--
-- Tables created:
--   1. product_inventory — 30 initial rows across 5 categories
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: product_inventory — E-commerce product catalog
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.product_inventory (
    id              INT,
    sku             VARCHAR,
    product_name    VARCHAR,
    category        VARCHAR,
    price           DOUBLE,
    stock_qty       INT,
    status          VARCHAR
) LOCATION '{{data_path}}/product_inventory';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.product_inventory TO USER {{current_user}};

-- V1: Insert 30 products across 5 categories
INSERT INTO {{zone_name}}.delta_demos.product_inventory VALUES
    -- Electronics (ids 1-6)
    (1,  'SKU-E001', 'Wireless Headphones',   'Electronics',  79.99,  150, 'active'),
    (2,  'SKU-E002', 'Bluetooth Speaker',      'Electronics', 149.99,   85, 'active'),
    (3,  'SKU-E003', 'USB-C Hub',             'Electronics',  49.99,  200, 'active'),
    (4,  'SKU-E004', 'Smart Watch',           'Electronics', 299.99,   60, 'active'),
    (5,  'SKU-E005', 'Portable Charger',      'Electronics',  34.99,  300, 'active'),
    (6,  'SKU-E006', 'Noise Cancelling Buds', 'Electronics', 199.99,  110, 'active'),
    -- Clothing (ids 7-12)
    (7,  'SKU-C001', 'Cotton T-Shirt',        'Clothing',     24.99,  500, 'active'),
    (8,  'SKU-C002', 'Denim Jeans',           'Clothing',     59.99,  200, 'active'),
    (9,  'SKU-C003', 'Running Shoes',         'Clothing',     89.99,  150, 'active'),
    (10, 'SKU-C004', 'Winter Jacket',         'Clothing',    149.99,   80, 'active'),
    (11, 'SKU-C005', 'Baseball Cap',          'Clothing',     19.99,  400, 'active'),
    (12, 'SKU-C006', 'Wool Scarf',            'Clothing',     39.99,  180, 'active'),
    -- Home (ids 13-18)
    (13, 'SKU-H001', 'LED Desk Lamp',         'Home',         44.99,  250, 'active'),
    (14, 'SKU-H002', 'Coffee Maker',          'Home',         89.99,  120, 'active'),
    (15, 'SKU-H003', 'Air Purifier',          'Home',        199.99,   75, 'active'),
    (16, 'SKU-H004', 'Throw Blanket',         'Home',         34.99,  300, 'active'),
    (17, 'SKU-H005', 'Kitchen Scale',         'Home',         29.99,  220, 'active'),
    (18, 'SKU-H006', 'Plant Pot Set',         'Home',         54.99,  160, 'active'),
    -- Sports (ids 19-24)
    (19, 'SKU-S001', 'Yoga Mat',              'Sports',       29.99,  350, 'active'),
    (20, 'SKU-S002', 'Resistance Bands',      'Sports',       19.99,  500, 'active'),
    (21, 'SKU-S003', 'Water Bottle',          'Sports',       14.99,  600, 'active'),
    (22, 'SKU-S004', 'Tennis Racket',         'Sports',       79.99,   90, 'active'),
    (23, 'SKU-S005', 'Fitness Tracker',       'Sports',       59.99,  180, 'active'),
    (24, 'SKU-S006', 'Jump Rope',             'Sports',       12.99,  400, 'active'),
    -- Food (ids 25-30)
    (25, 'SKU-F001', 'Organic Coffee Beans',  'Food',         18.99,  400, 'active'),
    (26, 'SKU-F002', 'Dark Chocolate Bar',    'Food',          5.99,  800, 'active'),
    (27, 'SKU-F003', 'Trail Mix Pack',        'Food',         12.99,  300, 'active'),
    (28, 'SKU-F004', 'Green Tea Box',         'Food',          9.99,  500, 'active'),
    (29, 'SKU-F005', 'Protein Bars 12pk',     'Food',         24.99,  250, 'active'),
    (30, 'SKU-F006', 'Dried Fruit Bag',       'Food',          7.99,  350, 'active');
