-- ============================================================================
-- Delta MERGE — CDC Upsert with BY SOURCE — Setup Script
-- ============================================================================
-- Creates the target and source tables for the MERGE CDC upsert demo.
--
-- Tables:
--   1. products        — 15 products (target)
--   2. product_feed    — 12 staged changes (source): 8 updates + 4 new
--
-- The MERGE in queries.sql will:
--   - Update 8 products (ids 1-8) with new prices from the daily feed
--   - Insert 4 new products (ids 16-19) from the feed
--   - Delete 3 discontinued products not in feed (ids 13, 14, 15)
--   - Final count: 15 - 3 + 4 = 16
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: products — 15 current products (target)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.products (
    id          INT,
    sku         VARCHAR,
    name        VARCHAR,
    category    VARCHAR,
    price       DOUBLE,
    in_stock    INT
) LOCATION 'products';


INSERT INTO {{zone_name}}.delta_demos.products VALUES
    (1,  'SKU-001', 'Wireless Mouse',       'electronics', 29.99,  150),
    (2,  'SKU-002', 'USB-C Hub',            'electronics', 49.99,  80),
    (3,  'SKU-003', 'Laptop Stand',         'accessories', 39.99,  200),
    (4,  'SKU-004', 'Webcam HD',            'electronics', 79.99,  45),
    (5,  'SKU-005', 'Desk Lamp',            'furniture',   34.99,  120),
    (6,  'SKU-006', 'Keyboard Mechanical',  'electronics', 89.99,  60),
    (7,  'SKU-007', 'Monitor Arm',          'accessories', 44.99,  90),
    (8,  'SKU-008', 'Headset Pro',          'electronics', 129.99, 35),
    (9,  'SKU-009', 'Cable Organizer',      'accessories', 14.99,  300),
    (10, 'SKU-010', 'Mousepad XL',          'accessories', 19.99,  250),
    (11, 'SKU-011', 'Docking Station',      'electronics', 199.99, 25),
    (12, 'SKU-012', 'Ergonomic Chair',      'furniture',   349.99, 15),
    (13, 'SKU-013', 'VGA Adapter',          'electronics', 12.99,  5),
    (14, 'SKU-014', 'Parallel Port Cable',  'accessories', 8.99,   2),
    (15, 'SKU-015', 'Floppy Drive USB',     'electronics', 24.99,  1);


-- ============================================================================
-- TABLE 2: product_feed — 12 items from daily supplier feed (source)
-- ============================================================================
-- IDs 1-8: price/stock updates for active products
-- IDs 16-19: brand new products entering the catalog
-- IDs 9-15: NOT in feed → candidates for NOT MATCHED BY SOURCE
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.product_feed (
    id          INT,
    sku         VARCHAR,
    name        VARCHAR,
    category    VARCHAR,
    price       DOUBLE,
    in_stock    INT
) LOCATION 'product_feed';


INSERT INTO {{zone_name}}.delta_demos.product_feed VALUES
    -- Updated products (price adjustments + restocking)
    (1,  'SKU-001', 'Wireless Mouse',       'electronics', 24.99,  200),
    (2,  'SKU-002', 'USB-C Hub',            'electronics', 44.99,  100),
    (3,  'SKU-003', 'Laptop Stand',         'accessories', 39.99,  180),
    (4,  'SKU-004', 'Webcam HD',            'electronics', 69.99,  70),
    (5,  'SKU-005', 'Desk Lamp',            'furniture',   34.99,  120),
    (6,  'SKU-006', 'Keyboard Mechanical',  'electronics', 84.99,  80),
    (7,  'SKU-007', 'Monitor Arm',          'accessories', 42.99,  100),
    (8,  'SKU-008', 'Headset Pro',          'electronics', 119.99, 50),
    -- New products
    (16, 'SKU-016', 'USB-C Monitor Cable',  'accessories', 19.99,  500),
    (17, 'SKU-017', 'Noise Cancelling Mic', 'electronics', 59.99,  75),
    (18, 'SKU-018', 'Standing Desk Mat',    'furniture',   49.99,  60),
    (19, 'SKU-019', 'Thunderbolt Dock',     'electronics', 229.99, 30);
