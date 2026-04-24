-- ============================================================================
-- Delta RESTORE — Rollback to Previous Versions — Setup Script
-- ============================================================================
-- Creates the product_inventory table with 30 products (V0 baseline).
-- All version operations (V1-V5) are in queries.sql so users can run them
-- step by step and observe the RESTORE rollback workflow.
--
-- Tables created:
--   1. product_inventory — 30 products (V0 baseline, all active)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- V0: CREATE + INSERT 30 products
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.product_inventory (
    id              INT,
    name            VARCHAR,
    category        VARCHAR,
    price           DOUBLE,
    qty             INT,
    status          VARCHAR
) LOCATION 'product_inventory';


INSERT INTO {{zone_name}}.delta_demos.product_inventory VALUES
    (1,  'Laptop Pro 15',       'Electronics', 1299.99, 50,  'active'),
    (2,  'Wireless Mouse',      'Electronics', 29.99,   200, 'active'),
    (3,  'USB-C Hub',           'Electronics', 49.99,   150, 'active'),
    (4,  'Monitor 27" 4K',      'Electronics', 449.99,  40,  'active'),
    (5,  'Mechanical Keyboard', 'Electronics', 129.99,  90,  'active'),
    (6,  'Webcam HD',           'Electronics', 69.99,   120, 'active'),
    (7,  'Office Chair',        'Furniture',   349.99,  25,  'active'),
    (8,  'Standing Desk',       'Furniture',   599.99,  15,  'active'),
    (9,  'Desk Lamp LED',       'Furniture',   44.99,   80,  'active'),
    (10, 'Bookshelf Oak',       'Furniture',   179.99,  30,  'active'),
    (11, 'Filing Cabinet',      'Furniture',   129.99,  45,  'active'),
    (12, 'Whiteboard 4x3',      'Furniture',   89.99,   35,  'active'),
    (13, 'Notebook A5',         'Stationery',  5.99,    500, 'active'),
    (14, 'Ballpoint Pen Set',   'Stationery',  12.99,   300, 'active'),
    (15, 'Sticky Notes Pack',   'Stationery',  3.49,    800, 'active'),
    (16, 'Highlighter Set',     'Stationery',  7.99,    250, 'active'),
    (17, 'Binder Clips Box',    'Stationery',  4.99,    400, 'active'),
    (18, 'Paper Ream A4',       'Stationery',  8.99,    600, 'active'),
    (19, 'Headphones NC',       'Audio',       199.99,  65,  'active'),
    (20, 'Bluetooth Speaker',   'Audio',       79.99,   85,  'active'),
    (21, 'Microphone USB',      'Audio',       149.99,  45,  'active'),
    (22, 'Earbuds Pro',         'Audio',       89.99,   110, 'active'),
    (23, 'Sound Bar',           'Audio',       249.99,  20,  'active'),
    (24, 'DAC Amplifier',       'Audio',       129.99,  30,  'active'),
    (25, 'Surge Protector',     'Accessories', 24.99,   180, 'active'),
    (26, 'Cable Organizer',     'Accessories', 14.99,   220, 'active'),
    (27, 'Laptop Stand',        'Accessories', 39.99,   95,  'active'),
    (28, 'Mouse Pad XL',        'Accessories', 19.99,   160, 'active'),
    (29, 'Screen Cleaner',      'Accessories', 9.99,    350, 'active'),
    (30, 'USB Flash Drive',     'Accessories', 12.99,   280, 'active');

