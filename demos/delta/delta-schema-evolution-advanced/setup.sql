-- ============================================================================
-- Delta Advanced Schema Evolution — Setup Script
-- ============================================================================
-- Creates the initial product_catalog table with 5 columns and 30 baseline
-- rows. The schema evolution (ADD COLUMN, backfill, etc.) happens in
-- queries.sql so users can step through each phase interactively.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- STEP 2: Create table with the initial 5-column schema
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.product_catalog (
    id       INT,
    name     VARCHAR,
    category VARCHAR,
    price    DOUBLE,
    stock    INT
) LOCATION 'product_catalog';


-- STEP 3: Insert 30 baseline products
INSERT INTO {{zone_name}}.delta_demos.product_catalog VALUES
    (1,  'Wireless Mouse',        'Electronics',   29.99,  150),
    (2,  'Mechanical Keyboard',   'Electronics',   89.99,  75),
    (3,  'USB-C Hub',             'Electronics',   45.99,  200),
    (4,  'Monitor Stand',         'Electronics',   34.99,  120),
    (5,  'Webcam HD',             'Electronics',   59.99,  90),
    (6,  'Office Chair',          'Furniture',     249.99, 30),
    (7,  'Standing Desk',         'Furniture',     399.99, 20),
    (8,  'Bookshelf',             'Furniture',     129.99, 45),
    (9,  'Desk Lamp',             'Furniture',     39.99,  80),
    (10, 'Filing Cabinet',        'Furniture',     89.99,  35),
    (11, 'Notebook A5',           'Stationery',    5.99,   500),
    (12, 'Gel Pen Set',           'Stationery',    12.99,  300),
    (13, 'Sticky Notes',          'Stationery',    3.99,   600),
    (14, 'Binder Clips',          'Stationery',    7.99,   400),
    (15, 'Whiteboard Marker Set', 'Stationery',    9.99,   250),
    (16, 'Laptop Sleeve 15in',    'Accessories',   24.99,  180),
    (17, 'Phone Stand',           'Accessories',   14.99,  220),
    (18, 'Cable Organizer',       'Accessories',   8.99,   350),
    (19, 'Screen Cleaner Kit',    'Accessories',   11.99,  270),
    (20, 'Mouse Pad XL',          'Accessories',   19.99,  160),
    (21, 'Bluetooth Speaker',     'Electronics',   49.99,  110),
    (22, 'Power Strip',           'Electronics',   22.99,  190),
    (23, 'Ergonomic Footrest',    'Furniture',     54.99,  60),
    (24, 'Wall Clock',            'Furniture',     29.99,  70),
    (25, 'Planner 2025',          'Stationery',    15.99,  200),
    (26, 'Highlighter Set',       'Stationery',    6.99,   350),
    (27, 'Desk Organizer',        'Accessories',   17.99,  140),
    (28, 'Wrist Rest',            'Accessories',   13.99,  190),
    (29, 'Surge Protector',       'Electronics',   31.99,  85),
    (30, 'Paper Shredder',        'Electronics',   79.99,  40);

