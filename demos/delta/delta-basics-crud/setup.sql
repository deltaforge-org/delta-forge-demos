-- ============================================================================
-- Delta Basics — CRUD Operations — Setup Script
-- ============================================================================
-- Creates the products table with 20 rows of baseline data.
-- The actual CRUD operations (UPDATE, DELETE, INSERT) are in queries.sql
-- so users can run them interactively and learn by doing.
--
-- Tables created:
--   1. products  — 20 hand-picked products across 4 categories
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: products — 20 products across 4 categories
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.products (
    id         INT,
    name       VARCHAR,
    category   VARCHAR,
    price      DOUBLE,
    stock      INT,
    is_active  BOOLEAN
) LOCATION 'products';


-- Insert 20 known products as baseline data
INSERT INTO {{zone_name}}.delta_demos.products VALUES
    (1,  'Laptop',            'Electronics', 999.99,  50,  true),
    (2,  'Wireless Mouse',    'Electronics', 29.99,   200, true),
    (3,  'USB-C Hub',         'Electronics', 49.99,   150, true),
    (4,  'Monitor 27"',       'Electronics', 349.99,  75,  true),
    (5,  'Keyboard',          'Electronics', 79.99,   120, true),
    (6,  'Office Chair',      'Furniture',   299.99,  30,  true),
    (7,  'Standing Desk',     'Furniture',   599.99,  20,  true),
    (8,  'Bookshelf',         'Furniture',   149.99,  45,  true),
    (9,  'Desk Lamp',         'Furniture',   39.99,   0,   true),
    (10, 'Filing Cabinet',    'Furniture',   89.99,   60,  true),
    (11, 'Notebook A5',       'Stationery',  5.99,    500, true),
    (12, 'Ballpoint Pen',     'Stationery',  1.99,    1000,true),
    (13, 'Sticky Notes',      'Stationery',  3.49,    800, true),
    (14, 'Highlighter Set',   'Stationery',  7.99,    300, true),
    (15, 'Binder Clips',      'Stationery',  2.49,    0,   true),
    (16, 'Headphones',        'Audio',       149.99,  90,  true),
    (17, 'Bluetooth Speaker', 'Audio',       79.99,   110, true),
    (18, 'Microphone',        'Audio',       199.99,  40,  true),
    (19, 'Earbuds',           'Audio',       59.99,   0,   true),
    (20, 'Sound Bar',         'Audio',       249.99,  25,  true);

