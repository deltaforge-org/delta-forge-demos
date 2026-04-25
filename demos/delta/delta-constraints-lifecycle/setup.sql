-- ============================================================================
-- Delta Constraints Lifecycle — Setup Script
-- ============================================================================
-- Demonstrates:
--   1. CHECK constraints surviving bulk UPDATEs
--   2. Constraint boundary testing (stock >= 0 allows zero)
--   3. Re-validation after every DML operation
--
-- Table:
--   products — 20 products across 6 categories with 3 CHECK constraints
--
-- CHECK constraints:
--   - price_positive:      price > 0
--   - stock_non_negative:  stock >= 0
--   - discount_non_negative: discount >= 0
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: products — catalog with CHECK constraints
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.products (
    id        INT,
    name      VARCHAR,
    category  VARCHAR,
    price     DOUBLE,
    stock     INT,
    discount  DOUBLE
) LOCATION 'products'
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.constraints.price_positive' = 'price > 0',
    'delta.constraints.stock_non_negative' = 'stock >= 0',
    'delta.constraints.discount_non_negative' = 'discount >= 0'
);


-- Insert 20 products (all satisfy constraints)
INSERT INTO {{zone_name}}.delta_demos.products VALUES
    (1,  'Widget A',     'Tools',       25.00,  100, 0.00),
    (2,  'Widget B',     'Tools',       30.00,  80,  5.00),
    (3,  'Gadget X',     'Electronics', 150.00, 50,  10.00),
    (4,  'Gadget Y',     'Electronics', 200.00, 30,  0.00),
    (5,  'Book Alpha',   'Books',       20.00,  200, 0.00),
    (6,  'Book Beta',    'Books',       35.00,  150, 15.00),
    (7,  'Shirt Red',    'Clothing',    40.00,  75,  20.00),
    (8,  'Shirt Blue',   'Clothing',    40.00,  60,  0.00),
    (9,  'Lamp Basic',   'Home',        45.00,  40,  5.00),
    (10, 'Lamp Pro',     'Home',        90.00,  25,  10.00),
    (11, 'Cable USB',    'Electronics', 10.00,  500, 0.00),
    (12, 'Cable HDMI',   'Electronics', 15.00,  300, 0.00),
    (13, 'Mug Plain',    'Home',        12.00,  200, 0.00),
    (14, 'Mug Fancy',    'Home',        18.00,  100, 5.00),
    (15, 'Pen Set',      'Office',      8.00,   400, 0.00),
    (16, 'Notebook',     'Office',      12.00,  250, 10.00),
    (17, 'Bag Small',    'Clothing',    55.00,  45,  0.00),
    (18, 'Bag Large',    'Clothing',    85.00,  20,  25.00),
    (19, 'Tool Kit',     'Tools',       60.00,  35,  0.00),
    (20, 'Tool Pro',     'Tools',       120.00, 15,  10.00);
