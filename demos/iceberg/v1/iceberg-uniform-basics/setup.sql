-- ============================================================================
-- Iceberg UniForm Basics — Setup
-- ============================================================================
-- Creates a Delta table with Iceberg UniForm enabled from the start.
-- Every subsequent commit will produce Iceberg metadata alongside the
-- Delta transaction log, making the table dual-readable.
--
-- Dataset: 15 products across 3 categories (Electronics, Furniture, Audio)
-- with columns: id, name, category, price, stock, rating.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create the Delta table with UniForm enabled
-- Setting delta.universalFormat.enabledFormats = 'iceberg' activates the
-- post-commit hook that generates Iceberg metadata after every Delta commit.
-- delta.columnMapping.mode = 'id' is required for Iceberg compatibility.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.product_catalog (
    id         INT,
    name       VARCHAR,
    category   VARCHAR,
    price      DOUBLE,
    stock      INT,
    rating     DOUBLE
) LOCATION '{{data_path}}/product_catalog'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.product_catalog TO USER {{current_user}};

-- STEP 3: Seed data — 15 products across 3 categories
INSERT INTO {{zone_name}}.iceberg_demos.product_catalog VALUES
    (1,  'Laptop Pro',       'Electronics', 1299.99, 45,  4.7),
    (2,  'Wireless Mouse',   'Electronics', 29.99,   200, 4.3),
    (3,  'USB-C Hub',        'Electronics', 49.99,   150, 4.5),
    (4,  'Monitor 27"',      'Electronics', 399.99,  60,  4.6),
    (5,  'Keyboard Mech',    'Electronics', 89.99,   120, 4.4),
    (6,  'Standing Desk',    'Furniture',   549.99,  30,  4.8),
    (7,  'Ergonomic Chair',  'Furniture',   449.99,  40,  4.7),
    (8,  'Monitor Arm',      'Furniture',   79.99,   100, 4.2),
    (9,  'Desk Lamp',        'Furniture',   39.99,   180, 4.1),
    (10, 'Footrest',         'Furniture',   59.99,   90,  3.9),
    (11, 'Headphones Pro',   'Audio',       249.99,  75,  4.6),
    (12, 'Bluetooth Speaker','Audio',       79.99,   110, 4.3),
    (13, 'Microphone USB',   'Audio',       129.99,  65,  4.5),
    (14, 'Earbuds Wireless', 'Audio',       59.99,   200, 4.2),
    (15, 'Soundbar',         'Audio',       199.99,  50,  4.4);
