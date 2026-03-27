-- ============================================================================
-- Iceberg UniForm Equality Deletes — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table with equality delete mode. When rows
-- are deleted, the UniForm writer generates Iceberg equality delete files
-- (keyed on the `id` column) instead of position delete files. An Iceberg
-- external table is then registered to read the same data through the
-- Iceberg metadata chain, verifying the full write → convert → read cycle.
--
-- Dataset: 10 products with columns: id, name, category, price, in_stock.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.eq_del_demo
    COMMENT 'Equality delete write/read demo';

-- STEP 2: Create Delta table with UniForm + equality delete mode
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.eq_del_demo.products (
    id          INT,
    name        VARCHAR,
    category    VARCHAR,
    price       DOUBLE,
    in_stock    BOOLEAN
) LOCATION '{{data_path}}/eq_del_products'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergDeleteMode' = 'equality-delete',
    'delta.universalFormat.icebergEqualityColumns' = 'id',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.eq_del_demo.products TO USER {{current_user}};

-- STEP 3: Seed 10 products
INSERT INTO {{zone_name}}.eq_del_demo.products VALUES
    (1,  'Quantum Widget',     'Electronics', 299.99,  true),
    (2,  'Nano Sensor',        'Electronics', 149.50,  true),
    (3,  'Bio Reactor Kit',    'Science',     599.00,  true),
    (4,  'Solar Panel Mini',   'Energy',      425.00,  true),
    (5,  'Carbon Filter XL',   'Industrial',  89.99,   true),
    (6,  'LED Matrix Board',   'Electronics', 175.00,  true),
    (7,  'Thermal Coupler',    'Industrial',  64.50,   true),
    (8,  'Gene Sequencer',     'Science',     1250.00, true),
    (9,  'Wind Turbine Blade', 'Energy',      850.00,  true),
    (10, 'Plasma Cutter Pro',  'Industrial',  399.99,  true);

-- STEP 4: Delete 3 products (triggers equality delete file generation)
-- These deletes create DVs on the Delta side, which the UniForm writer
-- translates into an equality delete Parquet file keyed on `id`.
DELETE FROM {{zone_name}}.eq_del_demo.products WHERE id IN (2, 5, 8);

-- STEP 5: Register an Iceberg external table pointing at the same location
-- This reads through the Iceberg metadata chain, including the equality
-- delete file, to verify the deletes are correctly applied.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.eq_del_demo.products_iceberg
USING ICEBERG
LOCATION '{{data_path}}/eq_del_products';

GRANT ADMIN ON TABLE {{zone_name}}.eq_del_demo.products_iceberg TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.eq_del_demo.products_iceberg;
