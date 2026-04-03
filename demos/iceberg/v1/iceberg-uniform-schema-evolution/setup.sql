-- ============================================================================
-- Iceberg UniForm Schema Evolution — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table with an initial schema and seeds
-- 20 customer orders. Schema evolution (ADD COLUMN) happens in queries.sql
-- to demonstrate how both Delta and Iceberg metadata track schema changes.
--
-- Dataset: 20 orders across 4 customers with columns:
-- id, customer_name, product, quantity, unit_price, order_date.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm and column mapping
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.customer_orders (
    id             INT,
    customer_name  VARCHAR,
    product        VARCHAR,
    quantity       INT,
    unit_price     DOUBLE,
    order_date     VARCHAR
) LOCATION '{{data_path}}/customer_orders'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.customer_orders TO USER {{current_user}};

-- STEP 3: Seed 20 orders (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.customer_orders VALUES
    (1,  'Acme Corp',      'Widget A',     10,  25.00, '2024-01-15'),
    (2,  'Acme Corp',      'Widget B',     5,   45.00, '2024-01-20'),
    (3,  'Acme Corp',      'Gadget X',     3,   120.00,'2024-02-01'),
    (4,  'Acme Corp',      'Widget A',     8,   25.00, '2024-03-10'),
    (5,  'Acme Corp',      'Gadget Y',     2,   200.00,'2024-03-15'),
    (6,  'TechStart Inc',  'Widget B',     15,  45.00, '2024-01-18'),
    (7,  'TechStart Inc',  'Gadget X',     4,   120.00,'2024-02-05'),
    (8,  'TechStart Inc',  'Widget A',     20,  25.00, '2024-02-20'),
    (9,  'TechStart Inc',  'Gadget Y',     1,   200.00,'2024-03-01'),
    (10, 'TechStart Inc',  'Widget C',     12,  35.00, '2024-03-20'),
    (11, 'Global Foods',   'Widget A',     25,  25.00, '2024-01-22'),
    (12, 'Global Foods',   'Widget C',     8,   35.00, '2024-02-10'),
    (13, 'Global Foods',   'Gadget X',     6,   120.00,'2024-02-28'),
    (14, 'Global Foods',   'Widget B',     10,  45.00, '2024-03-05'),
    (15, 'Global Foods',   'Gadget Y',     3,   200.00,'2024-03-25'),
    (16, 'DataFlow LLC',   'Gadget X',     7,   120.00,'2024-01-25'),
    (17, 'DataFlow LLC',   'Widget A',     18,  25.00, '2024-02-08'),
    (18, 'DataFlow LLC',   'Widget B',     6,   45.00, '2024-02-18'),
    (19, 'DataFlow LLC',   'Gadget Y',     4,   200.00,'2024-03-12'),
    (20, 'DataFlow LLC',   'Widget C',     10,  35.00, '2024-03-28');
