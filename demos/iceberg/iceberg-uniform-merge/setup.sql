-- ============================================================================
-- Iceberg UniForm MERGE INTO (CDC Upsert) — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table and seeds it with 30 e-commerce
-- orders. Two MERGE operations in queries.sql simulate daily CDC syncs,
-- each producing a new Delta version and Iceberg snapshot.
--
-- Dataset: 30 orders across 3 regions (us-east, us-west, eu-west)
-- Statuses: pending, shipped, delivered
-- Schema: order_id, customer_email, product_sku, quantity, unit_price,
--         status, region, order_date
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm enabled
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.order_fulfillment (
    order_id        INT,
    customer_email  VARCHAR,
    product_sku     VARCHAR,
    quantity        INT,
    unit_price      DOUBLE,
    status          VARCHAR,
    region          VARCHAR,
    order_date      VARCHAR
) LOCATION '{{data_path}}/order_fulfillment'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.order_fulfillment TO USER {{current_user}};

-- STEP 3: Seed 30 orders (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.order_fulfillment VALUES
    -- us-east: 10 orders
    (1,  'alice@example.com',   'SKU-1001', 2,  29.99,  'pending',   'us-east', '2024-01-15'),
    (2,  'bob@example.com',     'SKU-1002', 1,  49.99,  'shipped',   'us-east', '2024-01-16'),
    (3,  'carol@example.com',   'SKU-1003', 5,  12.50,  'delivered', 'us-east', '2024-01-17'),
    (4,  'dave@example.com',    'SKU-1004', 3,  89.99,  'pending',   'us-east', '2024-01-18'),
    (5,  'eve@example.com',     'SKU-1005', 1,  199.99, 'shipped',   'us-east', '2024-01-19'),
    (6,  'frank@example.com',   'SKU-1006', 4,  15.00,  'delivered', 'us-east', '2024-02-01'),
    (7,  'grace@example.com',   'SKU-1007', 2,  75.00,  'pending',   'us-east', '2024-02-02'),
    (8,  'hank@example.com',    'SKU-1008', 6,  22.50,  'shipped',   'us-east', '2024-02-03'),
    (9,  'iris@example.com',    'SKU-1009', 1,  350.00, 'delivered', 'us-east', '2024-02-04'),
    (10, 'jack@example.com',    'SKU-1010', 3,  45.00,  'pending',   'us-east', '2024-02-05'),
    -- us-west: 10 orders
    (11, 'karen@example.com',   'SKU-2001', 2,  59.99,  'pending',   'us-west', '2024-01-15'),
    (12, 'leo@example.com',     'SKU-2002', 1,  129.99, 'shipped',   'us-west', '2024-01-16'),
    (13, 'mia@example.com',     'SKU-2003', 4,  34.99,  'delivered', 'us-west', '2024-01-17'),
    (14, 'nick@example.com',    'SKU-2004', 2,  67.50,  'pending',   'us-west', '2024-01-18'),
    (15, 'olivia@example.com',  'SKU-2005', 3,  44.99,  'shipped',   'us-west', '2024-01-19'),
    (16, 'paul@example.com',    'SKU-2006', 1,  250.00, 'delivered', 'us-west', '2024-02-01'),
    (17, 'quinn@example.com',   'SKU-2007', 5,  18.00,  'pending',   'us-west', '2024-02-02'),
    (18, 'rachel@example.com',  'SKU-2008', 2,  85.00,  'shipped',   'us-west', '2024-02-03'),
    (19, 'sam@example.com',     'SKU-2009', 1,  420.00, 'delivered', 'us-west', '2024-02-04'),
    (20, 'tina@example.com',    'SKU-2010', 3,  39.99,  'pending',   'us-west', '2024-02-05'),
    -- eu-west: 10 orders
    (21, 'uma@example.com',     'SKU-3001', 2,  55.00,  'pending',   'eu-west', '2024-01-15'),
    (22, 'victor@example.com',  'SKU-3002', 1,  175.00, 'shipped',   'eu-west', '2024-01-16'),
    (23, 'wendy@example.com',   'SKU-3003', 3,  28.99,  'delivered', 'eu-west', '2024-01-17'),
    (24, 'xavier@example.com',  'SKU-3004', 4,  62.00,  'pending',   'eu-west', '2024-01-18'),
    (25, 'yara@example.com',    'SKU-3005', 1,  299.99, 'shipped',   'eu-west', '2024-01-19'),
    (26, 'zach@example.com',    'SKU-3006', 2,  110.00, 'delivered', 'eu-west', '2024-02-01'),
    (27, 'amy@example.com',     'SKU-3007', 6,  14.99,  'pending',   'eu-west', '2024-02-02'),
    (28, 'brian@example.com',   'SKU-3008', 1,  195.00, 'shipped',   'eu-west', '2024-02-03'),
    (29, 'cindy@example.com',   'SKU-3009', 2,  320.00, 'delivered', 'eu-west', '2024-02-04'),
    (30, 'derek@example.com',   'SKU-3010', 3,  42.00,  'pending',   'eu-west', '2024-02-05');
