-- ==========================================================================
-- Demo: E-Commerce Order Lifecycle — Change Data Feed with UniForm
-- Feature: CDF mutations tracked through UniForm Iceberg metadata
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos COMMENT 'Change Data Feed with UniForm';

-- --------------------------------------------------------------------------
-- Orders Table — CDF + UniForm enabled
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.orders (
    order_id        INT,
    customer_name   VARCHAR,
    product         VARCHAR,
    quantity        INT,
    unit_price      DECIMAL(10,2),
    status          VARCHAR,
    order_date      DATE
) LOCATION '{{data_path}}/orders'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id',
    'delta.enableChangeDataFeed' = 'true'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.orders TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- Seed Data — 30 e-commerce orders across 5 customers, 5 products, 5 statuses
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.iceberg_demos.orders VALUES
    (1,  'Alice Johnson',  'Laptop Pro',     1, 1299.99, 'pending',    '2025-03-01'),
    (2,  'Bob Chen',       'Wireless Mouse', 2, 29.99,   'pending',    '2025-03-01'),
    (3,  'Carol Davis',    'USB-C Hub',      1, 49.99,   'pending',    '2025-03-02'),
    (4,  'Dan Wilson',     'Monitor 27in',   1, 399.99,  'pending',    '2025-03-02'),
    (5,  'Eve Martinez',   'Keyboard Mech',  1, 149.99,  'pending',    '2025-03-03'),
    (6,  'Alice Johnson',  'Wireless Mouse', 3, 29.99,   'processing', '2025-03-03'),
    (7,  'Bob Chen',       'Monitor 27in',   1, 399.99,  'processing', '2025-03-04'),
    (8,  'Carol Davis',    'Laptop Pro',     1, 1299.99, 'processing', '2025-03-04'),
    (9,  'Dan Wilson',     'USB-C Hub',      2, 49.99,   'shipped',    '2025-03-05'),
    (10, 'Eve Martinez',   'Laptop Pro',     1, 1299.99, 'shipped',    '2025-03-05'),
    (11, 'Alice Johnson',  'Keyboard Mech',  2, 149.99,  'shipped',    '2025-03-06'),
    (12, 'Bob Chen',       'USB-C Hub',      1, 49.99,   'shipped',    '2025-03-06'),
    (13, 'Carol Davis',    'Wireless Mouse', 4, 29.99,   'delivered',  '2025-03-07'),
    (14, 'Dan Wilson',     'Keyboard Mech',  1, 149.99,  'delivered',  '2025-03-07'),
    (15, 'Eve Martinez',   'USB-C Hub',      3, 49.99,   'delivered',  '2025-03-08'),
    (16, 'Alice Johnson',  'Monitor 27in',   2, 399.99,  'pending',    '2025-03-08'),
    (17, 'Bob Chen',       'Laptop Pro',     1, 1299.99, 'pending',    '2025-03-09'),
    (18, 'Carol Davis',    'Keyboard Mech',  1, 149.99,  'pending',    '2025-03-09'),
    (19, 'Dan Wilson',     'Wireless Mouse', 5, 29.99,   'processing', '2025-03-10'),
    (20, 'Eve Martinez',   'Monitor 27in',   1, 399.99,  'processing', '2025-03-10'),
    (21, 'Alice Johnson',  'USB-C Hub',      2, 49.99,   'shipped',    '2025-03-11'),
    (22, 'Bob Chen',       'Keyboard Mech',  2, 149.99,  'shipped',    '2025-03-11'),
    (23, 'Carol Davis',    'Monitor 27in',   1, 399.99,  'delivered',  '2025-03-12'),
    (24, 'Dan Wilson',     'Laptop Pro',     1, 1299.99, 'delivered',  '2025-03-12'),
    (25, 'Eve Martinez',   'Wireless Mouse', 2, 29.99,   'cancelled',  '2025-03-13'),
    (26, 'Alice Johnson',  'Laptop Pro',     1, 1299.99, 'cancelled',  '2025-03-13'),
    (27, 'Bob Chen',       'Monitor 27in',   1, 399.99,  'pending',    '2025-03-14'),
    (28, 'Carol Davis',    'USB-C Hub',      3, 49.99,   'pending',    '2025-03-14'),
    (29, 'Dan Wilson',     'Keyboard Mech',  2, 149.99,  'processing', '2025-03-15'),
    (30, 'Eve Martinez',   'Laptop Pro',     2, 1299.99, 'processing', '2025-03-15');
