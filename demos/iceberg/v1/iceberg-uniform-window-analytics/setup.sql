-- ==========================================================================
-- Demo: Regional Sales Performance — Window Analytics with UniForm
-- Feature: Window functions on UniForm Iceberg tables
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos COMMENT 'Window functions with UniForm';

-- --------------------------------------------------------------------------
-- Sales Table
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.sales (
    sale_id          INT,
    rep_name         VARCHAR,
    region           VARCHAR,
    product_category VARCHAR,
    sale_amount      DECIMAL(10,2),
    commission_pct   DECIMAL(4,1),
    sale_date        DATE
) LOCATION '{{data_path}}/sales'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.sales TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- Seed Data — 40 sales across 7 reps, 4 regions, 3 categories
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.iceberg_demos.sales VALUES
    (1,  'Emma Clark',    'Northeast', 'Electronics', 4500.00,  8.5,  '2025-01-05'),
    (2,  'Liam Foster',   'Southeast', 'Furniture',   3200.00,  7.0,  '2025-01-07'),
    (3,  'Sophia Grant',  'West',      'Electronics', 5100.00,  9.0,  '2025-01-08'),
    (4,  'Noah Hayes',    'Midwest',   'Clothing',    1800.00,  6.0,  '2025-01-10'),
    (5,  'Olivia Kim',    'Northeast', 'Furniture',   6200.00,  8.5,  '2025-01-12'),
    (6,  'Emma Clark',    'Northeast', 'Clothing',    2100.00,  6.5,  '2025-01-14'),
    (7,  'Liam Foster',   'Southeast', 'Electronics', 7800.00,  9.5,  '2025-01-15'),
    (8,  'Sophia Grant',  'West',      'Furniture',   4400.00,  7.5,  '2025-01-17'),
    (9,  'Noah Hayes',    'Midwest',   'Electronics', 3600.00,  8.0,  '2025-01-19'),
    (10, 'Olivia Kim',    'Northeast', 'Clothing',    1500.00,  5.5,  '2025-01-20'),
    (11, 'James Lee',     'Southeast', 'Furniture',   5500.00,  8.0,  '2025-01-22'),
    (12, 'Ava Moore',     'West',      'Clothing',    2800.00,  7.0,  '2025-01-24'),
    (13, 'Emma Clark',    'Northeast', 'Electronics', 8900.00,  10.0, '2025-01-25'),
    (14, 'Liam Foster',   'Southeast', 'Clothing',    1200.00,  5.0,  '2025-01-27'),
    (15, 'Sophia Grant',  'West',      'Electronics', 6700.00,  9.5,  '2025-01-28'),
    (16, 'Noah Hayes',    'Midwest',   'Furniture',   4100.00,  7.5,  '2025-01-30'),
    (17, 'Olivia Kim',    'Northeast', 'Electronics', 9200.00,  10.0, '2025-02-01'),
    (18, 'James Lee',     'Southeast', 'Electronics', 3900.00,  8.0,  '2025-02-03'),
    (19, 'Ava Moore',     'West',      'Furniture',   5800.00,  8.5,  '2025-02-05'),
    (20, 'Emma Clark',    'Northeast', 'Furniture',   3700.00,  7.0,  '2025-02-07'),
    (21, 'Liam Foster',   'Southeast', 'Furniture',   4600.00,  7.5,  '2025-02-08'),
    (22, 'Sophia Grant',  'West',      'Clothing',    2200.00,  6.5,  '2025-02-10'),
    (23, 'Noah Hayes',    'Midwest',   'Clothing',    1600.00,  5.5,  '2025-02-12'),
    (24, 'Olivia Kim',    'Northeast', 'Furniture',   7100.00,  9.0,  '2025-02-14'),
    (25, 'James Lee',     'Southeast', 'Clothing',    2500.00,  6.5,  '2025-02-15'),
    (26, 'Ava Moore',     'West',      'Electronics', 8200.00,  9.5,  '2025-02-17'),
    (27, 'Emma Clark',    'Northeast', 'Clothing',    1900.00,  6.0,  '2025-02-19'),
    (28, 'Liam Foster',   'Southeast', 'Electronics', 6100.00,  9.0,  '2025-02-20'),
    (29, 'Sophia Grant',  'West',      'Furniture',   4900.00,  8.0,  '2025-02-22'),
    (30, 'Noah Hayes',    'Midwest',   'Electronics', 5200.00,  8.5,  '2025-02-24'),
    (31, 'Olivia Kim',    'Northeast', 'Clothing',    2300.00,  6.5,  '2025-02-25'),
    (32, 'James Lee',     'Southeast', 'Furniture',   4800.00,  7.5,  '2025-02-27'),
    (33, 'Ava Moore',     'West',      'Clothing',    3100.00,  7.0,  '2025-03-01'),
    (34, 'Emma Clark',    'Northeast', 'Electronics', 7500.00,  9.5,  '2025-03-03'),
    (35, 'Liam Foster',   'Southeast', 'Clothing',    1800.00,  5.5,  '2025-03-05'),
    (36, 'Sophia Grant',  'West',      'Electronics', 9400.00,  10.0, '2025-03-07'),
    (37, 'Noah Hayes',    'Midwest',   'Furniture',   3800.00,  7.0,  '2025-03-09'),
    (38, 'Olivia Kim',    'Northeast', 'Electronics', 6800.00,  9.0,  '2025-03-10'),
    (39, 'James Lee',     'Southeast', 'Electronics', 5300.00,  8.5,  '2025-03-12'),
    (40, 'Ava Moore',     'West',      'Furniture',   4200.00,  7.5,  '2025-03-14');
