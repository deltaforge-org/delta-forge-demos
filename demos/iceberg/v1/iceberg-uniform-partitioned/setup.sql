-- ============================================================================
-- Iceberg UniForm Partitioned — Setup
-- ============================================================================
-- Creates a partitioned Delta table with Iceberg UniForm enabled.
-- Iceberg metadata includes partition specs derived from the Delta
-- partition columns, enabling partition pruning in Iceberg readers.
--
-- Dataset: 24 sales transactions across 3 regions and 4 quarters.
-- Partitioned by: region
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create partitioned table with UniForm
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.regional_sales (
    id          INT,
    product     VARCHAR,
    region      VARCHAR,
    quarter     VARCHAR,
    amount      DOUBLE,
    quantity    INT,
    sales_rep   VARCHAR
) LOCATION '{{data_path}}/regional_sales'
PARTITIONED BY (region)
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.regional_sales TO USER {{current_user}};

-- STEP 3: Seed 24 transactions (Version 1, Iceberg Snapshot 1)
-- 8 transactions per region, 2 per quarter.
INSERT INTO {{zone_name}}.iceberg_demos.regional_sales VALUES
    -- us-east: 8 transactions
    (1,  'Widget Pro',   'us-east', 'Q1-2024', 1200.00, 12, 'Alice'),
    (2,  'Gadget Max',   'us-east', 'Q1-2024', 850.00,  5,  'Alice'),
    (3,  'Widget Pro',   'us-east', 'Q2-2024', 1500.00, 15, 'Bob'),
    (4,  'Gadget Max',   'us-east', 'Q2-2024', 680.00,  4,  'Bob'),
    (5,  'Widget Pro',   'us-east', 'Q3-2024', 900.00,  9,  'Alice'),
    (6,  'Gadget Max',   'us-east', 'Q3-2024', 1020.00, 6,  'Bob'),
    (7,  'Widget Pro',   'us-east', 'Q4-2024', 1800.00, 18, 'Alice'),
    (8,  'Gadget Max',   'us-east', 'Q4-2024', 510.00,  3,  'Bob'),
    -- us-west: 8 transactions
    (9,  'Widget Pro',   'us-west', 'Q1-2024', 1100.00, 11, 'Carol'),
    (10, 'Gadget Max',   'us-west', 'Q1-2024', 1360.00, 8,  'Carol'),
    (11, 'Widget Pro',   'us-west', 'Q2-2024', 700.00,  7,  'Dave'),
    (12, 'Gadget Max',   'us-west', 'Q2-2024', 1190.00, 7,  'Dave'),
    (13, 'Widget Pro',   'us-west', 'Q3-2024', 1400.00, 14, 'Carol'),
    (14, 'Gadget Max',   'us-west', 'Q3-2024', 850.00,  5,  'Dave'),
    (15, 'Widget Pro',   'us-west', 'Q4-2024', 600.00,  6,  'Carol'),
    (16, 'Gadget Max',   'us-west', 'Q4-2024', 1530.00, 9,  'Dave'),
    -- eu-west: 8 transactions
    (17, 'Widget Pro',   'eu-west', 'Q1-2024', 950.00,  10, 'Eve'),
    (18, 'Gadget Max',   'eu-west', 'Q1-2024', 1020.00, 6,  'Eve'),
    (19, 'Widget Pro',   'eu-west', 'Q2-2024', 1300.00, 13, 'Frank'),
    (20, 'Gadget Max',   'eu-west', 'Q2-2024', 680.00,  4,  'Frank'),
    (21, 'Widget Pro',   'eu-west', 'Q3-2024', 1150.00, 12, 'Eve'),
    (22, 'Gadget Max',   'eu-west', 'Q3-2024', 1700.00, 10, 'Frank'),
    (23, 'Widget Pro',   'eu-west', 'Q4-2024', 800.00,  8,  'Eve'),
    (24, 'Gadget Max',   'eu-west', 'Q4-2024', 1190.00, 7,  'Frank');
