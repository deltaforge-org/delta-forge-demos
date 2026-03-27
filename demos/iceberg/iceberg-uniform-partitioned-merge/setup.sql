-- ============================================================================
-- Iceberg UniForm Partitioned MERGE (Inventory Sync) — Setup
-- ============================================================================
-- Creates a partitioned Delta table with Iceberg UniForm enabled and seeds
-- it with 36 warehouse inventory records. Two MERGE operations in
-- queries.sql simulate shipment receiving and inventory audits.
--
-- Dataset: 36 SKUs across 3 warehouses (portland, dallas, charlotte)
-- Partitioned by: warehouse
-- Schema: sku, warehouse, product_name, quantity_on_hand, reorder_point,
--         unit_cost, last_received
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create partitioned table with UniForm
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.warehouse_inventory (
    sku               VARCHAR,
    warehouse         VARCHAR,
    product_name      VARCHAR,
    quantity_on_hand  INT,
    reorder_point     INT,
    unit_cost         DOUBLE,
    last_received     VARCHAR
) LOCATION '{{data_path}}/warehouse_inventory'
PARTITIONED BY (warehouse)
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.warehouse_inventory TO USER {{current_user}};

-- STEP 3: Seed 36 SKUs — 12 per warehouse (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.warehouse_inventory VALUES
    -- portland: 12 SKUs
    ('WH-P001', 'portland',  'Industrial Bolt M10',       500,  100, 0.45,  '2024-01-10'),
    ('WH-P002', 'portland',  'Steel Washer 3/8"',         1200, 200, 0.12,  '2024-01-10'),
    ('WH-P003', 'portland',  'Copper Wire 14ga (100ft)',   80,  20,  24.99, '2024-01-12'),
    ('WH-P004', 'portland',  'PVC Pipe 2" (10ft)',        150,  30,  8.75,  '2024-01-12'),
    ('WH-P005', 'portland',  'LED Panel 2x4',              60,  15,  45.00, '2024-01-14'),
    ('WH-P006', 'portland',  'Concrete Anchor 1/2"',      900,  150, 0.89,  '2024-01-14'),
    ('WH-P007', 'portland',  'Fiberglass Insulation R30',  200, 50,  32.50, '2024-01-16'),
    ('WH-P008', 'portland',  'Stainless Hinge 4"',        350,  75,  3.25,  '2024-01-16'),
    ('WH-P009', 'portland',  'Rubber Gasket Set',          400,  80,  6.99,  '2024-01-18'),
    ('WH-P010', 'portland',  'Aluminum Channel 6ft',      120,  25,  15.50, '2024-01-18'),
    ('WH-P011', 'portland',  'Circuit Breaker 20A',       180,  40,  12.75, '2024-01-20'),
    ('WH-P012', 'portland',  'Silicone Sealant 10oz',     600,  100, 4.50,  '2024-01-20'),
    -- dallas: 12 SKUs
    ('WH-D001', 'dallas',   'Industrial Bolt M10',        450,  100, 0.48,  '2024-01-11'),
    ('WH-D002', 'dallas',   'Steel Washer 3/8"',          1000, 200, 0.13,  '2024-01-11'),
    ('WH-D003', 'dallas',   'Copper Wire 14ga (100ft)',    70,  20,  25.50, '2024-01-13'),
    ('WH-D004', 'dallas',   'PVC Pipe 2" (10ft)',         130,  30,  9.00,  '2024-01-13'),
    ('WH-D005', 'dallas',   'LED Panel 2x4',               55,  15,  46.00, '2024-01-15'),
    ('WH-D006', 'dallas',   'Concrete Anchor 1/2"',       800,  150, 0.92,  '2024-01-15'),
    ('WH-D007', 'dallas',   'Fiberglass Insulation R30',   180, 50,  33.00, '2024-01-17'),
    ('WH-D008', 'dallas',   'Stainless Hinge 4"',         300,  75,  3.40,  '2024-01-17'),
    ('WH-D009', 'dallas',   'Rubber Gasket Set',           350,  80,  7.25,  '2024-01-19'),
    ('WH-D010', 'dallas',   'Aluminum Channel 6ft',       100,  25,  16.00, '2024-01-19'),
    ('WH-D011', 'dallas',   'Circuit Breaker 20A',        160,  40,  13.00, '2024-01-21'),
    ('WH-D012', 'dallas',   'Silicone Sealant 10oz',      550,  100, 4.75,  '2024-01-21'),
    -- charlotte: 12 SKUs
    ('WH-C001', 'charlotte', 'Industrial Bolt M10',       480,  100, 0.46,  '2024-01-10'),
    ('WH-C002', 'charlotte', 'Steel Washer 3/8"',         1100, 200, 0.11,  '2024-01-10'),
    ('WH-C003', 'charlotte', 'Copper Wire 14ga (100ft)',    90,  20,  24.50, '2024-01-12'),
    ('WH-C004', 'charlotte', 'PVC Pipe 2" (10ft)',        140,  30,  8.50,  '2024-01-12'),
    ('WH-C005', 'charlotte', 'LED Panel 2x4',              65,  15,  44.50, '2024-01-14'),
    ('WH-C006', 'charlotte', 'Concrete Anchor 1/2"',      850,  150, 0.88,  '2024-01-14'),
    ('WH-C007', 'charlotte', 'Fiberglass Insulation R30',  210, 50,  31.99, '2024-01-16'),
    ('WH-C008', 'charlotte', 'Stainless Hinge 4"',        320,  75,  3.15,  '2024-01-16'),
    ('WH-C009', 'charlotte', 'Rubber Gasket Set',          380,  80,  6.75,  '2024-01-18'),
    ('WH-C010', 'charlotte', 'Aluminum Channel 6ft',      110,  25,  15.25, '2024-01-18'),
    ('WH-C011', 'charlotte', 'Circuit Breaker 20A',       170,  40,  12.50, '2024-01-20'),
    ('WH-C012', 'charlotte', 'Silicone Sealant 10oz',     580,  100, 4.25,  '2024-01-20');
