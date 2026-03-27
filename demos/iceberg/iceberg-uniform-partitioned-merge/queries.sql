-- ============================================================================
-- Iceberg UniForm Partitioned MERGE (Inventory Sync) — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH PARTITIONED MERGES
-- -------------------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- When the table is PARTITIONED BY (warehouse), each MERGE only rewrites
-- data files in the affected partitions. The Iceberg snapshot tracks which
-- partition manifests changed, enabling external Iceberg engines to use
-- partition pruning and incremental reads.
--
-- MERGE on a partitioned table is the most demanding operation for
-- UniForm: the shadow metadata must correctly reflect per-partition
-- inserts, updates, and deletes in a single atomic snapshot.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running, verify partition specs in the Iceberg metadata:
--   python3 verify_iceberg_metadata.py <table_data_path>/warehouse_inventory -v
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline State (Version 1 / Snapshot 1)
-- ============================================================================
-- 36 SKUs: 12 per warehouse.

ASSERT ROW_COUNT = 36
SELECT * FROM {{zone_name}}.iceberg_demos.warehouse_inventory ORDER BY sku;


-- ============================================================================
-- Query 1: Per-Warehouse Summary
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sku_count = 12 WHERE warehouse = 'charlotte'
ASSERT VALUE sku_count = 12 WHERE warehouse = 'dallas'
ASSERT VALUE sku_count = 12 WHERE warehouse = 'portland'
ASSERT VALUE inventory_value = 23935.70 WHERE warehouse = 'charlotte'
ASSERT VALUE inventory_value = 22357.00 WHERE warehouse = 'dallas'
ASSERT VALUE inventory_value = 24470.20 WHERE warehouse = 'portland'
SELECT
    warehouse,
    COUNT(*) AS sku_count,
    SUM(quantity_on_hand) AS total_qty,
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS inventory_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- Query 2: Total Inventory Value
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_value = 70762.90
ASSERT VALUE total_quantity = 13380
SELECT
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS total_value,
    SUM(quantity_on_hand) AS total_quantity
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- LEARN: MERGE 1 — Receive Shipment (Version 2 / Snapshot 2)
-- ============================================================================
-- Shipment arrives: 18 existing SKUs get quantity additions (6 per
-- warehouse), and 6 new SKUs are added (2 per warehouse).
-- WHEN MATCHED: add received quantity to existing on-hand, update last_received
-- WHEN NOT MATCHED: insert new SKU

MERGE INTO {{zone_name}}.iceberg_demos.warehouse_inventory t
USING (VALUES
    -- Portland shipment (6 existing + 2 new)
    ('WH-P001', 'portland',  'Industrial Bolt M10',       250,  100, 0.45,  '2024-02-15'),
    ('WH-P003', 'portland',  'Copper Wire 14ga (100ft)',   40,  20,  24.99, '2024-02-15'),
    ('WH-P005', 'portland',  'LED Panel 2x4',              30,  15,  45.00, '2024-02-15'),
    ('WH-P007', 'portland',  'Fiberglass Insulation R30',  100, 50,  32.50, '2024-02-15'),
    ('WH-P009', 'portland',  'Rubber Gasket Set',          200, 80,  6.99,  '2024-02-15'),
    ('WH-P011', 'portland',  'Circuit Breaker 20A',        90,  40,  12.75, '2024-02-15'),
    ('WH-P013', 'portland',  'Epoxy Resin 1gal',           75,  20,  28.99, '2024-02-15'),
    ('WH-P014', 'portland',  'Cable Tie 12" (100pk)',      400, 80,  3.99,  '2024-02-15'),
    -- Dallas shipment (6 existing + 2 new)
    ('WH-D001', 'dallas',   'Industrial Bolt M10',        200,  100, 0.48,  '2024-02-16'),
    ('WH-D003', 'dallas',   'Copper Wire 14ga (100ft)',    35,  20,  25.50, '2024-02-16'),
    ('WH-D005', 'dallas',   'LED Panel 2x4',               25,  15,  46.00, '2024-02-16'),
    ('WH-D007', 'dallas',   'Fiberglass Insulation R30',   80,  50,  33.00, '2024-02-16'),
    ('WH-D009', 'dallas',   'Rubber Gasket Set',           150, 80,  7.25,  '2024-02-16'),
    ('WH-D011', 'dallas',   'Circuit Breaker 20A',         80,  40,  13.00, '2024-02-16'),
    ('WH-D013', 'dallas',   'Epoxy Resin 1gal',            60,  20,  29.50, '2024-02-16'),
    ('WH-D014', 'dallas',   'Cable Tie 12" (100pk)',       350, 80,  4.10,  '2024-02-16'),
    -- Charlotte shipment (6 existing + 2 new)
    ('WH-C001', 'charlotte', 'Industrial Bolt M10',       220,  100, 0.46,  '2024-02-17'),
    ('WH-C003', 'charlotte', 'Copper Wire 14ga (100ft)',    45,  20,  24.50, '2024-02-17'),
    ('WH-C005', 'charlotte', 'LED Panel 2x4',               35,  15,  44.50, '2024-02-17'),
    ('WH-C007', 'charlotte', 'Fiberglass Insulation R30',  110, 50,  31.99, '2024-02-17'),
    ('WH-C009', 'charlotte', 'Rubber Gasket Set',          180, 80,  6.75,  '2024-02-17'),
    ('WH-C011', 'charlotte', 'Circuit Breaker 20A',         85,  40,  12.50, '2024-02-17'),
    ('WH-C013', 'charlotte', 'Epoxy Resin 1gal',            65,  20,  28.75, '2024-02-17'),
    ('WH-C014', 'charlotte', 'Cable Tie 12" (100pk)',      380, 80,  3.85,  '2024-02-17')
) AS source(sku, warehouse, product_name, quantity_on_hand, reorder_point, unit_cost, last_received)
ON t.sku = source.sku
WHEN MATCHED THEN UPDATE SET
    t.quantity_on_hand = t.quantity_on_hand + source.quantity_on_hand,
    t.last_received = source.last_received
WHEN NOT MATCHED THEN INSERT (sku, warehouse, product_name, quantity_on_hand, reorder_point, unit_cost, last_received)
    VALUES (source.sku, source.warehouse, source.product_name, source.quantity_on_hand, source.reorder_point, source.unit_cost, source.last_received);


-- ============================================================================
-- Query 3: Post-Shipment Row Count
-- ============================================================================
-- 36 + 6 new = 42 SKUs

ASSERT ROW_COUNT = 42
SELECT * FROM {{zone_name}}.iceberg_demos.warehouse_inventory ORDER BY sku;


-- ============================================================================
-- Query 4: Post-Shipment Per-Warehouse Summary
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sku_count = 14 WHERE warehouse = 'charlotte'
ASSERT VALUE sku_count = 14 WHERE warehouse = 'dallas'
ASSERT VALUE sku_count = 14 WHERE warehouse = 'portland'
ASSERT VALUE inventory_value = 35825.05 WHERE warehouse = 'charlotte'
ASSERT VALUE inventory_value = 32468.00 WHERE warehouse = 'dallas'
ASSERT VALUE inventory_value = 36498.05 WHERE warehouse = 'portland'
SELECT
    warehouse,
    COUNT(*) AS sku_count,
    SUM(quantity_on_hand) AS total_qty,
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS inventory_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- Query 5: Verify Specific Quantity Additions
-- ============================================================================
-- Spot-check that received quantities were added to existing on-hand.

ASSERT ROW_COUNT = 6
ASSERT VALUE quantity_on_hand = 750 WHERE sku = 'WH-P001'
ASSERT VALUE quantity_on_hand = 120 WHERE sku = 'WH-P003'
ASSERT VALUE quantity_on_hand = 650 WHERE sku = 'WH-D001'
ASSERT VALUE quantity_on_hand = 105 WHERE sku = 'WH-D003'
ASSERT VALUE quantity_on_hand = 700 WHERE sku = 'WH-C001'
ASSERT VALUE quantity_on_hand = 135 WHERE sku = 'WH-C003'
SELECT
    sku,
    warehouse,
    quantity_on_hand,
    last_received
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
WHERE sku IN ('WH-P001', 'WH-P003', 'WH-D001', 'WH-D003', 'WH-C001', 'WH-C003')
ORDER BY sku;


-- ============================================================================
-- Query 6: Post-Shipment Total Inventory Value
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_value = 104791.10
ASSERT VALUE total_quantity = 16665
SELECT
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS total_value,
    SUM(quantity_on_hand) AS total_quantity
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- LEARN: MERGE 2 — Inventory Audit (Version 3 / Snapshot 3)
-- ============================================================================
-- Annual audit results: 6 SKUs discontinued (DELETE), 6 SKUs have
-- quantity corrections (UPDATE with corrected counts).
-- Discontinued: Silicone Sealant and Stainless Hinge across all warehouses.
-- Corrections: Steel Washer and Concrete Anchor quantities adjusted.

MERGE INTO {{zone_name}}.iceberg_demos.warehouse_inventory t
USING (VALUES
    -- Discontinued items (DELETE) — Silicone Sealant
    ('WH-P012', 'portland',  'Silicone Sealant 10oz',      0,   0,   4.50,  'discontinued'),
    ('WH-D012', 'dallas',   'Silicone Sealant 10oz',       0,   0,   4.75,  'discontinued'),
    ('WH-C012', 'charlotte', 'Silicone Sealant 10oz',      0,   0,   4.25,  'discontinued'),
    -- Discontinued items (DELETE) — Stainless Hinge
    ('WH-P008', 'portland',  'Stainless Hinge 4"',         0,   0,   3.25,  'discontinued'),
    ('WH-D008', 'dallas',   'Stainless Hinge 4"',          0,   0,   3.40,  'discontinued'),
    ('WH-C008', 'charlotte', 'Stainless Hinge 4"',         0,   0,   3.15,  'discontinued'),
    -- Audit corrections — Steel Washer (shrinkage adjustment)
    ('WH-P002', 'portland',  'Steel Washer 3/8"',          1150, 200, 0.12,  '2024-03-01'),
    ('WH-D002', 'dallas',   'Steel Washer 3/8"',           960,  200, 0.13,  '2024-03-01'),
    ('WH-C002', 'charlotte', 'Steel Washer 3/8"',          1060, 200, 0.11,  '2024-03-01'),
    -- Audit corrections — Concrete Anchor (shrinkage adjustment)
    ('WH-P006', 'portland',  'Concrete Anchor 1/2"',       870,  150, 0.89,  '2024-03-01'),
    ('WH-D006', 'dallas',   'Concrete Anchor 1/2"',        780,  150, 0.92,  '2024-03-01'),
    ('WH-C006', 'charlotte', 'Concrete Anchor 1/2"',       820,  150, 0.88,  '2024-03-01')
) AS source(sku, warehouse, product_name, quantity_on_hand, reorder_point, unit_cost, last_received)
ON t.sku = source.sku
WHEN MATCHED AND source.last_received = 'discontinued' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    t.quantity_on_hand = source.quantity_on_hand,
    t.last_received = source.last_received
WHEN NOT MATCHED THEN INSERT (sku, warehouse, product_name, quantity_on_hand, reorder_point, unit_cost, last_received)
    VALUES (source.sku, source.warehouse, source.product_name, source.quantity_on_hand, source.reorder_point, source.unit_cost, source.last_received);


-- ============================================================================
-- Query 7: Post-Audit Row Count
-- ============================================================================
-- 42 - 6 discontinued = 36 SKUs

ASSERT ROW_COUNT = 36
SELECT * FROM {{zone_name}}.iceberg_demos.warehouse_inventory ORDER BY sku;


-- ============================================================================
-- Query 8: Discontinued SKUs Removed
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
WHERE sku IN ('WH-P012', 'WH-D012', 'WH-C012', 'WH-P008', 'WH-D008', 'WH-C008');


-- ============================================================================
-- Query 9: Post-Audit Per-Warehouse Summary
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sku_count = 12 WHERE warehouse = 'charlotte'
ASSERT VALUE sku_count = 12 WHERE warehouse = 'dallas'
ASSERT VALUE sku_count = 12 WHERE warehouse = 'portland'
ASSERT VALUE inventory_value = 32321.25 WHERE warehouse = 'charlotte'
ASSERT VALUE inventory_value = 28811.90 WHERE warehouse = 'dallas'
ASSERT VALUE inventory_value = 32627.85 WHERE warehouse = 'portland'
SELECT
    warehouse,
    COUNT(*) AS sku_count,
    SUM(quantity_on_hand) AS total_qty,
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS inventory_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- Query 10: Verify Audit Corrections Applied
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE quantity_on_hand = 1150 WHERE sku = 'WH-P002'
ASSERT VALUE quantity_on_hand = 960 WHERE sku = 'WH-D002'
ASSERT VALUE quantity_on_hand = 1060 WHERE sku = 'WH-C002'
ASSERT VALUE quantity_on_hand = 870 WHERE sku = 'WH-P006'
ASSERT VALUE quantity_on_hand = 780 WHERE sku = 'WH-D006'
ASSERT VALUE quantity_on_hand = 820 WHERE sku = 'WH-C006'
SELECT
    sku,
    warehouse,
    product_name,
    quantity_on_hand,
    last_received
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
WHERE sku IN ('WH-P002', 'WH-D002', 'WH-C002', 'WH-P006', 'WH-D006', 'WH-C006')
ORDER BY sku;


-- ============================================================================
-- Query 11: Time Travel — SKU Counts Across All Versions
-- ============================================================================
-- V1: 36 (seed), V2: 42 (shipment), V3: 36 (audit with discontinuations)

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_count = 36
ASSERT VALUE v2_count = 42
ASSERT VALUE v3_count = 36
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.warehouse_inventory VERSION AS OF 1) AS v1_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.warehouse_inventory VERSION AS OF 2) AS v2_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.warehouse_inventory) AS v3_count;


-- ============================================================================
-- Query 12: Time Travel — Inventory Value Across Versions
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_value = 70762.90
ASSERT VALUE v2_value = 104791.10
ASSERT VALUE v3_value = 93761.00
SELECT
    ROUND((SELECT SUM(quantity_on_hand * unit_cost) FROM {{zone_name}}.iceberg_demos.warehouse_inventory VERSION AS OF 1), 2) AS v1_value,
    ROUND((SELECT SUM(quantity_on_hand * unit_cost) FROM {{zone_name}}.iceberg_demos.warehouse_inventory VERSION AS OF 2), 2) AS v2_value,
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS v3_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- Query 13: Version History
-- ============================================================================
-- V1: Seed 36 SKUs
-- V2: MERGE shipment (18 quantity updates + 6 new SKUs)
-- V3: MERGE audit (6 discontinuations + 6 quantity corrections)

ASSERT WARNING ROW_COUNT >= 3
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check after both MERGE operations.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_skus = 36
ASSERT VALUE warehouse_count = 3
ASSERT VALUE total_inventory_value = 93761.00
ASSERT VALUE total_quantity = 13755
SELECT
    COUNT(*) AS total_skus,
    COUNT(DISTINCT warehouse) AS warehouse_count,
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS total_inventory_value,
    SUM(quantity_on_hand) AS total_quantity
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents partitioned data after two MERGE
-- operations (shipment receiving + inventory audit with discontinuations).
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg
USING ICEBERG
LOCATION '{{data_path}}/warehouse_inventory';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg;


-- ============================================================================
-- Iceberg Verify 1: Row Count — 36 SKUs After Both MERGEs
-- ============================================================================

ASSERT ROW_COUNT = 36
SELECT * FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg ORDER BY sku;


-- ============================================================================
-- Iceberg Verify 2: Per-Warehouse Counts — Reflect Discontinuations
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE sku_count = 12 WHERE warehouse = 'charlotte'
ASSERT VALUE sku_count = 12 WHERE warehouse = 'dallas'
ASSERT VALUE sku_count = 12 WHERE warehouse = 'portland'
SELECT
    warehouse,
    COUNT(*) AS sku_count
FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- Iceberg Verify 3: Grand Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_skus = 36
ASSERT VALUE total_inventory_value = 93761.00
ASSERT VALUE total_quantity = 13755
SELECT
    COUNT(*) AS total_skus,
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS total_inventory_value,
    SUM(quantity_on_hand) AS total_quantity
FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg;
