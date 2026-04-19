-- ============================================================================
-- Iceberg V3 UniForm — Supply Chain Inventory MERGE Sync — Queries
-- ============================================================================
-- Demonstrates MERGE INTO on a V3 UniForm Delta table. Each MERGE generates
-- deletion vectors (for updated rows) and new data files (for inserts),
-- with Iceberg V3 metadata tracking each snapshot.
--
-- MERGE Round 1: Supplier shipment to WH-EAST
--   - MATCHED: Update quantities for 3 existing SKUs
--   - NOT MATCHED: Insert 2 new products
--
-- MERGE Round 2: Price adjustment sync for WH-WEST
--   - MATCHED: Update unit prices for 3 SKUs
--   - NOT MATCHED: Insert 1 new product
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — 30 Items Across 3 Warehouses
-- ============================================================================

ASSERT ROW_COUNT = 30
ASSERT VALUE product_name = 'Wireless Mouse' WHERE item_id = 1
ASSERT VALUE quantity = 150 WHERE item_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
ORDER BY item_id;


-- ============================================================================
-- Query 2: Baseline Per-Warehouse Totals
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE item_count = 10 WHERE warehouse = 'WH-CENTRAL'
ASSERT VALUE total_qty = 608 WHERE warehouse = 'WH-CENTRAL'
ASSERT VALUE inventory_value = 43943.92 WHERE warehouse = 'WH-CENTRAL'
ASSERT VALUE item_count = 10 WHERE warehouse = 'WH-EAST'
ASSERT VALUE total_qty = 787 WHERE warehouse = 'WH-EAST'
ASSERT VALUE inventory_value = 58092.13 WHERE warehouse = 'WH-EAST'
ASSERT VALUE item_count = 10 WHERE warehouse = 'WH-WEST'
ASSERT VALUE total_qty = 978 WHERE warehouse = 'WH-WEST'
ASSERT VALUE inventory_value = 73190.22 WHERE warehouse = 'WH-WEST'
SELECT
    warehouse,
    COUNT(*) AS item_count,
    SUM(quantity) AS total_qty,
    ROUND(SUM(quantity * unit_price), 2) AS inventory_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- Query 3: Baseline Grand Totals
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_items = 30
ASSERT VALUE total_qty = 2373
ASSERT VALUE total_value = 175226.27
SELECT
    COUNT(*) AS total_items,
    SUM(quantity) AS total_qty,
    ROUND(SUM(quantity * unit_price), 2) AS total_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- MERGE Round 1: Supplier Shipment to WH-EAST
-- ============================================================================
-- Source data: 5 items from supplier feed. 3 match existing WH-EAST SKUs
-- (quantity update), 2 are new products (insert).

MERGE INTO {{zone_name}}.iceberg_demos.warehouse_inventory AS t
USING (
    SELECT * FROM (VALUES
        (1,  'WH-EAST', 'SKU-1001', 'Electronics', 'Wireless Mouse',     175, 24.99,  '2024-02-01'),
        (3,  'WH-EAST', 'SKU-1003', 'Office',      'Ergonomic Chair',    30,  299.99, '2024-02-01'),
        (7,  'WH-EAST', 'SKU-1007', 'Electronics', 'Mechanical Keyboard',110, 89.99,  '2024-02-01'),
        (31, 'WH-EAST', 'SKU-2001', 'Electronics', 'Wireless Earbuds',   200, 49.99,  '2024-02-01'),
        (32, 'WH-EAST', 'SKU-2002', 'Office',      'Desk Lamp LED',      85,  44.99,  '2024-02-01')
    ) AS s(item_id, warehouse, sku, category, product_name, quantity, unit_price, last_received)
) AS s
ON t.item_id = s.item_id
WHEN MATCHED THEN UPDATE SET
    quantity = s.quantity,
    last_received = s.last_received
WHEN NOT MATCHED THEN INSERT VALUES (
    s.item_id, s.warehouse, s.sku, s.category, s.product_name,
    s.quantity, s.unit_price, s.last_received
);


-- ============================================================================
-- Query 4: Post-Merge-1 — 32 Items (3 Updated, 2 Inserted)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_items = 32
SELECT COUNT(*) AS total_items
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- Query 5: Verify Updated Quantities in WH-EAST
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE quantity = 175 WHERE item_id = 1
ASSERT VALUE quantity = 30 WHERE item_id = 3
ASSERT VALUE quantity = 110 WHERE item_id = 7
SELECT item_id, sku, product_name, quantity, last_received
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
WHERE item_id IN (1, 3, 7)
ORDER BY item_id;


-- ============================================================================
-- Query 6: Verify New Products Inserted
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE product_name = 'Wireless Earbuds' WHERE item_id = 31
ASSERT VALUE quantity = 200 WHERE item_id = 31
ASSERT VALUE product_name = 'Desk Lamp LED' WHERE item_id = 32
ASSERT VALUE quantity = 85 WHERE item_id = 32
SELECT *
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
WHERE item_id IN (31, 32)
ORDER BY item_id;


-- ============================================================================
-- MERGE Round 2: Price Adjustment for WH-WEST
-- ============================================================================
-- Supplier raised prices on 3 items. Also stocking new Wireless Earbuds.

MERGE INTO {{zone_name}}.iceberg_demos.warehouse_inventory AS t
USING (
    SELECT * FROM (VALUES
        (11, 'WH-WEST', 'SKU-1001', 'Electronics', 'Wireless Mouse',   180, 26.99,  '2024-02-05'),
        (15, 'WH-WEST', 'SKU-1005', 'Accessories', 'Monitor Stand',    75,  84.99,  '2024-02-05'),
        (19, 'WH-WEST', 'SKU-1009', 'Accessories', 'Webcam HD',        130, 64.99,  '2024-02-05'),
        (33, 'WH-WEST', 'SKU-2001', 'Electronics', 'Wireless Earbuds', 160, 49.99,  '2024-02-05')
    ) AS s(item_id, warehouse, sku, category, product_name, quantity, unit_price, last_received)
) AS s
ON t.item_id = s.item_id
WHEN MATCHED THEN UPDATE SET
    unit_price = s.unit_price,
    last_received = s.last_received
WHEN NOT MATCHED THEN INSERT VALUES (
    s.item_id, s.warehouse, s.sku, s.category, s.product_name,
    s.quantity, s.unit_price, s.last_received
);


-- ============================================================================
-- Query 7: Post-Merge-2 — 33 Items Total
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_items = 33
SELECT COUNT(*) AS total_items
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- Query 8: Verify Price Updates in WH-WEST
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE unit_price = 26.99 WHERE item_id = 11
ASSERT VALUE unit_price = 84.99 WHERE item_id = 15
ASSERT VALUE unit_price = 64.99 WHERE item_id = 19
SELECT item_id, sku, product_name, unit_price, last_received
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
WHERE item_id IN (11, 15, 19)
ORDER BY item_id;


-- ============================================================================
-- Query 9: Final Per-Warehouse Summary
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE item_count = 10 WHERE warehouse = 'WH-CENTRAL'
ASSERT VALUE total_qty = 608 WHERE warehouse = 'WH-CENTRAL'
ASSERT VALUE inventory_value = 43943.92 WHERE warehouse = 'WH-CENTRAL'
ASSERT VALUE item_count = 12 WHERE warehouse = 'WH-EAST'
ASSERT VALUE total_qty = 1122 WHERE warehouse = 'WH-EAST'
ASSERT VALUE inventory_value = 75838.78 WHERE warehouse = 'WH-EAST'
ASSERT VALUE item_count = 11 WHERE warehouse = 'WH-WEST'
ASSERT VALUE total_qty = 1138 WHERE warehouse = 'WH-WEST'
ASSERT VALUE inventory_value = 82573.62 WHERE warehouse = 'WH-WEST'
SELECT
    warehouse,
    COUNT(*) AS item_count,
    SUM(quantity) AS total_qty,
    ROUND(SUM(quantity * unit_price), 2) AS inventory_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_items = 33
ASSERT VALUE total_qty = 2868
ASSERT VALUE total_value = 202356.32
ASSERT VALUE warehouse_count = 3
ASSERT VALUE distinct_skus = 12
SELECT
    COUNT(*) AS total_items,
    SUM(quantity) AS total_qty,
    ROUND(SUM(quantity * unit_price), 2) AS total_value,
    COUNT(DISTINCT warehouse) AS warehouse_count,
    COUNT(DISTINCT sku) AS distinct_skus
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- ICEBERG V3 READ-BACK VERIFICATION
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg
USING ICEBERG
LOCATION '{{data_subdir}}/warehouse_inventory';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg TO USER {{current_user}};


-- ============================================================================
-- Iceberg Verify 1: Row Count — 33 Items
-- ============================================================================

ASSERT ROW_COUNT = 33
SELECT * FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg ORDER BY item_id;


-- ============================================================================
-- Iceberg Verify 2: Spot-Check MERGE-Updated Row — SKU-1001 WH-EAST
-- ============================================================================
-- Quantity was updated from 150 to 175 by MERGE Round 1.

ASSERT ROW_COUNT = 1
ASSERT VALUE warehouse = 'WH-EAST' WHERE item_id = 1
ASSERT VALUE sku = 'SKU-1001' WHERE item_id = 1
ASSERT VALUE product_name = 'Wireless Mouse' WHERE item_id = 1
ASSERT VALUE quantity = 175 WHERE item_id = 1
ASSERT VALUE unit_price = 24.99 WHERE item_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg
WHERE item_id = 1;


-- ============================================================================
-- Iceberg Verify 3: Spot-Check MERGE-Inserted Row — Wireless Earbuds WH-EAST
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE warehouse = 'WH-EAST' WHERE item_id = 31
ASSERT VALUE product_name = 'Wireless Earbuds' WHERE item_id = 31
ASSERT VALUE quantity = 200 WHERE item_id = 31
ASSERT VALUE unit_price = 49.99 WHERE item_id = 31
SELECT *
FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg
WHERE item_id = 31;


-- ============================================================================
-- Iceberg Verify 4: Spot-Check Price-Updated Row — SKU-1001 WH-WEST
-- ============================================================================
-- Price was updated from 24.99 to 26.99 by MERGE Round 2.

ASSERT ROW_COUNT = 1
ASSERT VALUE warehouse = 'WH-WEST' WHERE item_id = 11
ASSERT VALUE quantity = 180 WHERE item_id = 11
ASSERT VALUE unit_price = 26.99 WHERE item_id = 11
SELECT *
FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg
WHERE item_id = 11;


-- ============================================================================
-- Iceberg Verify 5: Untouched Row — WH-CENTRAL Preserved
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE warehouse = 'WH-CENTRAL' WHERE item_id = 21
ASSERT VALUE sku = 'SKU-1001' WHERE item_id = 21
ASSERT VALUE quantity = 100 WHERE item_id = 21
ASSERT VALUE unit_price = 24.99 WHERE item_id = 21
SELECT *
FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg
WHERE item_id = 21;


-- ============================================================================
-- Iceberg Verify 6: Per-Warehouse Aggregates Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE item_count = 10 WHERE warehouse = 'WH-CENTRAL'
ASSERT VALUE total_qty = 608 WHERE warehouse = 'WH-CENTRAL'
ASSERT VALUE inventory_value = 43943.92 WHERE warehouse = 'WH-CENTRAL'
ASSERT VALUE item_count = 12 WHERE warehouse = 'WH-EAST'
ASSERT VALUE total_qty = 1122 WHERE warehouse = 'WH-EAST'
ASSERT VALUE inventory_value = 75838.78 WHERE warehouse = 'WH-EAST'
ASSERT VALUE item_count = 11 WHERE warehouse = 'WH-WEST'
ASSERT VALUE total_qty = 1138 WHERE warehouse = 'WH-WEST'
ASSERT VALUE inventory_value = 82573.62 WHERE warehouse = 'WH-WEST'
SELECT
    warehouse,
    COUNT(*) AS item_count,
    SUM(quantity) AS total_qty,
    ROUND(SUM(quantity * unit_price), 2) AS inventory_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- Iceberg Verify 7: Grand Totals Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_items = 33
ASSERT VALUE total_qty = 2868
ASSERT VALUE total_value = 202356.32
SELECT
    COUNT(*) AS total_items,
    SUM(quantity) AS total_qty,
    ROUND(SUM(quantity * unit_price), 2) AS total_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg;
