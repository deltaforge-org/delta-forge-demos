-- ============================================================================
-- Iceberg V1 Warehouse Inventory — Queries
-- ============================================================================
-- Demonstrates native Iceberg format-version 1 table reading: schema
-- inference from v1 metadata, manifest-based file discovery, aggregations,
-- filtering, and inventory analytics. All queries are read-only.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Total Row Count
-- ============================================================================
-- Verifies that DeltaForge discovered the Parquet data file via the
-- Iceberg v1 manifest chain (metadata.json → manifest list → manifest → file).

ASSERT ROW_COUNT = 489
SELECT * FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- Query 2: Schema Inference from Iceberg V1 Metadata
-- ============================================================================
-- The schema comes from the v1 metadata.json (not Parquet footers). This
-- query exercises all 10 columns to prove correct Iceberg→Arrow type mapping.

ASSERT ROW_COUNT = 489
ASSERT VALUE sku IS NOT NULL WHERE sku = 'SKU-00001'
SELECT
    sku,
    product_name,
    category,
    warehouse,
    quantity_on_hand,
    reorder_point,
    unit_cost,
    last_restock_date,
    supplier,
    aisle_location
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
ORDER BY sku;


-- ============================================================================
-- Query 3: Per-Warehouse Row Counts
-- ============================================================================
-- Three warehouses with varying item counts.

ASSERT ROW_COUNT = 3
ASSERT VALUE item_count = 159 WHERE warehouse = 'Charlotte-NC'
ASSERT VALUE item_count = 166 WHERE warehouse = 'Dallas-TX'
ASSERT VALUE item_count = 164 WHERE warehouse = 'Portland-OR'
SELECT
    warehouse,
    COUNT(*) AS item_count
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- Query 4: Per-Category Row Counts
-- ============================================================================
-- Five product categories across all warehouses.

ASSERT ROW_COUNT = 5
ASSERT VALUE item_count = 100 WHERE category = 'Apparel'
ASSERT VALUE item_count = 99 WHERE category = 'Electronics'
ASSERT VALUE item_count = 97 WHERE category = 'Food-Bev'
ASSERT VALUE item_count = 94 WHERE category = 'Furniture'
ASSERT VALUE item_count = 99 WHERE category = 'Industrial'
SELECT
    category,
    COUNT(*) AS item_count
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 5: Total Inventory Value
-- ============================================================================
-- Aggregation across all 489 SKUs: SUM(quantity_on_hand * unit_cost).
-- Proof value computed independently from the seed data.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_value = 17554271.58
SELECT
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS total_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;


-- ============================================================================
-- Query 6: Per-Warehouse Inventory Value
-- ============================================================================
-- Proves correct aggregation grouped by warehouse.

ASSERT ROW_COUNT = 3
ASSERT VALUE warehouse_value = 5047746.44 WHERE warehouse = 'Charlotte-NC'
ASSERT VALUE warehouse_value = 6234098.71 WHERE warehouse = 'Dallas-TX'
ASSERT VALUE warehouse_value = 6272426.43 WHERE warehouse = 'Portland-OR'
SELECT
    warehouse,
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS warehouse_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- Query 7: Items Below Reorder Point
-- ============================================================================
-- Identifies SKUs where quantity_on_hand < reorder_point — a real-world
-- warehouse alert. Exercises predicate evaluation on integer columns.

ASSERT ROW_COUNT = 56
SELECT
    sku,
    product_name,
    warehouse,
    category,
    quantity_on_hand,
    reorder_point
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
WHERE quantity_on_hand < reorder_point
ORDER BY quantity_on_hand ASC;


-- ============================================================================
-- Query 8: Below Reorder by Warehouse
-- ============================================================================
-- Per-warehouse count of items needing restocking.

ASSERT ROW_COUNT = 3
ASSERT VALUE reorder_needed = 21 WHERE warehouse = 'Charlotte-NC'
ASSERT VALUE reorder_needed = 15 WHERE warehouse = 'Dallas-TX'
ASSERT VALUE reorder_needed = 20 WHERE warehouse = 'Portland-OR'
SELECT
    warehouse,
    COUNT(*) AS reorder_needed
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
WHERE quantity_on_hand < reorder_point
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- Query 9: Average Unit Cost by Category
-- ============================================================================
-- Proves correct floating-point aggregation grouped by string column.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_cost = 137.18 WHERE category = 'Apparel'
ASSERT VALUE avg_cost = 148.05 WHERE category = 'Electronics'
ASSERT VALUE avg_cost = 144.74 WHERE category = 'Food-Bev'
ASSERT VALUE avg_cost = 137.43 WHERE category = 'Furniture'
ASSERT VALUE avg_cost = 150.28 WHERE category = 'Industrial'
SELECT
    category,
    ROUND(AVG(unit_cost), 2) AS avg_cost
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 10: Supplier Distribution
-- ============================================================================
-- Item counts per supplier across all warehouses.

ASSERT ROW_COUNT = 5
ASSERT VALUE item_count = 88 WHERE supplier = 'Acme Corp'
ASSERT VALUE item_count = 101 WHERE supplier = 'EcoSupply'
ASSERT VALUE item_count = 82 WHERE supplier = 'GlobalTrade'
ASSERT VALUE item_count = 108 WHERE supplier = 'PrimeParts'
ASSERT VALUE item_count = 110 WHERE supplier = 'QuickShip'
SELECT
    supplier,
    COUNT(*) AS item_count
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
GROUP BY supplier
ORDER BY supplier;


-- ============================================================================
-- Query 11: High-Value Inventory Items
-- ============================================================================
-- Items where quantity_on_hand * unit_cost > 10,000. Exercises compound
-- predicate evaluation with arithmetic expressions.

ASSERT ROW_COUNT = 372
SELECT
    sku,
    product_name,
    warehouse,
    quantity_on_hand,
    unit_cost,
    ROUND(quantity_on_hand * unit_cost, 2) AS line_value
FROM {{zone_name}}.iceberg_demos.warehouse_inventory
WHERE quantity_on_hand * unit_cost > 10000
ORDER BY line_value DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, grand totals, and key invariants.
-- A user who runs only this query can verify the Iceberg v1 reader works.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 489
ASSERT VALUE total_value = 17554271.58
ASSERT VALUE warehouse_count = 3
ASSERT VALUE category_count = 5
ASSERT VALUE supplier_count = 5
ASSERT VALUE below_reorder = 56
SELECT
    COUNT(*) AS total_rows,
    ROUND(SUM(quantity_on_hand * unit_cost), 2) AS total_value,
    COUNT(DISTINCT warehouse) AS warehouse_count,
    COUNT(DISTINCT category) AS category_count,
    COUNT(DISTINCT supplier) AS supplier_count,
    SUM(CASE WHEN quantity_on_hand < reorder_point THEN 1 ELSE 0 END) AS below_reorder
FROM {{zone_name}}.iceberg_demos.warehouse_inventory;
