-- ============================================================================
-- Demo: ORC Warehouse Inventory — Stock Level Analytics
-- ============================================================================
-- Proves window functions work correctly on ORC-backed external tables with
-- mixed numeric types. Covers ROW_NUMBER, RANK, LAG/LEAD, NTILE, and
-- running totals via SUM OVER.

-- ============================================================================
-- Query 1: Full Scan — 100 inventory items across 2 warehouses
-- ============================================================================

ASSERT ROW_COUNT = 100
SELECT *
FROM {{zone_name}}.orc_inventory.stock;

-- ============================================================================
-- Query 2: ROW_NUMBER — rank items within each category by quantity DESC
-- ============================================================================
-- Each category has 20 items (10 products × 2 warehouses).
-- We verify the top-ranked item per category.

ASSERT ROW_COUNT = 100
ASSERT VALUE rn = 1 WHERE sku_id = 'SKU-1027'
ASSERT VALUE rn = 1 WHERE sku_id = 'SKU-1007'
ASSERT VALUE rn = 1 WHERE sku_id = 'SKU-1038'
ASSERT VALUE rn = 1 WHERE sku_id = 'SKU-1067'
ASSERT VALUE rn = 1 WHERE sku_id = 'SKU-1098'
SELECT sku_id, warehouse, category, product_name, quantity_on_hand,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY quantity_on_hand DESC) AS rn
FROM {{zone_name}}.orc_inventory.stock;

-- ============================================================================
-- Query 3: RANK — rank items by unit_cost within each warehouse
-- ============================================================================
-- Shows tied ranks when costs are identical (unlikely with float64 but
-- demonstrates RANK vs ROW_NUMBER semantics).

ASSERT ROW_COUNT = 100
SELECT sku_id, warehouse, category, product_name, unit_cost,
       RANK() OVER (PARTITION BY warehouse ORDER BY unit_cost DESC) AS cost_rank
FROM {{zone_name}}.orc_inventory.stock;

-- ============================================================================
-- Query 4: LAG/LEAD — previous and next product quantity within category
-- ============================================================================
-- Ordered by product_name within WH-NORTH Electronics.
-- Cable (qty=453) is first → LAG is NULL, LEAD is 338 (Charger).
-- Webcam (qty=47) is last → LAG is 235 (USB Hub), LEAD is NULL.

ASSERT ROW_COUNT = 10
ASSERT VALUE prev_qty IS NULL WHERE product_name = 'Cable'
ASSERT VALUE next_qty = 338 WHERE product_name = 'Cable'
ASSERT VALUE prev_qty = 235 WHERE product_name = 'Webcam'
ASSERT VALUE next_qty IS NULL WHERE product_name = 'Webcam'
SELECT product_name, quantity_on_hand,
       LAG(quantity_on_hand) OVER (ORDER BY product_name) AS prev_qty,
       LEAD(quantity_on_hand) OVER (ORDER BY product_name) AS next_qty
FROM {{zone_name}}.orc_inventory.stock
WHERE warehouse = 'WH-NORTH' AND category = 'Electronics'
ORDER BY product_name;

-- ============================================================================
-- Query 5: NTILE — divide all items into 4 cost quartiles
-- ============================================================================
-- 100 items → 25 per quartile. Verify quartile boundaries.

ASSERT ROW_COUNT = 4
ASSERT VALUE item_count = 25 WHERE cost_quartile = 1
ASSERT VALUE item_count = 25 WHERE cost_quartile = 4
SELECT cost_quartile,
       COUNT(*) AS item_count,
       ROUND(MIN(unit_cost), 2) AS min_cost,
       ROUND(MAX(unit_cost), 2) AS max_cost
FROM (
    SELECT unit_cost,
           NTILE(4) OVER (ORDER BY unit_cost) AS cost_quartile
    FROM {{zone_name}}.orc_inventory.stock
) q
GROUP BY cost_quartile
ORDER BY cost_quartile;

-- ============================================================================
-- Query 6: Running Total — cumulative quantity within each category
-- ============================================================================
-- SUM(quantity_on_hand) OVER with ORDER BY gives a running total.
-- The last row in each partition should equal the category total.

ASSERT ROW_COUNT = 5
ASSERT VALUE category_total = 5080 WHERE category = 'Clothing'
ASSERT VALUE category_total = 3787 WHERE category = 'Electronics'
ASSERT VALUE category_total = 5029 WHERE category = 'Food'
ASSERT VALUE category_total = 3978 WHERE category = 'Furniture'
ASSERT VALUE category_total = 5938 WHERE category = 'Tools'
SELECT category, SUM(quantity_on_hand) AS category_total
FROM {{zone_name}}.orc_inventory.stock
GROUP BY category
ORDER BY category;

-- ============================================================================
-- Query 7: Combined — CTE with window + aggregation for reorder alerts
-- ============================================================================
-- Identifies items below reorder point, ranked by urgency within category.

ASSERT ROW_COUNT = 15
SELECT sku_id, warehouse, category, product_name,
       quantity_on_hand, reorder_point,
       reorder_point - quantity_on_hand AS deficit,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY reorder_point - quantity_on_hand DESC) AS urgency_rank
FROM {{zone_name}}.orc_inventory.stock
WHERE quantity_on_hand < reorder_point
ORDER BY category, urgency_rank;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_rows_100'
ASSERT VALUE result = 'PASS' WHERE check_name = 'active_items_90'
ASSERT VALUE result = 'PASS' WHERE check_name = 'below_reorder_15'
ASSERT VALUE result = 'PASS' WHERE check_name = 'null_restock_14'
SELECT check_name, result FROM (

    SELECT 'total_rows_100' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_inventory.stock) = 100
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'active_items_90' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_inventory.stock WHERE is_active = true
           ) = 90 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'below_reorder_15' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_inventory.stock
               WHERE quantity_on_hand < reorder_point
           ) = 15 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'null_restock_14' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_inventory.stock
               WHERE last_restock_date IS NULL
           ) = 14 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'distinct_products_50' AS check_name,
           CASE WHEN (
               SELECT COUNT(DISTINCT product_name) FROM {{zone_name}}.orc_inventory.stock
           ) = 50 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
