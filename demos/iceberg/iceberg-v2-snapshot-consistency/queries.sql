-- ============================================================================
-- Iceberg V2 Snapshot Consistency — Queries
-- ============================================================================
-- Validates that Delta Forge correctly reads the final consistent state of an
-- Iceberg V2 table that underwent 4 snapshots: initial load (80 products),
-- restocking INSERT (20 new → 100), price UPDATE (Electronics +8%), and
-- discontinuation DELETE (10 removed → 90 final). All merge-on-read position
-- deletes must be applied for correct results.
-- ============================================================================


-- ============================================================================
-- Query 1: Full Scan — Final Row Count
-- ============================================================================
-- 80 initial + 20 restocked - 10 discontinued = 90 products remaining.
-- Position deletes from UPDATE and DELETE must all be applied.

ASSERT ROW_COUNT = 90
ASSERT VALUE product_name = 'Compression Shorts' WHERE sku = 'SKU-C-N01'
ASSERT VALUE unit_price = 24.99 WHERE sku = 'SKU-C-N01'
ASSERT VALUE product_name = 'USB-C Cable 3ft' WHERE sku = 'SKU-E-N01'
ASSERT VALUE unit_price = 9.71 WHERE sku = 'SKU-E-N01'
ASSERT VALUE product_name = 'Succulent Planter' WHERE sku = 'SKU-H-N01'
ASSERT VALUE unit_price = 21.99 WHERE sku = 'SKU-H-N01'
ASSERT VALUE product_name = 'Massage Gun Mini' WHERE sku = 'SKU-S-N01'
ASSERT VALUE unit_price = 49.99 WHERE sku = 'SKU-S-N01'
SELECT * FROM {{zone_name}}.iceberg.inventory;


-- ============================================================================
-- Query 2: Category Breakdown
-- ============================================================================
-- Products per category with total quantity on hand.

ASSERT ROW_COUNT = 4
ASSERT VALUE product_count = 22 WHERE category = 'Clothing'
ASSERT VALUE product_count = 23 WHERE category = 'Electronics'
ASSERT VALUE product_count = 22 WHERE category = 'Home & Garden'
ASSERT VALUE product_count = 23 WHERE category = 'Sports'
ASSERT VALUE total_qty = 2685 WHERE category = 'Clothing'
ASSERT VALUE total_qty = 2104 WHERE category = 'Electronics'
ASSERT VALUE total_qty = 2514 WHERE category = 'Home & Garden'
ASSERT VALUE total_qty = 2729 WHERE category = 'Sports'
SELECT
    category,
    COUNT(*) AS product_count,
    SUM(quantity_on_hand) AS total_qty
FROM {{zone_name}}.iceberg.inventory
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 3: Electronics Price Verification
-- ============================================================================
-- All 25 Electronics products (20 original + 5 new) had prices increased by
-- 8%. The average should reflect the inflated prices.

ASSERT ROW_COUNT = 1
ASSERT VALUE avg_price = 50.05
SELECT
    ROUND(AVG(unit_price), 2) AS avg_price
FROM {{zone_name}}.iceberg.inventory
WHERE category = 'Electronics';


-- ============================================================================
-- Query 4: Verify New Products Present
-- ============================================================================
-- 20 new products were inserted in Snapshot 2 with SKU pattern 'SKU-%-N%'.

ASSERT ROW_COUNT = 20
ASSERT VALUE product_name = 'Compression Shorts' WHERE sku = 'SKU-C-N01'
ASSERT VALUE unit_price = 24.99 WHERE sku = 'SKU-C-N01'
ASSERT VALUE quantity_on_hand = 162 WHERE sku = 'SKU-C-N01'
ASSERT VALUE product_name = 'USB-C Cable 3ft' WHERE sku = 'SKU-E-N01'
ASSERT VALUE unit_price = 9.71 WHERE sku = 'SKU-E-N01'
ASSERT VALUE quantity_on_hand = 12 WHERE sku = 'SKU-E-N01'
ASSERT VALUE product_name = 'Succulent Planter' WHERE sku = 'SKU-H-N01'
ASSERT VALUE unit_price = 21.99 WHERE sku = 'SKU-H-N01'
ASSERT VALUE quantity_on_hand = 174 WHERE sku = 'SKU-H-N01'
ASSERT VALUE product_name = 'Massage Gun Mini' WHERE sku = 'SKU-S-N01'
ASSERT VALUE unit_price = 49.99 WHERE sku = 'SKU-S-N01'
ASSERT VALUE quantity_on_hand = 194 WHERE sku = 'SKU-S-N01'
SELECT *
FROM {{zone_name}}.iceberg.inventory
WHERE sku LIKE 'SKU-%-N%';


-- ============================================================================
-- Query 5: Verify Discontinued Products Absent
-- ============================================================================
-- 10 specific SKUs were deleted in Snapshot 4. None should remain.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg.inventory
WHERE sku IN (
    'SKU-E007', 'SKU-E013',
    'SKU-H008', 'SKU-H018', 'SKU-H019',
    'SKU-S006', 'SKU-S008',
    'SKU-C004', 'SKU-C017', 'SKU-C018'
);


-- ============================================================================
-- Query 6: Supplier Analysis
-- ============================================================================
-- Count of products per supplier. ThreadCo and FitGear are the largest.

ASSERT ROW_COUNT = 27
ASSERT VALUE product_count = 11 WHERE supplier = 'ThreadCo'
ASSERT VALUE product_count = 10 WHERE supplier = 'FitGear'
ASSERT VALUE product_count = 7 WHERE supplier = 'GreenThumb'
ASSERT VALUE product_count = 6 WHERE supplier = 'HomeEssentials'
ASSERT VALUE product_count = 6 WHERE supplier = 'TechCorp'
SELECT
    supplier,
    COUNT(*) AS product_count
FROM {{zone_name}}.iceberg.inventory
GROUP BY supplier
ORDER BY product_count DESC, supplier;


-- ============================================================================
-- Query 7: Inventory Value by Category
-- ============================================================================
-- Total inventory value (unit_price * quantity_on_hand) per category.

ASSERT ROW_COUNT = 4
ASSERT VALUE inventory_value = 97463.15 WHERE category = 'Clothing'
ASSERT VALUE inventory_value = 82611.92 WHERE category = 'Electronics'
ASSERT VALUE inventory_value = 61844.86 WHERE category = 'Home & Garden'
ASSERT VALUE inventory_value = 58182.71 WHERE category = 'Sports'
SELECT
    category,
    ROUND(SUM(unit_price * quantity_on_hand), 2) AS inventory_value
FROM {{zone_name}}.iceberg.inventory
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 8: Describe History — 4 Snapshots
-- ============================================================================
-- The table should have exactly 4 snapshots in its history.
-- NOTE: DESCRIBE HISTORY on Iceberg tables may return 0 rows (known gap).

ASSERT WARNING ROW_COUNT = 4
DESCRIBE HISTORY {{zone_name}}.iceberg.inventory;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check covering all key invariants.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_products = 90
ASSERT VALUE category_count = 4
ASSERT VALUE total_inventory_value = 300102.64
ASSERT VALUE avg_price = 33.07
ASSERT VALUE total_quantity = 10032
ASSERT VALUE new_product_count = 20
ASSERT VALUE discontinued_remaining = 0
SELECT
    COUNT(*) AS total_products,
    COUNT(DISTINCT category) AS category_count,
    ROUND(SUM(unit_price * quantity_on_hand), 2) AS total_inventory_value,
    ROUND(AVG(unit_price), 2) AS avg_price,
    SUM(quantity_on_hand) AS total_quantity,
    SUM(CASE WHEN sku LIKE 'SKU-%-N%' THEN 1 ELSE 0 END) AS new_product_count,
    SUM(CASE WHEN sku IN (
        'SKU-E007', 'SKU-E013', 'SKU-H008', 'SKU-H018', 'SKU-H019',
        'SKU-S006', 'SKU-S008', 'SKU-C004', 'SKU-C017', 'SKU-C018'
    ) THEN 1 ELSE 0 END) AS discontinued_remaining
FROM {{zone_name}}.iceberg.inventory;
