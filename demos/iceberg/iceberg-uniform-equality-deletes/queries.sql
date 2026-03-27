-- ============================================================================
-- Iceberg UniForm Equality Deletes — Queries
-- ============================================================================
-- Full-cycle verification: Delta table writes with equality delete mode,
-- then Iceberg external table reads to confirm deletes are applied.
-- Products 2, 5, and 8 were deleted in setup, leaving 7 of 10 products.
-- ============================================================================


-- ============================================================================
-- Query 1: Delta Table — Post-Delete Row Count
-- ============================================================================
-- Verify the Delta table correctly shows 7 rows after deleting 3 products.

ASSERT ROW_COUNT = 7
SELECT * FROM {{zone_name}}.eq_del_demo.products;


-- ============================================================================
-- Query 2: Iceberg External Table — Post-Delete Row Count
-- ============================================================================
-- The Iceberg external table reads through the metadata chain including the
-- equality delete file. It should also show exactly 7 rows.

ASSERT ROW_COUNT = 7
SELECT * FROM {{zone_name}}.eq_del_demo.products_iceberg;


-- ============================================================================
-- Query 3: Delta — Deleted Products Not Visible
-- ============================================================================
-- Confirm that the 3 deleted products (id 2, 5, 8) are gone from Delta.

ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.eq_del_demo.products WHERE id IN (2, 5, 8);


-- ============================================================================
-- Query 4: Iceberg — Deleted Products Not Visible
-- ============================================================================
-- Confirm the equality delete file correctly filters out the same 3 products
-- when reading through Iceberg metadata.

ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.eq_del_demo.products_iceberg WHERE id IN (2, 5, 8);


-- ============================================================================
-- Query 5: Delta — Remaining Products Match
-- ============================================================================
-- Verify the exact set of remaining product IDs on the Delta side.

ASSERT ROW_COUNT = 1
ASSERT VALUE product_count = 7
ASSERT VALUE total_price = 2403.48
SELECT
    COUNT(*) AS product_count,
    ROUND(SUM(price), 2) AS total_price
FROM {{zone_name}}.eq_del_demo.products;


-- ============================================================================
-- Query 6: Iceberg — Remaining Products Match
-- ============================================================================
-- The Iceberg read should produce identical aggregates to Delta.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_price = 2403.48
SELECT
    ROUND(SUM(price), 2) AS total_price
FROM {{zone_name}}.eq_del_demo.products_iceberg;


-- ============================================================================
-- Query 7: Delta — Category Distribution
-- ============================================================================
-- Post-delete category breakdown on the Delta side.

ASSERT ROW_COUNT = 4
ASSERT VALUE product_count = 2 WHERE category = 'Electronics'
ASSERT VALUE product_count = 2 WHERE category = 'Energy'
ASSERT VALUE product_count = 2 WHERE category = 'Industrial'
ASSERT VALUE product_count = 1 WHERE category = 'Science'
SELECT
    category,
    COUNT(*) AS product_count
FROM {{zone_name}}.eq_del_demo.products
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 8: Iceberg — Category Distribution
-- ============================================================================
-- Same category breakdown via Iceberg — must match Delta exactly.

ASSERT ROW_COUNT = 4
ASSERT VALUE product_count = 2 WHERE category = 'Electronics'
ASSERT VALUE product_count = 2 WHERE category = 'Energy'
ASSERT VALUE product_count = 2 WHERE category = 'Industrial'
ASSERT VALUE product_count = 1 WHERE category = 'Science'
SELECT
    category,
    COUNT(*) AS product_count
FROM {{zone_name}}.eq_del_demo.products_iceberg
GROUP BY category
ORDER BY category;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: both Delta and Iceberg views agree on row
-- count, deleted product visibility, and total price.

ASSERT ROW_COUNT = 1
ASSERT VALUE delta_rows = 7
ASSERT VALUE iceberg_rows = 7
ASSERT VALUE delta_deleted = 0
ASSERT VALUE iceberg_deleted = 0
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.eq_del_demo.products) AS delta_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.eq_del_demo.products_iceberg) AS iceberg_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.eq_del_demo.products WHERE id IN (2, 5, 8)) AS delta_deleted,
    (SELECT COUNT(*) FROM {{zone_name}}.eq_del_demo.products_iceberg WHERE id IN (2, 5, 8)) AS iceberg_deleted;
