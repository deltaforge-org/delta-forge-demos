-- ============================================================================
-- Iceberg UniForm Puffin Deletion Vectors — Queries
-- ============================================================================
-- Full-cycle verification: Delta table writes with puffin-v1 delete mode,
-- then Iceberg external table reads to confirm Puffin DVs are applied.
-- Products 2, 5, and 8 were deleted in setup, leaving 7 of 10 products.
-- ============================================================================


-- ============================================================================
-- Query 1: Delta Table — Post-Delete Row Count
-- ============================================================================
-- Verify the Delta table correctly shows 7 rows after deleting 3 products.

ASSERT ROW_COUNT = 7
SELECT * FROM {{zone_name}}.puffin_dv_demo.products;


-- ============================================================================
-- Query 2: Iceberg External Table — Post-Delete Row Count
-- ============================================================================
-- The Iceberg external table reads through the metadata chain including the
-- Puffin deletion vector. It should also show exactly 7 rows.

ASSERT ROW_COUNT = 7
SELECT * FROM {{zone_name}}.puffin_dv_demo.products_iceberg;


-- ============================================================================
-- Query 3: Delta — Deleted Products Not Visible
-- ============================================================================
-- Confirm that the 3 deleted products (id 2, 5, 8) are gone from Delta.

ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.puffin_dv_demo.products WHERE id IN (2, 5, 8);


-- ============================================================================
-- Query 4: Iceberg — Deleted Products Not Visible
-- ============================================================================
-- Confirm the Puffin deletion vector correctly filters out the same 3 products
-- when reading through Iceberg metadata.

ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.puffin_dv_demo.products_iceberg WHERE id IN (2, 5, 8);


-- ============================================================================
-- Query 5: Delta — Remaining Products Match
-- ============================================================================
-- Verify the exact set of remaining product IDs on the Delta side.

ASSERT ROW_COUNT = 1
ASSERT VALUE product_count = 7
ASSERT VALUE total_price = 2813.48
SELECT
    COUNT(*) AS product_count,
    ROUND(SUM(price), 2) AS total_price
FROM {{zone_name}}.puffin_dv_demo.products;


-- ============================================================================
-- Query 6: Iceberg — Remaining Products Match
-- ============================================================================
-- The Iceberg read should produce identical aggregates to Delta.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_price = 2813.48
SELECT
    ROUND(SUM(price), 2) AS total_price
FROM {{zone_name}}.puffin_dv_demo.products_iceberg;


-- ============================================================================
-- Query 7: Delta — Per-Row Value Check
-- ============================================================================
-- Verify each surviving product's actual name and price through Delta.
-- This catches corruption where row counts are right but values are wrong
-- (e.g., wrong rows deleted, data shifted, or columns swapped).

ASSERT ROW_COUNT = 7
ASSERT VALUE name = 'Quantum Widget' WHERE id = 1
ASSERT VALUE price = 299.99 WHERE id = 1
ASSERT VALUE name = 'Bio Reactor Kit' WHERE id = 3
ASSERT VALUE price = 599.0 WHERE id = 3
ASSERT VALUE name = 'Solar Panel Mini' WHERE id = 4
ASSERT VALUE price = 425.0 WHERE id = 4
ASSERT VALUE name = 'LED Matrix Board' WHERE id = 6
ASSERT VALUE price = 175.0 WHERE id = 6
ASSERT VALUE name = 'Thermal Coupler' WHERE id = 7
ASSERT VALUE price = 64.5 WHERE id = 7
ASSERT VALUE name = 'Wind Turbine Blade' WHERE id = 9
ASSERT VALUE price = 850.0 WHERE id = 9
ASSERT VALUE name = 'Plasma Cutter Pro' WHERE id = 10
ASSERT VALUE price = 399.99 WHERE id = 10
SELECT id, name, price
FROM {{zone_name}}.puffin_dv_demo.products
ORDER BY id;


-- ============================================================================
-- Query 8: Iceberg — Per-Row Value Check
-- ============================================================================
-- Same per-row verification via Iceberg. Values must match Delta exactly.

ASSERT ROW_COUNT = 7
ASSERT VALUE name = 'Quantum Widget' WHERE id = 1
ASSERT VALUE price = 299.99 WHERE id = 1
ASSERT VALUE name = 'Bio Reactor Kit' WHERE id = 3
ASSERT VALUE price = 599.0 WHERE id = 3
ASSERT VALUE name = 'Solar Panel Mini' WHERE id = 4
ASSERT VALUE price = 425.0 WHERE id = 4
ASSERT VALUE name = 'LED Matrix Board' WHERE id = 6
ASSERT VALUE price = 175.0 WHERE id = 6
ASSERT VALUE name = 'Thermal Coupler' WHERE id = 7
ASSERT VALUE price = 64.5 WHERE id = 7
ASSERT VALUE name = 'Wind Turbine Blade' WHERE id = 9
ASSERT VALUE price = 850.0 WHERE id = 9
ASSERT VALUE name = 'Plasma Cutter Pro' WHERE id = 10
ASSERT VALUE price = 399.99 WHERE id = 10
SELECT id, name, price
FROM {{zone_name}}.puffin_dv_demo.products_iceberg
ORDER BY id;


-- ============================================================================
-- Query 9: Delta — Category Distribution
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
FROM {{zone_name}}.puffin_dv_demo.products
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 10: Iceberg — Category Distribution
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
FROM {{zone_name}}.puffin_dv_demo.products_iceberg
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
    (SELECT COUNT(*) FROM {{zone_name}}.puffin_dv_demo.products) AS delta_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.puffin_dv_demo.products_iceberg) AS iceberg_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.puffin_dv_demo.products WHERE id IN (2, 5, 8)) AS delta_deleted,
    (SELECT COUNT(*) FROM {{zone_name}}.puffin_dv_demo.products_iceberg WHERE id IN (2, 5, 8)) AS iceberg_deleted;
