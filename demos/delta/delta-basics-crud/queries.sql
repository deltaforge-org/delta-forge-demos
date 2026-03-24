-- ============================================================================
-- Delta Basics CRUD — Educational Queries
-- ============================================================================
-- WHAT: The four fundamental Delta table operations: CREATE, INSERT, UPDATE, DELETE
-- WHY:  Unlike plain Parquet, Delta's transaction log enables mutability —
--       you can update and delete individual rows, not just append
-- HOW:  Each DML operation writes new Parquet data files and a JSON commit to
--       the _delta_log/ directory. UPDATEs and DELETEs use copy-on-write:
--       affected files are rewritten, and the old files are marked as removed
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline Table State — 20 Products
-- ============================================================================
-- The setup script created the products table and inserted 20 rows.
-- Let's inspect the starting state before we modify anything.

-- 4 categories before any modifications: Electronics, Furniture, Stationery, Audio
ASSERT ROW_COUNT = 4
ASSERT VALUE total_stock = 595 WHERE category = 'Electronics'
ASSERT VALUE total_stock = 2600 WHERE category = 'Stationery'
SELECT category,
       COUNT(*) AS product_count,
       ROUND(MIN(price), 2) AS min_price,
       ROUND(MAX(price), 2) AS max_price,
       SUM(stock) AS total_stock
FROM {{zone_name}}.delta_demos.products
GROUP BY category
ORDER BY category;


-- ============================================================================
-- CRUD: UPDATE — 10% Price Increase for Electronics
-- ============================================================================
-- Delta does NOT modify Parquet files in-place. Instead it uses copy-on-write:
--   1. Reads data files containing Electronics rows
--   2. Writes NEW Parquet files with the updated prices
--   3. Records an "add" action for new files and "remove" action for old files
--      in the _delta_log/ transaction log
-- This creates a new table version while preserving the old data for time travel.

UPDATE {{zone_name}}.delta_demos.products
SET price = ROUND(price * 1.10, 2)
WHERE category = 'Electronics';

-- Confirm: Electronics prices are 10% higher, Furniture unchanged
-- Uses time travel to compare current prices against the previous version
-- 5 Electronics + 5 Furniture = 10 rows shown
ASSERT ROW_COUNT = 10
WITH old AS (
    SELECT id, price AS old_price
    FROM {{zone_name}}.delta_demos.products VERSION AS OF 1
)
SELECT p.id, p.name, p.category, old.old_price, p.price AS new_price,
       CASE WHEN p.price > old.old_price THEN 'Price increased 10%'
            ELSE 'Price unchanged' END AS update_note
FROM {{zone_name}}.delta_demos.products p
JOIN old ON p.id = old.id
WHERE p.category IN ('Electronics', 'Furniture')
ORDER BY p.category, p.id;


-- ============================================================================
-- CRUD: UPDATE — Deactivate Products with Zero Stock
-- ============================================================================
-- Three products have stock=0: Desk Lamp (id=9), Binder Clips (id=15),
-- Earbuds (id=19). We'll mark them as inactive.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.products
SET is_active = false
WHERE stock = 0;

-- Confirm: compare is_active before and after using time travel
-- 3 products have stock=0 (Desk Lamp, Binder Clips, Earbuds)
ASSERT ROW_COUNT = 3
WITH before AS (
    SELECT id, is_active AS was_active
    FROM {{zone_name}}.delta_demos.products VERSION AS OF 2
)
SELECT p.id, p.name, p.stock, before.was_active, p.is_active,
       CASE WHEN before.was_active AND NOT p.is_active THEN 'Deactivated'
            ELSE 'No change' END AS update_note
FROM {{zone_name}}.delta_demos.products p
JOIN before ON p.id = before.id
WHERE p.stock = 0
ORDER BY p.id;


-- ============================================================================
-- CRUD: DELETE — Remove Inactive Products
-- ============================================================================
-- DELETE works similarly to UPDATE under the hood: Delta rewrites affected
-- data files without the deleted rows, and logs "remove" + "add" actions.
-- This removes the 3 inactive products (Desk Lamp, Binder Clips, Earbuds).

ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.delta_demos.products
WHERE is_active = false;

-- Confirm: show which products were removed using time travel
-- The previous version (before DELETE) still has them; current version does not
-- 3 products deleted (Desk Lamp id=9, Binder Clips id=15, Earbuds id=19)
ASSERT ROW_COUNT = 3
WITH before_delete AS (
    SELECT id, name, category
    FROM {{zone_name}}.delta_demos.products VERSION AS OF 3
)
SELECT b.id, b.name, b.category,
       CASE WHEN p.id IS NULL THEN 'Deleted' ELSE 'Still exists' END AS status
FROM before_delete b
LEFT JOIN {{zone_name}}.delta_demos.products p ON b.id = p.id
WHERE p.id IS NULL
ORDER BY b.id;


-- ============================================================================
-- CRUD: INSERT INTO...SELECT — Adding New Products
-- ============================================================================
-- INSERT creates a new data file and a new commit version in the log.
-- Here we add 5 new products using INSERT INTO...SELECT with a VALUES clause.

INSERT INTO {{zone_name}}.delta_demos.products
SELECT * FROM (VALUES
    (21, 'Webcam',         'Electronics', 69.99,  80,  true),
    (22, 'Footrest',       'Furniture',   49.99,  55,  true),
    (23, 'Stapler',        'Stationery',  12.99,  200, true),
    (24, 'DAC Amplifier',  'Audio',       129.99, 35,  true),
    (25, 'Whiteboard',     'Furniture',   79.99,  40,  true)
) AS t(id, name, category, price, stock, is_active);

-- Confirm: the 5 new products exist
ASSERT ROW_COUNT = 5
SELECT id, name, category, price, stock
FROM {{zone_name}}.delta_demos.products
WHERE id BETWEEN 21 AND 25
ORDER BY id;


-- ============================================================================
-- EXPLORE: Final Product Listing
-- ============================================================================
-- All 22 remaining products (20 - 3 deleted + 5 inserted), ordered by category.

ASSERT ROW_COUNT = 22
SELECT id, name, category, price, stock, is_active
FROM {{zone_name}}.delta_demos.products
ORDER BY category, name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Comprehensive verification of the final table state after all CRUD operations.

-- Verify total row count
ASSERT ROW_COUNT = 22
SELECT * FROM {{zone_name}}.delta_demos.products;

-- Verify all products are active
ASSERT VALUE inactive_count = 0
SELECT COUNT(*) FILTER (WHERE is_active = false) AS inactive_count FROM {{zone_name}}.delta_demos.products;

-- Verify deleted products are gone
ASSERT VALUE deleted_count = 0
SELECT COUNT(*) FILTER (WHERE id IN (9, 15, 19)) AS deleted_count FROM {{zone_name}}.delta_demos.products;

-- Verify electronics prices increased by 10%
ASSERT VALUE electronics_price_match = 5
SELECT COUNT(*) AS electronics_price_match
FROM {{zone_name}}.delta_demos.products p
JOIN {{zone_name}}.delta_demos.products VERSION AS OF 1 old ON p.id = old.id
WHERE old.category = 'Electronics'
  AND ROUND(p.price, 2) = ROUND(old.price * 1.10, 2);

-- Verify non-electronics prices unchanged
ASSERT VALUE price_drift_count = 0
SELECT COUNT(*) AS price_drift_count
FROM {{zone_name}}.delta_demos.products p
JOIN {{zone_name}}.delta_demos.products VERSION AS OF 1 old ON p.id = old.id
WHERE old.category != 'Electronics'
  AND p.price != old.price;

-- Verify new products count
ASSERT VALUE new_products_count = 5
SELECT COUNT(*) AS new_products_count FROM {{zone_name}}.delta_demos.products WHERE id BETWEEN 21 AND 25;

-- Verify webcam price
ASSERT VALUE price = 69.99
SELECT price FROM {{zone_name}}.delta_demos.products WHERE id = 21;

-- Verify electronics count
ASSERT VALUE electronics_count = 6
SELECT COUNT(*) AS electronics_count FROM {{zone_name}}.delta_demos.products WHERE category = 'Electronics';

-- Verify furniture count
ASSERT VALUE furniture_count = 6
SELECT COUNT(*) AS furniture_count FROM {{zone_name}}.delta_demos.products WHERE category = 'Furniture';

-- Verify category count
ASSERT VALUE category_count = 4
SELECT COUNT(DISTINCT category) AS category_count FROM {{zone_name}}.delta_demos.products;

-- Verify total stock
ASSERT VALUE total_stock = 4025
SELECT SUM(stock) AS total_stock FROM {{zone_name}}.delta_demos.products;
