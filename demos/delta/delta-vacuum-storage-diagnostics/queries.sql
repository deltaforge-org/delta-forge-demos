-- ============================================================================
-- Delta VACUUM Storage Diagnostics — Educational Queries
-- ============================================================================
-- WHAT: DESCRIBE DETAIL exposes a Delta table's physical state: version
--       number, active file count, and total byte size. By running it before
--       and after mutations and VACUUM, you can quantify exactly how storage
--       evolves and how much VACUUM reclaims.
-- WHY:  Cloud storage costs are opaque. Teams run VACUUM but never measure
--       its impact. Without before/after metrics, you cannot justify
--       maintenance windows or set compaction schedules.
-- HOW:  We walk through 4 versions of a product inventory — price updates,
--       discontinued removals, and new arrivals — running DESCRIBE DETAIL at
--       each checkpoint to track the table's physical footprint.
-- ============================================================================


-- ============================================================================
-- INSPECT: Baseline table metrics (V1 — after initial 30-row INSERT)
-- ============================================================================
-- DESCRIBE DETAIL returns the table's physical state as key-value properties.
-- At this point we have a single INSERT (V1), so we expect version=1 and
-- format=delta. The num_files and size_in_bytes values establish our baseline
-- for measuring storage growth from subsequent mutations.

-- Non-deterministic: num_files and size_in_bytes depend on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
ASSERT VALUE value = 'delta' WHERE property = 'format'
DESCRIBE DETAIL {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- EXPLORE: Baseline inventory — 30 products across 5 categories
-- ============================================================================
-- Every product starts as 'active'. This snapshot represents V1 before any
-- mutations create orphaned files.

ASSERT ROW_COUNT = 5
ASSERT VALUE product_count = 6 WHERE category = 'Electronics'
ASSERT VALUE product_count = 6 WHERE category = 'Food'
ASSERT VALUE total_value = 814.94 WHERE category = 'Electronics'
ASSERT VALUE total_value = 80.94 WHERE category = 'Food'
SELECT category,
       COUNT(*) AS product_count,
       ROUND(SUM(price), 2) AS total_value,
       ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.delta_demos.product_inventory
GROUP BY category
ORDER BY category;


-- ============================================================================
-- V2: UPDATE — 10% price increase for all Electronics
-- ============================================================================
-- Delta uses copy-on-write: every file containing an Electronics row is
-- rewritten with updated prices. The old files become orphaned — they still
-- sit on disk but are no longer referenced by the current table version.

ASSERT ROW_COUNT = 6
UPDATE {{zone_name}}.delta_demos.product_inventory
SET price = ROUND(price * 1.10, 2)
WHERE category = 'Electronics';


-- ============================================================================
-- INSPECT: Table metrics after price update (V2)
-- ============================================================================
-- The version has incremented. Depending on the engine's write strategy,
-- num_files may have changed (old files orphaned, new files created). The
-- orphaned files are invisible here — DESCRIBE DETAIL only shows the current
-- snapshot's active files — but they still consume disk space.

-- Non-deterministic: file metrics depend on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
ASSERT VALUE value = 'delta' WHERE property = 'format'
DESCRIBE DETAIL {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- EXPLORE: Verify Electronics prices after 10% increase
-- ============================================================================
-- Confirm the update applied correctly before proceeding to more mutations.

ASSERT ROW_COUNT = 6
ASSERT VALUE price = 87.99 WHERE id = 1
ASSERT VALUE price = 219.99 WHERE id = 6
ASSERT VALUE price = 329.99 WHERE id = 4
SELECT id, sku, product_name, price
FROM {{zone_name}}.delta_demos.product_inventory
WHERE category = 'Electronics'
ORDER BY id;


-- ============================================================================
-- V3: DELETE — 4 discontinued Food products removed
-- ============================================================================
-- Products 27-30 (Trail Mix, Green Tea, Protein Bars, Dried Fruit) are
-- discontinued and purged. Delta rewrites affected files WITHOUT these rows,
-- orphaning the previous versions.

ASSERT ROW_COUNT = 4
DELETE FROM {{zone_name}}.delta_demos.product_inventory
WHERE id BETWEEN 27 AND 30;


-- ============================================================================
-- INSPECT: Table metrics after deletion (V3)
-- ============================================================================
-- Another version increment, another round of orphaned files. The storage
-- overhead compounds with each mutation — old files from V1 and V2 are
-- still sitting on disk alongside the current V3 files.

-- Non-deterministic: file metrics depend on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
ASSERT VALUE value = 'delta' WHERE property = 'format'
DESCRIBE DETAIL {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- V4: INSERT — 5 new premium products added
-- ============================================================================
-- New arrivals create additional data files. These files are brand-new and
-- will NOT be orphaned by VACUUM — only files from prior versions that have
-- been superseded are candidates for cleanup.

ASSERT ROW_COUNT = 5
INSERT INTO {{zone_name}}.delta_demos.product_inventory VALUES
    (31, 'SKU-P001', 'Premium Headphones',  'Electronics', 349.99,  40, 'active'),
    (32, 'SKU-P002', 'Cashmere Sweater',    'Clothing',    249.99,  30, 'active'),
    (33, 'SKU-P003', 'Espresso Machine',    'Home',        499.99,  25, 'active'),
    (34, 'SKU-P004', 'Carbon Bike Frame',   'Sports',      899.99,  15, 'active'),
    (35, 'SKU-P005', 'Wagyu Beef Box',      'Food',        159.99,  20, 'active');


-- ============================================================================
-- INSPECT: Pre-VACUUM metrics (V4) — maximum storage overhead
-- ============================================================================
-- This is the high-water mark: 4 versions of mutations have accumulated
-- orphaned files on disk. DESCRIBE DETAIL shows the current snapshot's
-- active files, but the actual disk footprint includes all orphaned files
-- from V1-V3 that are still physically present.

-- Non-deterministic: file metrics depend on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
ASSERT VALUE value = 'delta' WHERE property = 'format'
DESCRIBE DETAIL {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- LEARN: Version timeline — DESCRIBE HISTORY before VACUUM
-- ============================================================================
-- DESCRIBE HISTORY reveals every operation that touched this table. Each
-- entry corresponds to a version in the transaction log. After VACUUM, these
-- metadata entries survive (the log is never pruned by VACUUM), but the
-- data files for old versions may be gone.

-- Non-deterministic: DESCRIBE HISTORY may include extra internal versions
ASSERT WARNING ROW_COUNT >= 4
DESCRIBE HISTORY {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- VACUUM — remove orphaned data files
-- ============================================================================
-- VACUUM scans the transaction log to identify which Parquet files are
-- referenced by the current version, then deletes everything else that
-- exceeds the retention period. After this command:
--   - Orphaned files from the UPDATE, DELETE, and previous INSERTs are gone
--   - Only files referenced by V4 remain on disk
--   - The transaction log is untouched (DESCRIBE HISTORY still works)
--   - All current data is bit-for-bit identical

VACUUM {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- INSPECT: Post-VACUUM metrics — storage reclaimed
-- ============================================================================
-- Compare this output with the pre-VACUUM DESCRIBE DETAIL above. The version
-- may have incremented (VACUUM can create a commit entry), but the key
-- observation is: format=delta, and the table is healthy. The orphaned files
-- from V1-V3 have been physically deleted from disk.

-- Non-deterministic: file metrics depend on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
ASSERT VALUE value = 'delta' WHERE property = 'format'
DESCRIBE DETAIL {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- LEARN: Post-VACUUM data integrity — category breakdown
-- ============================================================================
-- The most critical validation: every query returns the exact same results
-- as before VACUUM. The UPDATE prices, DELETE removals, and INSERT additions
-- are all preserved. VACUUM only removed unreferenced files — it never
-- touches the current snapshot's data.

ASSERT ROW_COUNT = 5
ASSERT VALUE product_count = 7 WHERE category = 'Electronics'
ASSERT VALUE product_count = 7 WHERE category = 'Clothing'
ASSERT VALUE product_count = 7 WHERE category = 'Home'
ASSERT VALUE product_count = 7 WHERE category = 'Sports'
ASSERT VALUE product_count = 3 WHERE category = 'Food'
SELECT category,
       COUNT(*) AS product_count,
       ROUND(SUM(price), 2) AS total_value,
       ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.delta_demos.product_inventory
GROUP BY category
ORDER BY category;


-- ============================================================================
-- LEARN: Post-VACUUM — Electronics prices survived the 10% increase
-- ============================================================================
-- The V2 price update applied ROUND(price * 1.10, 2) to all Electronics.
-- After VACUUM cleaned up the pre-update files, the updated prices remain
-- exactly as computed. VACUUM does not rewrite or modify active files.

ASSERT ROW_COUNT = 7
ASSERT VALUE price = 87.99 WHERE id = 1
ASSERT VALUE price = 219.99 WHERE id = 6
ASSERT VALUE price = 349.99 WHERE id = 31
SELECT id, sku, product_name, price, stock_qty
FROM {{zone_name}}.delta_demos.product_inventory
WHERE category = 'Electronics'
ORDER BY id;


-- ============================================================================
-- LEARN: Post-VACUUM — discontinued products stay deleted
-- ============================================================================
-- The 4 Food products removed in V3 are permanently gone. VACUUM deleted
-- the old files that still contained those rows, but the deletion was
-- already committed in V3. The current snapshot never referenced those rows.

ASSERT VALUE remaining_food = 3
ASSERT VALUE deleted_products = 0
ASSERT ROW_COUNT = 1
SELECT (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.product_inventory
        WHERE category = 'Food') AS remaining_food,
       (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.product_inventory
        WHERE id BETWEEN 27 AND 30) AS deleted_products;


-- ============================================================================
-- LEARN: Post-VACUUM — DESCRIBE HISTORY survives cleanup
-- ============================================================================
-- VACUUM removes data files, NOT transaction log entries. The full version
-- history is still visible. This is a critical distinction: the log records
-- WHAT happened and WHEN, while VACUUM only controls WHETHER you can still
-- READ the data from old versions (time travel).

-- Non-deterministic: DESCRIBE HISTORY may include extra internal versions
ASSERT WARNING ROW_COUNT >= 4
DESCRIBE HISTORY {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total product count is 31 (30 - 4 + 5)
ASSERT VALUE total_products = 31
SELECT COUNT(*) AS total_products FROM {{zone_name}}.delta_demos.product_inventory;

-- Verify total inventory value
ASSERT VALUE total_value = 4139.19
SELECT ROUND(SUM(price), 2) AS total_value FROM {{zone_name}}.delta_demos.product_inventory;

-- Verify 5 distinct categories
ASSERT VALUE category_count = 5
SELECT COUNT(DISTINCT category) AS category_count FROM {{zone_name}}.delta_demos.product_inventory;

-- Verify Electronics price increase applied (id=1: 79.99 * 1.10 = 87.99)
ASSERT VALUE price = 87.99
SELECT price FROM {{zone_name}}.delta_demos.product_inventory WHERE id = 1;

-- Verify discontinued products are gone
ASSERT VALUE discontinued_count = 0
SELECT COUNT(*) AS discontinued_count FROM {{zone_name}}.delta_demos.product_inventory WHERE id BETWEEN 27 AND 30;

-- Verify new premium products present
ASSERT VALUE premium_count = 5
SELECT COUNT(*) AS premium_count FROM {{zone_name}}.delta_demos.product_inventory WHERE id BETWEEN 31 AND 35;

-- Verify total stock quantity
ASSERT VALUE total_stock = 6990
SELECT SUM(stock_qty) AS total_stock FROM {{zone_name}}.delta_demos.product_inventory;
