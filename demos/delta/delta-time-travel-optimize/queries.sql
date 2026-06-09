-- ============================================================================
-- Delta Time Travel & OPTIMIZE — Educational Queries
-- ============================================================================
-- WHAT: OPTIMIZE compacts small Parquet files into fewer, larger files to
--       improve read performance without changing any data.
-- WHY:  Each INSERT, UPDATE, and DELETE creates new small Parquet files.
--       Over time, a table may accumulate thousands of tiny files (the
--       "small file problem"). Reading many small files is slow because of
--       per-file overhead (opening, metadata parsing, I/O scheduling).
--       OPTIMIZE solves this by merging files into optimally-sized ones.
-- HOW:  OPTIMIZE reads all active data files, merges them into larger files
--       (typically ~1GB each), and writes a new commit that removes the old
--       files and adds the merged ones. The data is identical — only the
--       physical layout changes. This creates a new version in the log.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Fragmented State (Before OPTIMIZE)
-- ============================================================================
-- After setup, the table went through 4 versions of DML operations:
--
--   V0: CREATE empty table             (creates Delta log)
--   V1: INSERT 25 items               (creates initial data files)
--   V2: UPDATE 5 low-stock items      (creates rewritten files for changed rows)
--   V3: DELETE 3 discontinued items   (creates rewritten files minus deleted rows)
--   V4: INSERT 10 new items           (creates additional data files)
--
-- Each operation added new small Parquet files. Let's see the current state
-- before running OPTIMIZE:

ASSERT ROW_COUNT = 1
ASSERT VALUE items = 32
ASSERT VALUE categories = 4
ASSERT VALUE warehouses = 3
SELECT 'Pre-OPTIMIZE inventory' AS state,
       COUNT(*) AS items,
       COUNT(DISTINCT category) AS categories,
       COUNT(DISTINCT warehouse) AS warehouses
FROM {{zone_name}}.delta_demos.inventory;


-- ============================================================================
-- LEARN: Why OPTIMIZE Matters — The Small File Problem
-- ============================================================================
-- Each DML operation writes new Parquet files:
--   V0: ~1 file (initial INSERT)
--   V1: +1 file (UPDATE rewrites affected rows)
--   V2: +1 file (DELETE rewrites affected rows)
--   V3: +1 file (new INSERT)
--
-- After V3, the table has 4+ small data files. On a production table with
-- thousands of daily micro-batches, this could grow to 10,000+ files.
-- OPTIMIZE merges them into ~1 optimally-sized file.
--
-- Let's look at the inventory breakdown before compaction:

ASSERT ROW_COUNT = 4
ASSERT VALUE inventory_value = 170840.35 WHERE category = 'Electronics'
ASSERT VALUE inventory_value = 112843.65 WHERE category = 'Furniture'
ASSERT VALUE inventory_value = 112443.55 WHERE category = 'Audio'
ASSERT VALUE inventory_value = 30474.40 WHERE category = 'Stationery'
SELECT category,
       COUNT(*) AS items,
       SUM(qty) AS total_stock,
       ROUND(SUM(qty * price), 2) AS inventory_value
FROM {{zone_name}}.delta_demos.inventory
GROUP BY category
ORDER BY inventory_value DESC;


-- ============================================================================
-- ACTION: Run OPTIMIZE to Compact Data Files
-- ============================================================================
-- After multiple inserts, updates, and deletes, the table has many small files.
-- OPTIMIZE merges them into fewer, larger files for better read performance.
-- The data content is unchanged — only the physical file layout improves.
-- This creates a new version (V4) in the Delta log.

OPTIMIZE {{zone_name}}.delta_demos.inventory;


-- ============================================================================
-- LEARN: OPTIMIZE Creates a New Version
-- ============================================================================
-- OPTIMIZE is itself a versioned operation (V5 in this demo). This means:
--   1. You can time-travel back to before OPTIMIZE ran
--   2. The old small files are not immediately deleted (they are just
--      marked as removed in the log)
--   3. VACUUM is needed later to physically delete the old files
--
-- The data at V4 and V5 is identical — only the file layout differs:

ASSERT ROW_COUNT = 5
ASSERT VALUE row_count = 25 WHERE version = 'V1 (initial)'
ASSERT VALUE row_count = 25 WHERE version = 'V2 (restocked)'
ASSERT VALUE row_count = 22 WHERE version = 'V3 (deleted 3)'
ASSERT VALUE row_count = 32 WHERE version = 'V4 (added 10)'
ASSERT VALUE row_count = 32 WHERE version = 'V5 (optimized)'
SELECT 'V1 (initial)' AS version, COUNT(*) AS row_count
FROM {{zone_name}}.delta_demos.inventory VERSION AS OF 1
UNION ALL
SELECT 'V2 (restocked)', COUNT(*)
FROM {{zone_name}}.delta_demos.inventory VERSION AS OF 2
UNION ALL
SELECT 'V3 (deleted 3)', COUNT(*)
FROM {{zone_name}}.delta_demos.inventory VERSION AS OF 3
UNION ALL
SELECT 'V4 (added 10)', COUNT(*)
FROM {{zone_name}}.delta_demos.inventory VERSION AS OF 4
UNION ALL
SELECT 'V5 (optimized)', COUNT(*)
FROM {{zone_name}}.delta_demos.inventory;


-- ============================================================================
-- LEARN: What Happened at Each Version — Tracking Changes
-- ============================================================================
-- V1 restocked 5 items that had qty < 50 by adding 100 units each.
-- Let's verify the restock by looking at items that were affected:

ASSERT ROW_COUNT = 5
ASSERT VALUE qty = 145 WHERE id = 4
ASSERT VALUE qty = 125 WHERE id = 7
ASSERT VALUE qty = 135 WHERE id = 18
ASSERT VALUE qty = 120 WHERE id = 20
ASSERT VALUE qty = 115 WHERE id = 23
SELECT id, item, category, qty, warehouse,
       'Restocked: was < 50, now +100' AS note
FROM {{zone_name}}.delta_demos.inventory
WHERE id IN (4, 7, 18, 20, 23)
ORDER BY id;


-- ============================================================================
-- EXPLORE: Items Deleted in V2
-- ============================================================================
-- 3 items were deleted in V2 (ids 9, 15, 24). They no longer appear in
-- the current version, but we can still see them via time travel to V1:

ASSERT ROW_COUNT = 3
ASSERT VALUE qty = 90 WHERE id = 9
ASSERT VALUE qty = 400 WHERE id = 15
ASSERT VALUE qty = 200 WHERE id = 24
SELECT id, item, category, qty, price
FROM {{zone_name}}.delta_demos.inventory VERSION AS OF 1
WHERE id IN (9, 15, 24)
ORDER BY id;


-- ============================================================================
-- EXPLORE: Items Added in V3
-- ============================================================================
-- 10 new items were added to fill gaps and expand the catalog:

ASSERT ROW_COUNT = 10
SELECT id, item, category, qty, price, warehouse
FROM {{zone_name}}.delta_demos.inventory
WHERE id BETWEEN 26 AND 35
ORDER BY id;


-- ============================================================================
-- LEARN: OPTIMIZE + VACUUM — The Complete Maintenance Cycle
-- ============================================================================
-- OPTIMIZE compacts files but leaves old files on disk (for time travel).
-- VACUUM physically deletes old files that are older than the retention
-- period (default: 7 days). Together they form the maintenance cycle:
--
--   1. OPTIMIZE — merge small files into large ones (improves reads)
--   2. VACUUM   — remove old unreferenced files (reclaims storage)
--
-- After VACUUM, time travel to versions before the retention period
-- will no longer work because the old files are physically gone.

ASSERT ROW_COUNT = 1
ASSERT VALUE qty = 50
ASSERT VALUE price = 1299.99
SELECT 'Items unchanged by OPTIMIZE' AS observation,
       id, item, qty, price
FROM {{zone_name}}.delta_demos.inventory
WHERE id = 1;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify final row count is 32
ASSERT ROW_COUNT = 32
SELECT * FROM {{zone_name}}.delta_demos.inventory;

-- Verify deleted items (ids 9, 15, 24) are gone
ASSERT VALUE deleted_count = 0
SELECT COUNT(*) FILTER (WHERE id IN (9, 15, 24)) AS deleted_count FROM {{zone_name}}.delta_demos.inventory;

-- Verify standing desk (id=7) was restocked to 125
ASSERT VALUE qty = 125
SELECT qty FROM {{zone_name}}.delta_demos.inventory WHERE id = 7;

-- Verify sound bar (id=20) was restocked to 120
ASSERT VALUE qty = 120
SELECT qty FROM {{zone_name}}.delta_demos.inventory WHERE id = 20;

-- Verify 10 new items were added (ids 26-35)
ASSERT VALUE new_items_count = 10
SELECT COUNT(*) AS new_items_count FROM {{zone_name}}.delta_demos.inventory WHERE id BETWEEN 26 AND 35;

-- Verify laptop (id=1) is unchanged after OPTIMIZE
ASSERT VALUE laptop_match = 1
SELECT COUNT(*) AS laptop_match FROM {{zone_name}}.delta_demos.inventory WHERE id = 1 AND qty = 50 AND price = 1299.99;

-- Verify Electronics category has 9 items
ASSERT VALUE electronics_count = 9
SELECT COUNT(*) AS electronics_count FROM {{zone_name}}.delta_demos.inventory WHERE category = 'Electronics';
