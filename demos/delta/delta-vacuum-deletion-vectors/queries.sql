-- ============================================================================
-- Delta VACUUM with Deletion Vectors: Why Space Is Not Reclaimed
-- ============================================================================
-- WHAT: With deletion vectors ON, VACUUM RETAIN 0 HOURS reclaims ZERO files
--       even after many UPDATE/DELETE operations. OPTIMIZE is what unlocks the
--       space.
-- WHY:  A deletion-vector UPDATE/DELETE does not rewrite the whole Parquet file.
--       It records the affected row positions in a tiny .bin sidecar and keeps
--       the original file ACTIVE (updated rows go to a small companion file).
--       Because no whole data file is ever dereferenced, nothing is orphaned,
--       so VACUUM has nothing to delete. The dead rows still occupy space INSIDE
--       the live files.
-- HOW:  Assert the first VACUUM reclaims 0 files, then run OPTIMIZE (which
--       physically rewrites the files without the deleted rows and drops the
--       deletion vectors, orphaning the originals), then assert a second VACUUM
--       finally reclaims those orphaned files. Data stays identical throughout.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Order status after the deletion-vector mutations
-- ============================================================================
-- setup.sql shipped orders 1-10, packed orders 11-16, and cancelled (deleted)
-- orders 25-29. 25 live rows remain. All of this was done through deletion
-- vectors, so it wrote .bin sidecars rather than rewriting whole files.

ASSERT VALUE order_count = 10 WHERE status = 'shipped'
ASSERT VALUE order_count = 6 WHERE status = 'packed'
ASSERT VALUE order_count = 9 WHERE status = 'received'
ASSERT ROW_COUNT = 3
SELECT status,
       COUNT(*) AS order_count,
       SUM(quantity) AS total_units
FROM {{zone_name}}.delta_demos.fulfillment_orders
GROUP BY status
ORDER BY status;


-- ============================================================================
-- EXPLORE: Orders per warehouse: the cancellations landed in WH-Central
-- ============================================================================
-- WH-East (1-10) and WH-West (11-20) keep all 10 orders each; WH-Central
-- (21-30) lost 5 to cancellation, leaving 5. Total 25 live rows.

ASSERT VALUE order_count = 10 WHERE warehouse = 'WH-East'
ASSERT VALUE order_count = 10 WHERE warehouse = 'WH-West'
ASSERT VALUE order_count = 5 WHERE warehouse = 'WH-Central'
ASSERT ROW_COUNT = 3
SELECT warehouse,
       COUNT(*) AS order_count
FROM {{zone_name}}.delta_demos.fulfillment_orders
GROUP BY warehouse
ORDER BY warehouse;


-- ============================================================================
-- VACUUM RETAIN 0 HOURS: reclaims NOTHING (this is the whole point)
-- ============================================================================
-- Even though setup ran two UPDATEs and a DELETE, VACUUM reclaims ZERO files.
-- For a VACUUM statement ASSERT ROW_COUNT equals the number of files reclaimed,
-- so ROW_COUNT = 0 proves the mutations orphaned no data file.
--
-- Why zero? Deletion vectors. Each UPDATE/DELETE recorded the affected rows in
-- a small .bin sidecar and left the original Parquet file ACTIVE (it still holds
-- the surviving rows). The updated rows were appended to a tiny companion file,
-- also active. No whole data file was ever dereferenced, so there is nothing for
-- VACUUM to delete. The bytes for the deleted/superseded rows are still on disk,
-- sitting INSIDE the live files, masked by the deletion vectors. VACUUM only
-- removes orphaned FILES; it never rewrites a file to drop masked rows.

ASSERT ROW_COUNT = 0
VACUUM {{zone_name}}.delta_demos.fulfillment_orders RETAIN 0 HOURS;


-- ============================================================================
-- LEARN: the data is correct: deletion vectors hide the masked rows
-- ============================================================================
-- Reads already skip the vector-masked rows, so the logical table is exactly
-- 25 rows with no cancelled orders present, even though their bytes remain on
-- disk. Deletion vectors trade deferred space for cheap, fast mutations.

ASSERT VALUE total_orders = 25
ASSERT VALUE cancelled_present = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_orders,
       COUNT(*) FILTER (WHERE order_id IN (25, 26, 27, 28, 29)) AS cancelled_present
FROM {{zone_name}}.delta_demos.fulfillment_orders;


-- ============================================================================
-- OPTIMIZE: the lever that finally frees the space
-- ============================================================================
-- OPTIMIZE physically rewrites the table's files, dropping the deletion-vector
-- masked rows and merging the tiny companion files into a compacted file. The
-- deletion vectors disappear (their rows are now physically gone) and the
-- ORIGINAL pre-OPTIMIZE files become orphaned: no longer referenced by the
-- current version, but still on disk until VACUUM removes them.

-- Non-deterministic: files compacted depends on engine write strategy
ASSERT WARNING ROW_COUNT >= 1
OPTIMIZE {{zone_name}}.delta_demos.fulfillment_orders;


-- ============================================================================
-- VACUUM RETAIN 0 HOURS (again): now it reclaims the orphaned originals
-- ============================================================================
-- After OPTIMIZE the pre-compaction files are orphaned, so this VACUUM finally
-- reclaims them. ROW_COUNT here is the number of files reclaimed (> 0), the
-- mirror image of the first VACUUM. This is the deletion-vector reclaim recipe:
-- OPTIMIZE to materialise the deletes, THEN VACUUM to free the disk.

ASSERT ROW_COUNT = 3
VACUUM {{zone_name}}.delta_demos.fulfillment_orders RETAIN 0 HOURS;


-- ============================================================================
-- LEARN: data integrity: OPTIMIZE and VACUUM changed zero rows
-- ============================================================================
-- The physical layout changed twice (OPTIMIZE rewrote files, VACUUM deleted the
-- orphans) but the logical table is untouched: still 25 orders with the same
-- status breakdown captured at the top of this script.

ASSERT VALUE order_count = 10 WHERE status = 'shipped'
ASSERT VALUE order_count = 6 WHERE status = 'packed'
ASSERT VALUE order_count = 9 WHERE status = 'received'
ASSERT ROW_COUNT = 3
SELECT status,
       COUNT(*) AS order_count,
       SUM(quantity) AS total_units
FROM {{zone_name}}.delta_demos.fulfillment_orders
GROUP BY status
ORDER BY status;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total live row count is 25
ASSERT ROW_COUNT = 25
SELECT * FROM {{zone_name}}.delta_demos.fulfillment_orders;

-- Verify order 1 was shipped by the first UPDATE
ASSERT VALUE status = 'shipped'
SELECT status FROM {{zone_name}}.delta_demos.fulfillment_orders WHERE order_id = 1;

-- Verify order 11 was packed by the second UPDATE
ASSERT VALUE status = 'packed'
SELECT status FROM {{zone_name}}.delta_demos.fulfillment_orders WHERE order_id = 11;

-- Verify the cancelled orders (25-29) are gone from the logical table
ASSERT VALUE cancelled_count = 0
SELECT COUNT(*) AS cancelled_count FROM {{zone_name}}.delta_demos.fulfillment_orders WHERE order_id IN (25, 26, 27, 28, 29);

-- Verify 3 distinct warehouses remain
ASSERT VALUE warehouse_count = 3
SELECT COUNT(DISTINCT warehouse) AS warehouse_count FROM {{zone_name}}.delta_demos.fulfillment_orders;

-- Verify a surviving WH-Central order is still present
ASSERT VALUE status = 'received'
SELECT status FROM {{zone_name}}.delta_demos.fulfillment_orders WHERE order_id = 30;
