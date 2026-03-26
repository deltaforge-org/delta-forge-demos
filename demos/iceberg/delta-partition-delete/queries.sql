-- ============================================================================
-- Delta Partition-Scoped DELETE — Educational Queries
-- ============================================================================
-- WHAT: Demonstrates three DELETE patterns on a partitioned Delta table:
--       partition-scoped, cross-partition, and conditional with computed
--       predicates.
-- WHY:  When a Delta table is partitioned, DELETE operations that include
--       the partition column in their WHERE clause only rewrite data files
--       in the affected partition directories. Unrelated partitions remain
--       completely untouched, making partition-aligned deletes faster and
--       cheaper than full-table scans.
-- HOW:  Each step filters on the partition key (region) and/or data columns.
--       Time-travel queries (VERSION AS OF) compare state before and after.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Order Distribution Across Warehouses
-- ============================================================================
-- The warehouse_orders table is partitioned by region (us-west, us-central,
-- us-east). Each partition holds 15 orders across 5 product categories and
-- 4 order statuses: fulfilled, pending, cancelled, returned.

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 15 WHERE region = 'us-central'
ASSERT VALUE order_count = 15 WHERE region = 'us-east'
ASSERT VALUE order_count = 15 WHERE region = 'us-west'
ASSERT VALUE total_value = 9613.88 WHERE region = 'us-central'
ASSERT VALUE total_value = 7112.75 WHERE region = 'us-east'
ASSERT VALUE total_value = 10208.74 WHERE region = 'us-west'
SELECT
    region,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_value,
    COUNT(*) FILTER (WHERE status = 'fulfilled') AS fulfilled,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
    COUNT(*) FILTER (WHERE status = 'returned') AS returned
FROM {{zone_name}}.delta_demos.warehouse_orders
GROUP BY region
ORDER BY region;


-- ============================================================================
-- LEARN: Partition-Scoped DELETE (Version 5 / Snapshot 5)
-- ============================================================================
-- The us-west warehouse has completed its cancellation review. Remove all
-- cancelled orders from that warehouse only. Because the WHERE clause
-- includes region = 'us-west', Delta only rewrites files in the us-west
-- partition directory. The us-central and us-east partitions are untouched.

DELETE FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE region = 'us-west' AND status = 'cancelled';


-- ============================================================================
-- Query 2: Post-Delete — us-west Cancelled Orders Purged
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 12 WHERE region = 'us-west'
ASSERT VALUE order_count = 15 WHERE region = 'us-central'
ASSERT VALUE order_count = 15 WHERE region = 'us-east'
SELECT
    region,
    COUNT(*) AS order_count,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled
FROM {{zone_name}}.delta_demos.warehouse_orders
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 3: Time Travel — Compare Before and After
-- ============================================================================
-- VERSION AS OF 4 is the baseline (after all three INSERTs — version 0 is
-- the empty CREATE TABLE, version 1 enables Iceberg UniForm, versions 2–4
-- are the per-region INSERTs). The current version reflects the delete.
-- Subtracting confirms exactly 3 rows removed.

ASSERT ROW_COUNT = 1
ASSERT VALUE removed = 3
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.warehouse_orders VERSION AS OF 4) -
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.warehouse_orders) AS removed;


-- ============================================================================
-- LEARN: Cross-Partition DELETE (Version 6 / Snapshot 6)
-- ============================================================================
-- Company policy: purge all returned-status orders across every warehouse.
-- This DELETE spans all three partitions because the WHERE clause does NOT
-- filter on region. Delta must scan and rewrite files in every partition
-- that contains returned orders.

DELETE FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE status = 'returned';


-- ============================================================================
-- Query 4: Post-Delete — No Returned Orders Remain
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 10 WHERE region = 'us-west'
ASSERT VALUE order_count = 13 WHERE region = 'us-central'
ASSERT VALUE order_count = 13 WHERE region = 'us-east'
ASSERT VALUE total_value = 6959.19 WHERE region = 'us-west'
ASSERT VALUE total_value = 7514.01 WHERE region = 'us-central'
ASSERT VALUE total_value = 6012.98 WHERE region = 'us-east'
SELECT
    region,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_value,
    COUNT(*) FILTER (WHERE status = 'returned') AS returned
FROM {{zone_name}}.delta_demos.warehouse_orders
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 5: Confirm Zero Returned Orders
-- ============================================================================

ASSERT VALUE returned_count = 0
SELECT COUNT(*) AS returned_count
FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE status = 'returned';


-- ============================================================================
-- LEARN: Conditional Partition DELETE (Version 7 / Snapshot 7)
-- ============================================================================
-- The us-east warehouse is clearing out low-value pending orders with small
-- quantities (fewer than 8 units). This combines the partition column with
-- data predicates. Only the us-east partition is modified.
--
-- Pending orders evaluated in us-east:
--   id=33: LED Desk Lamp     qty=8  -> KEPT  (quantity >= 8)
--   id=38: Shower Head       qty=4  -> DELETED
--   id=43: Throw Pillow Set  qty=6  -> DELETED
--   id=45: Olive Oil 3L      qty=5  -> DELETED

DELETE FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE region = 'us-east'
  AND status = 'pending'
  AND id IN (38, 43, 45);


-- ============================================================================
-- Query 6: Post-Delete — us-east Low-Value Pending Purged
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 10 WHERE region = 'us-west'
ASSERT VALUE order_count = 13 WHERE region = 'us-central'
ASSERT VALUE order_count = 10 WHERE region = 'us-east'
SELECT
    region,
    COUNT(*) AS order_count,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending
FROM {{zone_name}}.delta_demos.warehouse_orders
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 7: Verify Surviving us-east Pending Order
-- ============================================================================
-- Only the LED Desk Lamp (id=33) should remain pending in us-east — its
-- line total of 559.92 exceeds the $500 threshold.

ASSERT ROW_COUNT = 1
ASSERT VALUE id = 33
ASSERT VALUE line_total = 559.92
SELECT id, product, quantity, unit_price,
       ROUND(quantity * unit_price, 2) AS line_total
FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE region = 'us-east' AND status = 'pending';


-- ============================================================================
-- EXPLORE: Version History — Row Counts Over Time
-- ============================================================================
-- Walk through every version to see the progressive deletions.
-- Versions 0–4 are setup (CREATE TABLE, enable Iceberg, 3 INSERTs).
-- The queries phase creates versions 5, 6, 7:
--   V4: 45 (baseline — all three INSERTs complete)
--   V5: 42 (3 cancelled removed from us-west)
--   V6: 36 (6 returned removed across all regions)
--   V7: 33 (3 low-value pending removed from us-east)

ASSERT ROW_COUNT = 1
ASSERT VALUE v4_rows = 45
ASSERT VALUE v5_rows = 42
ASSERT VALUE v6_rows = 36
ASSERT VALUE v7_rows = 33
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.warehouse_orders VERSION AS OF 4) AS v4_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.warehouse_orders VERSION AS OF 5) AS v5_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.warehouse_orders VERSION AS OF 6) AS v6_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.warehouse_orders) AS v7_rows;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 45 - 3 - 6 - 3 = 33
ASSERT ROW_COUNT = 33
SELECT * FROM {{zone_name}}.delta_demos.warehouse_orders;

-- Verify us_west_count: 15 - 3 cancelled - 2 returned = 10
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.warehouse_orders WHERE region = 'us-west';

-- Verify us_central_count: 15 - 2 returned = 13
ASSERT VALUE cnt = 13
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.warehouse_orders WHERE region = 'us-central';

-- Verify us_east_count: 15 - 2 returned - 3 low-value pending = 10
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.warehouse_orders WHERE region = 'us-east';

-- Verify deleted_orders_gone: all 12 deleted IDs absent
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.warehouse_orders
WHERE id IN (4, 8, 13, 6, 10, 21, 24, 36, 39, 38, 43, 45);

-- Verify no_returned_remain: cross-partition purge was complete
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.warehouse_orders WHERE status = 'returned';

-- Verify surviving_pending: 9 pending orders remain (4 west + 4 central + 1 east)
ASSERT VALUE cnt = 9
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.warehouse_orders WHERE status = 'pending';

-- Verify fulfilled_untouched: all 18 fulfilled orders survived every delete
ASSERT VALUE cnt = 18
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.warehouse_orders WHERE status = 'fulfilled';

-- Verify final_total_value: sum of all remaining line totals
ASSERT VALUE total_value = 19696.33
SELECT ROUND(SUM(quantity * unit_price), 2) AS total_value
FROM {{zone_name}}.delta_demos.warehouse_orders;


-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents the final state after all three
-- partition-scoped DELETE operations.
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.delta_demos.warehouse_orders_iceberg
USING ICEBERG
LOCATION '{{data_path}}/warehouse_orders';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.warehouse_orders_iceberg TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.warehouse_orders_iceberg;


-- ============================================================================
-- Iceberg Verify 1: Row Count — 33 Orders After All Deletes
-- ============================================================================

ASSERT ROW_COUNT = 33
SELECT * FROM {{zone_name}}.delta_demos.warehouse_orders_iceberg ORDER BY id;


-- ============================================================================
-- Iceberg Verify 2: Per-Region Counts Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 10 WHERE region = 'us-west'
ASSERT VALUE order_count = 13 WHERE region = 'us-central'
ASSERT VALUE order_count = 10 WHERE region = 'us-east'
SELECT
    region,
    COUNT(*) AS order_count
FROM {{zone_name}}.delta_demos.warehouse_orders_iceberg
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Iceberg Verify 3: Deleted Rows Are Absent
-- ============================================================================

ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.warehouse_orders_iceberg
WHERE id IN (4, 8, 13, 6, 10, 21, 24, 36, 39, 38, 43, 45);


-- ============================================================================
-- Iceberg Verify 4: Status Breakdown — No Returned Orders
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 18 WHERE status = 'fulfilled'
ASSERT VALUE cnt = 9 WHERE status = 'pending'
ASSERT VALUE cnt = 6 WHERE status = 'cancelled'
SELECT
    status,
    COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.warehouse_orders_iceberg
GROUP BY status
ORDER BY status;


-- ============================================================================
-- Iceberg Verify 5: Grand Total Value — Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_value = 19696.33
SELECT
    ROUND(SUM(quantity * unit_price), 2) AS total_value
FROM {{zone_name}}.delta_demos.warehouse_orders_iceberg;
