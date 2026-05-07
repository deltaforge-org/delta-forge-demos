-- ============================================================================
-- Delta Partitioning — Educational Queries
-- ============================================================================
-- WHAT: PARTITIONED BY organizes Delta table data files into separate
--       directories based on partition column values (e.g., region=North/).
-- WHY:  Partition pruning allows queries with WHERE region = 'North' to
--       skip reading files from South, East, and West entirely, reducing
--       I/O by up to 75% for a 4-partition table.
-- HOW:  Delta records partition values in the transaction log's add/remove
--       file actions. The query engine reads these metadata entries first,
--       then only opens Parquet files in matching partition directories.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — All 4 Partitions Before Any Changes
-- ============================================================================
-- The orders table is partitioned by region with 20 rows each. Let's see
-- the starting state of every partition before we make any modifications:

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 3355.5 WHERE region = 'North'
ASSERT VALUE total_revenue = 3459.0 WHERE region = 'South'
ASSERT VALUE total_revenue = 3290.0 WHERE region = 'East'
ASSERT VALUE total_revenue = 3283.0 WHERE region = 'West'
SELECT region,
       COUNT(*) AS order_count,
       ROUND(SUM(amount), 2) AS total_revenue,
       ROUND(AVG(amount), 2) AS avg_order,
       ROUND(MIN(amount), 2) AS min_order,
       ROUND(MAX(amount), 2) AS max_order
FROM {{zone_name}}.delta_demos.partitioned_orders
GROUP BY region
ORDER BY region;


-- ============================================================================
-- LEARN: Partition-Scoped UPDATE — 15% Discount for South Region
-- ============================================================================
-- In a partitioned table, an UPDATE with a predicate on the partition column
-- only rewrites the Parquet files in that partition's directory. Here, only
-- the region=South/ directory is affected. Files in North, East, and West
-- are untouched — their data files are not read or rewritten.
--
-- South amounts before: 130+260+80+190+40+135+320+99+210+28+160+290+92+440+33+145+480+70+235+22 = 3459
-- After 15% discount: each amount * 0.85, rounded to 2 decimals

ASSERT ROW_COUNT = 20
UPDATE {{zone_name}}.delta_demos.partitioned_orders
SET amount = ROUND(amount * 0.85, 2)
WHERE region = 'South';


-- ============================================================================
-- EXPLORE: Verify the South Discount
-- ============================================================================
-- Let's check specific South orders to confirm the 15% discount was applied.
-- We can reverse-calculate the original amount by dividing by 0.85:

ASSERT ROW_COUNT = 3
ASSERT VALUE discounted_amount = 110.50 WHERE id = 21
SELECT id, customer, product,
       amount AS discounted_amount,
       ROUND(amount / 0.85, 2) AS original_amount
FROM {{zone_name}}.delta_demos.partitioned_orders
WHERE region = 'South' AND id IN (21, 34, 37)
ORDER BY id;


-- ============================================================================
-- EXPLORE: South vs Unmodified Regions
-- ============================================================================
-- North and East were not touched by the UPDATE. Compare their averages
-- against the discounted South partition to see the effect:

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 20 WHERE region = 'North'
ASSERT VALUE order_count = 20 WHERE region = 'South'
ASSERT VALUE order_count = 20 WHERE region = 'East'
SELECT region,
       COUNT(*) AS order_count,
       ROUND(AVG(amount), 2) AS avg_order,
       ROUND(SUM(amount), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.partitioned_orders
WHERE region IN ('North', 'South', 'East')
GROUP BY region
ORDER BY region;


-- ============================================================================
-- LEARN: Partition-Scoped DELETE — Cancel West Orders Under $50
-- ============================================================================
-- This DELETE combines the partition column (region = 'West') with a data
-- column predicate (amount < 50). Delta first prunes to the West partition,
-- then scans only those files to find rows matching amount < 50.
--
-- West orders < $50: id=65($42), id=70($29), id=75($36), id=80($24) = 4 deleted
-- West remaining: 20 - 4 = 16

ASSERT ROW_COUNT = 4
DELETE FROM {{zone_name}}.delta_demos.partitioned_orders
WHERE region = 'West' AND amount < 50;


-- ============================================================================
-- EXPLORE: Verify West After DELETE
-- ============================================================================
-- All remaining West orders should have amounts >= $50. The 4 small orders
-- (ids 65, 70, 75, 80) should be gone:

ASSERT ROW_COUNT = 16
SELECT id, customer, product, amount
FROM {{zone_name}}.delta_demos.partitioned_orders
WHERE region = 'West'
ORDER BY amount;


-- ============================================================================
-- EXPLORE: Cross-Partition Product Analysis
-- ============================================================================
-- Even though data is physically separated by region, queries can still
-- aggregate across all partitions. The query engine reads from all partition
-- directories and combines the results:

ASSERT ROW_COUNT = 5
ASSERT VALUE total_revenue = 5102.5 WHERE product = 'Widget B'
ASSERT VALUE total_orders = 12 WHERE product = 'Tool Z'
SELECT product,
       COUNT(*) AS total_orders,
       COUNT(*) FILTER (WHERE region = 'North') AS north,
       COUNT(*) FILTER (WHERE region = 'South') AS south,
       COUNT(*) FILTER (WHERE region = 'East') AS east,
       COUNT(*) FILTER (WHERE region = 'West') AS west,
       ROUND(SUM(amount), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.partitioned_orders
GROUP BY product
ORDER BY total_revenue DESC;


-- ============================================================================
-- EXPLORE: Final Partition Overview
-- ============================================================================
-- Let's see the final state of all partitions after both the UPDATE and
-- DELETE operations:

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 20 WHERE region = 'East'
ASSERT VALUE order_count = 20 WHERE region = 'North'
ASSERT VALUE order_count = 20 WHERE region = 'South'
ASSERT VALUE order_count = 16 WHERE region = 'West'
SELECT region,
       COUNT(*) AS order_count,
       ROUND(SUM(amount), 2) AS total_revenue,
       ROUND(AVG(amount), 2) AS avg_order,
       ROUND(MIN(amount), 2) AS min_order,
       ROUND(MAX(amount), 2) AS max_order
FROM {{zone_name}}.delta_demos.partitioned_orders
GROUP BY region
ORDER BY region;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 80 - 4 deleted from West = 76
ASSERT ROW_COUNT = 76
SELECT * FROM {{zone_name}}.delta_demos.partitioned_orders;

-- Verify north_count: North partition untouched at 20 rows
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_orders WHERE region = 'North';

-- Verify south_count: South partition has 20 rows (discounted, not deleted)
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_orders WHERE region = 'South';

-- Verify east_count: East partition untouched at 20 rows
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_orders WHERE region = 'East';

-- Verify west_count: West partition has 16 rows after deleting 4 under $50
ASSERT VALUE cnt = 16
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_orders WHERE region = 'West';

-- Verify south_discount_check: id=21 South order discounted 15% to 110.50
ASSERT VALUE amount = 110.50
SELECT amount FROM {{zone_name}}.delta_demos.partitioned_orders WHERE id = 21;

-- Verify west_deleted_gone: 4 small West orders removed
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_orders WHERE id IN (65, 70, 75, 80);
