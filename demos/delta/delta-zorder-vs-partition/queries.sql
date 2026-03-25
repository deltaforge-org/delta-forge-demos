-- ============================================================================
-- Delta Z-ORDER vs Partitioning — Choosing the Right Data Layout
-- ============================================================================
-- The #1 question in Delta table design: should I partition or Z-ORDER?
-- This demo runs the SAME queries against TWO tables with the same data:
--   1. orders_partitioned — PARTITIONED BY (customer_region)
--   2. orders_zorder      — Unpartitioned, OPTIMIZE ZORDER BY 3 columns
--
-- KEY INSIGHT:
--   Partition when you have a LOW-cardinality column you ALWAYS filter on.
--   Z-ORDER when you have MULTIPLE filter dimensions or HIGH-cardinality cols.
--   They are not mutually exclusive — you can partition AND Z-ORDER.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Data distribution across regions and categories
-- ============================================================================
-- Both tables contain identical data: 100 orders, 5 regions, 4 categories.
-- This query confirms the distribution is realistic (not perfectly uniform).

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 20
ASSERT RESULT SET INCLUDES ('europe', 'electronics', 6, 301.72)
SELECT customer_region, product_category,
       COUNT(*) AS order_count,
       ROUND(AVG(order_amount), 2) AS avg_amount
FROM {{zone_name}}.delta_demos.orders_zorder
GROUP BY customer_region, product_category
ORDER BY customer_region, product_category;


-- ============================================================================
-- Query 2: DESCRIBE DETAIL — Observe file layout before optimization
-- ============================================================================
-- The Z-ORDER table has 2 batch-insert files with data in insertion order.
-- No co-location of similar rows — every query scans everything.

ASSERT NO_FAIL IN result
DESCRIBE DETAIL {{zone_name}}.delta_demos.orders_zorder;


-- ============================================================================
-- Query 3: OPTIMIZE ZORDER BY (customer_region, product_category, order_date)
-- ============================================================================
-- Reorganize the Z-ORDER table using a space-filling curve across 3 columns.
-- This co-locates rows with similar region + category + date values in the
-- same files, enabling data skipping on ANY combination of these columns.

OPTIMIZE {{zone_name}}.delta_demos.orders_zorder
ZORDER BY (customer_region, product_category, order_date);


-- ============================================================================
-- Query 4: DESCRIBE DETAIL — File layout after Z-ORDER
-- ============================================================================
-- After OPTIMIZE ZORDER, the 2 batch files are compacted into fewer files
-- with tighter min/max statistics per file. DESCRIBE DETAIL shows the change.

ASSERT NO_FAIL IN result
DESCRIBE DETAIL {{zone_name}}.delta_demos.orders_zorder;


-- ============================================================================
-- Query 5: PARTITIONING WINS — Single partition-key filter
-- ============================================================================
-- When filtering ONLY on the partition key, partitioning is ideal: the engine
-- reads only the 'europe' partition directory and ignores the other 4 regions
-- entirely. This is partition pruning — zero scanning of irrelevant data.
-- Z-ORDER can skip files too, but partition pruning is absolute.

ASSERT ROW_COUNT = 22
SELECT id, order_id, product_category, order_amount, order_date
FROM {{zone_name}}.delta_demos.orders_partitioned
WHERE customer_region = 'europe'
ORDER BY order_date, product_category;


-- ============================================================================
-- Query 6: Z-ORDER — Same single-column filter for comparison
-- ============================================================================
-- The Z-ORDER table returns the same 22 rows. With tight min/max stats from
-- Z-ORDER, the engine can skip files that don't contain 'europe'. Not as
-- absolute as partition pruning, but effective without directory overhead.

ASSERT ROW_COUNT = 22
SELECT id, order_id, product_category, order_amount, order_date
FROM {{zone_name}}.delta_demos.orders_zorder
WHERE customer_region = 'europe'
ORDER BY order_date, product_category;


-- ============================================================================
-- Query 7: Z-ORDER WINS — Multi-column ad-hoc filter
-- ============================================================================
-- This is where Z-ORDER shines. Filtering on product_category AND order_date
-- crosses ALL partitions in the partitioned table — partition pruning cannot
-- help because we're not filtering on customer_region. The engine must scan
-- every partition directory.
-- Z-ORDER co-locates (category, date) values in the same files, so data
-- skipping works across both columns simultaneously.

ASSERT ROW_COUNT = 22
ASSERT VALUE total_revenue = 5623.4
SELECT COUNT(*) AS order_count,
       ROUND(SUM(order_amount), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.orders_zorder
WHERE product_category = 'electronics'
  AND order_date >= '2025-02-01';


-- ============================================================================
-- Query 8: PARTITIONED — Same cross-column filter (partition pruning useless)
-- ============================================================================
-- Same query on the partitioned table. Since we filter on product_category
-- and order_date (neither is the partition key), the engine must scan ALL 5
-- partition directories. The partitioning adds overhead here (directory
-- listing + per-partition file opens) with no pruning benefit.

ASSERT ROW_COUNT = 22
ASSERT VALUE total_revenue = 5623.4
SELECT COUNT(*) AS order_count,
       ROUND(SUM(order_amount), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.orders_partitioned
WHERE product_category = 'electronics'
  AND order_date >= '2025-02-01';


-- ============================================================================
-- Query 9: Z-ORDER WINS — Multi-column point query (the sweet spot)
-- ============================================================================
-- Filtering on region + category + date range uses ALL 3 Z-ORDER columns.
-- The space-filling curve ensures these values are co-located. With tight
-- file-level min/max stats, the engine can skip most files immediately.
-- On the partitioned table, only region would be pruned — category and date
-- still require scanning within the partition.

ASSERT ROW_COUNT = 4
SELECT id, order_id, order_amount, order_date
FROM {{zone_name}}.delta_demos.orders_zorder
WHERE customer_region = 'asia-pacific'
  AND product_category = 'electronics'
  AND order_date BETWEEN '2025-02-01' AND '2025-02-28'
ORDER BY order_date;


-- ============================================================================
-- Query 10: HIGH CARDINALITY — Z-ORDER handles dates without directory explosion
-- ============================================================================
-- Z-ORDER includes order_date as the 3rd column. With 20 distinct dates,
-- partitioning by date would create 20 directories (potentially with small
-- files in each). Z-ORDER handles high-cardinality columns gracefully —
-- dates are interleaved with region and category in the Z-curve, giving
-- date-range queries data skipping without directory overhead.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 20
ASSERT RESULT SET INCLUDES ('2025-02-15', 7, 119.01)
SELECT order_date, COUNT(*) AS orders,
       ROUND(AVG(order_amount), 2) AS avg_amount
FROM {{zone_name}}.delta_demos.orders_zorder
GROUP BY order_date
ORDER BY order_date;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Both tables have 100 rows
ASSERT VALUE zorder_count = 100
SELECT COUNT(*) AS zorder_count FROM {{zone_name}}.delta_demos.orders_zorder;

ASSERT VALUE partitioned_count = 100
SELECT COUNT(*) AS partitioned_count FROM {{zone_name}}.delta_demos.orders_partitioned;

-- 5 distinct regions
ASSERT VALUE region_count = 5
SELECT COUNT(DISTINCT customer_region) AS region_count FROM {{zone_name}}.delta_demos.orders_zorder;

-- 4 distinct categories
ASSERT VALUE category_count = 4
SELECT COUNT(DISTINCT product_category) AS category_count FROM {{zone_name}}.delta_demos.orders_zorder;

-- 20 distinct dates
ASSERT VALUE date_count = 20
SELECT COUNT(DISTINCT order_date) AS date_count FROM {{zone_name}}.delta_demos.orders_zorder;

-- North-america has the most orders
ASSERT VALUE na_count = 33
SELECT COUNT(*) AS na_count FROM {{zone_name}}.delta_demos.orders_zorder WHERE customer_region = 'north-america';

-- Total revenue matches across both tables
ASSERT VALUE total_revenue = 14941.06
SELECT ROUND(SUM(order_amount), 2) AS total_revenue FROM {{zone_name}}.delta_demos.orders_zorder;

-- Average quantity
ASSERT VALUE avg_qty = 2.1
SELECT ROUND(AVG(quantity), 1) AS avg_qty FROM {{zone_name}}.delta_demos.orders_zorder;
