-- ============================================================================
-- Delta OPTIMIZE — Manual File Compaction & TARGET SIZE — Queries
-- ============================================================================
-- WHAT: OPTIMIZE compacts small Parquet files into larger, optimally-sized ones
-- WHY:  Daily batch INSERTs and DML operations create many small files that
--       degrade read performance due to excessive file-open overhead
-- HOW:  OPTIMIZE reads all small files and rewrites them as fewer, larger files.
--       TARGET SIZE controls the output file size (default 128 MB).
--
-- This demo verifies data integrity before compaction, runs OPTIMIZE, then
-- verifies the exact same data is returned after compaction — proving that
-- OPTIMIZE is a lossless operation that only changes physical file layout.
-- ============================================================================


-- ============================================================================
-- PRE-COMPACTION: Verify baseline data integrity
-- ============================================================================
-- Before running OPTIMIZE, capture the full state of the table. These values
-- must be identical after compaction — any difference means data loss.

-- Total row count and key dimensions
ASSERT VALUE total_orders = 80
ASSERT VALUE distinct_products = 15
ASSERT VALUE distinct_customers = 80
ASSERT VALUE distinct_dates = 8
ASSERT VALUE distinct_regions = 4
SELECT COUNT(*) AS total_orders,
       COUNT(DISTINCT product) AS distinct_products,
       COUNT(DISTINCT customer_id) AS distinct_customers,
       COUNT(DISTINCT order_date) AS distinct_dates,
       COUNT(DISTINCT region) AS distinct_regions
FROM {{zone_name}}.delta_demos.daily_orders;

-- Status breakdown
ASSERT VALUE completed_count = 72
ASSERT VALUE cancelled_count = 5
ASSERT VALUE refunded_count = 3
SELECT SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_count,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
       SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded_count
FROM {{zone_name}}.delta_demos.daily_orders;

-- Total revenue (quantity * unit_price) — the critical business metric
ASSERT VALUE total_revenue = 22515.8
SELECT ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.daily_orders;

-- Per-region order counts (must be perfectly balanced: 20 each)
ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 20 WHERE region = 'North'
ASSERT VALUE order_count = 20 WHERE region = 'South'
ASSERT VALUE order_count = 20 WHERE region = 'East'
ASSERT VALUE order_count = 20 WHERE region = 'West'
SELECT region,
       COUNT(*) AS order_count,
       ROUND(SUM(quantity * unit_price), 2) AS revenue
FROM {{zone_name}}.delta_demos.daily_orders
GROUP BY region
ORDER BY region;

-- Spot-check cancelled and refunded orders
ASSERT VALUE status = 'cancelled' WHERE order_id = 8
ASSERT VALUE status = 'cancelled' WHERE order_id = 23
ASSERT VALUE status = 'refunded' WHERE order_id = 15
ASSERT VALUE status = 'refunded' WHERE order_id = 39
SELECT order_id, product, status, unit_price
FROM {{zone_name}}.delta_demos.daily_orders
WHERE status != 'completed'
ORDER BY order_id;


-- ============================================================================
-- OPTIMIZE — Compact fragmented files
-- ============================================================================
-- The table currently has 8+ small Parquet files from daily batch INSERTs
-- plus additional fragments from UPDATE operations. OPTIMIZE merges them
-- into fewer, larger files without changing any data.
--
-- TARGET SIZE controls the output file size. The default is 128 MB, but
-- for this small demo table we omit it to let the engine choose optimally.

OPTIMIZE {{zone_name}}.delta_demos.daily_orders;


-- ============================================================================
-- POST-COMPACTION: Verify data integrity — must match pre-compaction exactly
-- ============================================================================
-- This is the critical proof that OPTIMIZE is lossless. Every value below
-- must be identical to the pre-compaction checks above.

-- Same total rows and dimensions
ASSERT VALUE total_orders = 80
ASSERT VALUE distinct_products = 15
ASSERT VALUE distinct_customers = 80
ASSERT VALUE distinct_dates = 8
SELECT COUNT(*) AS total_orders,
       COUNT(DISTINCT product) AS distinct_products,
       COUNT(DISTINCT customer_id) AS distinct_customers,
       COUNT(DISTINCT order_date) AS distinct_dates
FROM {{zone_name}}.delta_demos.daily_orders;

-- Same status breakdown
ASSERT VALUE completed_count = 72
ASSERT VALUE cancelled_count = 5
ASSERT VALUE refunded_count = 3
SELECT SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_count,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
       SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded_count
FROM {{zone_name}}.delta_demos.daily_orders;

-- Same total revenue — the most important business invariant
ASSERT VALUE total_revenue = 22515.8
SELECT ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.daily_orders;

-- Same per-region distribution
ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 20 WHERE region = 'North'
ASSERT VALUE order_count = 20 WHERE region = 'East'
SELECT region,
       COUNT(*) AS order_count,
       ROUND(SUM(quantity * unit_price), 2) AS revenue
FROM {{zone_name}}.delta_demos.daily_orders
GROUP BY region
ORDER BY region;

-- Same spot-check values — individual rows survived compaction
ASSERT VALUE unit_price = 999.99 WHERE order_id = 1
ASSERT VALUE unit_price = 349.99 WHERE order_id = 80
ASSERT VALUE status = 'cancelled' WHERE order_id = 56
ASSERT VALUE status = 'refunded' WHERE order_id = 72
SELECT order_id, product, quantity, unit_price, status
FROM {{zone_name}}.delta_demos.daily_orders
WHERE order_id IN (1, 56, 72, 80)
ORDER BY order_id;


-- ============================================================================
-- LEARN: Per-Day Revenue Analysis (post-compaction)
-- ============================================================================
-- After compaction the data is physically reorganized but logically identical.
-- Analytical queries run on the compacted files just as before.

ASSERT ROW_COUNT = 8
ASSERT VALUE revenue = 2051.83 WHERE order_date = '2025-03-10'
ASSERT VALUE revenue = 3319.86 WHERE order_date = '2025-03-11'
SELECT order_date,
       COUNT(*) AS orders,
       ROUND(SUM(quantity * unit_price), 2) AS revenue
FROM {{zone_name}}.delta_demos.daily_orders
GROUP BY order_date
ORDER BY order_date;


-- ============================================================================
-- LEARN: Top Products by Revenue
-- ============================================================================

ASSERT ROW_COUNT = 15
ASSERT VALUE revenue = 9199.92 WHERE product = 'Laptop'
ASSERT VALUE order_count = 8 WHERE product = 'Laptop'
SELECT product,
       COUNT(*) AS order_count,
       SUM(quantity) AS total_qty,
       ROUND(SUM(quantity * unit_price), 2) AS revenue
FROM {{zone_name}}.delta_demos.daily_orders
GROUP BY product
ORDER BY revenue DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Final cross-cutting verification after all operations.

-- Row count
ASSERT ROW_COUNT = 80
SELECT * FROM {{zone_name}}.delta_demos.daily_orders;

-- Business totals
ASSERT VALUE total_revenue = 22515.8
ASSERT VALUE completed_revenue = 21310.95
SELECT ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
       ROUND(SUM(CASE WHEN status = 'completed' THEN quantity * unit_price ELSE 0 END), 2) AS completed_revenue
FROM {{zone_name}}.delta_demos.daily_orders;

-- ID range integrity
ASSERT VALUE min_id = 1
ASSERT VALUE max_id = 80
SELECT MIN(order_id) AS min_id, MAX(order_id) AS max_id
FROM {{zone_name}}.delta_demos.daily_orders;

-- Region count
ASSERT VALUE region_count = 4
SELECT COUNT(DISTINCT region) AS region_count FROM {{zone_name}}.delta_demos.daily_orders;

-- Product count
ASSERT VALUE product_count = 15
SELECT COUNT(DISTINCT product) AS product_count FROM {{zone_name}}.delta_demos.daily_orders;
