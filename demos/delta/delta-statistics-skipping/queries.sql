-- ============================================================================
-- Data Skipping — Range Statistics in Action — Educational Queries
-- ============================================================================
-- WHAT: Delta stores per-file min/max statistics for every column in the
--       transaction log. Each INSERT batch creates a separate Parquet file.
-- WHY:  When a query filters by value range (WHERE unit_price >= 500), the
--       engine checks file-level stats BEFORE reading any data. If a file's
--       max value is below the filter threshold, it is skipped entirely.
-- HOW:  Statistics are written into each Add action in the _delta_log JSON.
--       Non-overlapping value ranges across files maximize skipping potential.
-- ============================================================================


-- ============================================================================
-- Query 1: Non-Overlapping Price Ranges Per Batch
-- ============================================================================
-- Three monthly batches were inserted separately. Each batch creates its own
-- Parquet file with distinct price ranges:
--   Jan (ids 1-15):  unit_price [10.99 - 95.00]
--   Feb (ids 16-30): unit_price [100.00 - 475.00]
--   Mar (ids 31-45): unit_price [500.00 - 2000.00]
--
-- These non-overlapping ranges are the foundation of effective data skipping.

ASSERT VALUE min_price = 10.99 WHERE month = 'Jan'
ASSERT VALUE max_price = 95.0 WHERE month = 'Jan'
ASSERT VALUE min_price = 100.0 WHERE month = 'Feb'
ASSERT VALUE max_price = 475.0 WHERE month = 'Feb'
ASSERT VALUE min_price = 500.0 WHERE month = 'Mar'
ASSERT VALUE max_price = 2000.0 WHERE month = 'Mar'
ASSERT VALUE order_count = 15 WHERE month = 'Jan'
ASSERT VALUE order_count = 15 WHERE month = 'Feb'
ASSERT VALUE order_count = 15 WHERE month = 'Mar'
ASSERT ROW_COUNT = 3
SELECT
    CASE
        WHEN id BETWEEN 1 AND 15 THEN 'Jan'
        WHEN id BETWEEN 16 AND 30 THEN 'Feb'
        ELSE 'Mar'
    END AS month,
    MIN(unit_price) AS min_price,
    MAX(unit_price) AS max_price,
    COUNT(*) AS order_count
FROM {{zone_name}}.skipping_demos.orders
GROUP BY CASE
    WHEN id BETWEEN 1 AND 15 THEN 'Jan'
    WHEN id BETWEEN 16 AND 30 THEN 'Feb'
    ELSE 'Mar'
END
ORDER BY month;


-- ============================================================================
-- Query 2: Data Skipping in Action — High-Value Orders
-- ============================================================================
-- WHERE unit_price >= 500.0 triggers file-level stats check:
--   - Jan file: max = 95.00   -> SKIP (95 < 500)
--   - Feb file: max = 475.00  -> SKIP (475 < 500)
--   - Mar file: min = 500.00  -> READ (overlaps the filter range)
--
-- Result: only 1 of 3 files is read. On tables with thousands of files,
-- this optimization can skip 90%+ of the data.

ASSERT VALUE high_value_count = 15
ASSERT VALUE min_high = 500.0
ASSERT VALUE max_high = 2000.0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS high_value_count,
       MIN(unit_price) AS min_high,
       MAX(unit_price) AS max_high
FROM {{zone_name}}.skipping_demos.orders
WHERE unit_price >= 500.0;


-- ============================================================================
-- Query 3: Zero Range Overlap Between Batches
-- ============================================================================
-- Non-overlapping ranges are key to effective skipping. If values from
-- different batches overlapped, the engine would need to read multiple
-- files for narrow range filters. This verifies clean separation.

ASSERT VALUE cross_batch_overlap = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS cross_batch_overlap
FROM {{zone_name}}.skipping_demos.orders
WHERE id > 15 AND unit_price < 100.0;


-- ============================================================================
-- Query 4: Category Breakdown Within a Single Batch
-- ============================================================================
-- Filtering unit_price <= 95.0 targets only Batch 1 (Jan). The engine skips
-- Feb and Mar files entirely. Within Batch 1, we see the category mix.

ASSERT VALUE order_count = 4 WHERE category = 'clothing'
ASSERT VALUE order_count = 4 WHERE category = 'electronics'
ASSERT VALUE order_count = 4 WHERE category = 'groceries'
ASSERT VALUE order_count = 3 WHERE category = 'home'
ASSERT ROW_COUNT = 4
SELECT category,
       COUNT(*) AS order_count,
       ROUND(AVG(unit_price), 2) AS avg_price
FROM {{zone_name}}.skipping_demos.orders
WHERE unit_price <= 95.0
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 5: Revenue Per Batch
-- ============================================================================
-- Line totals (unit_price * quantity) show the revenue distribution. Higher
-- price tiers in later months drive larger totals despite equal order counts.

ASSERT VALUE revenue = 1017.93 WHERE batch = 'Jan (ids 1-15)'
ASSERT VALUE revenue = 4653.2 WHERE batch = 'Feb (ids 16-30)'
ASSERT VALUE revenue = 17050.47 WHERE batch = 'Mar (ids 31-45)'
ASSERT ROW_COUNT = 3
SELECT
    CASE
        WHEN id BETWEEN 1 AND 15 THEN 'Jan (ids 1-15)'
        WHEN id BETWEEN 16 AND 30 THEN 'Feb (ids 16-30)'
        ELSE 'Mar (ids 31-45)'
    END AS batch,
    ROUND(SUM(line_total), 2) AS revenue,
    COUNT(*) AS orders
FROM {{zone_name}}.skipping_demos.orders
GROUP BY CASE
    WHEN id BETWEEN 1 AND 15 THEN 'Jan (ids 1-15)'
    WHEN id BETWEEN 16 AND 30 THEN 'Feb (ids 16-30)'
    ELSE 'Mar (ids 31-45)'
END
ORDER BY batch;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 45
ASSERT ROW_COUNT = 45
SELECT * FROM {{zone_name}}.skipping_demos.orders;

-- Verify Batch 1 min price is 10.99
ASSERT VALUE batch1_min = 10.99
SELECT MIN(unit_price) AS batch1_min FROM {{zone_name}}.skipping_demos.orders WHERE id BETWEEN 1 AND 15;

-- Verify Batch 2 max price is 475.0
ASSERT VALUE batch2_max = 475.0
SELECT MAX(unit_price) AS batch2_max FROM {{zone_name}}.skipping_demos.orders WHERE id BETWEEN 16 AND 30;

-- Verify Batch 3 max price is 2000.0
ASSERT VALUE batch3_max = 2000.0
SELECT MAX(unit_price) AS batch3_max FROM {{zone_name}}.skipping_demos.orders WHERE id BETWEEN 31 AND 45;

-- Verify 15 high-value orders (>= 500.0)
ASSERT VALUE high_count = 15
SELECT COUNT(*) AS high_count FROM {{zone_name}}.skipping_demos.orders WHERE unit_price >= 500.0;

-- Verify no range overlap between batches
ASSERT VALUE overlap = 0
SELECT COUNT(*) AS overlap FROM {{zone_name}}.skipping_demos.orders WHERE id > 15 AND unit_price < 100.0;

-- Verify electronics revenue
ASSERT VALUE electronics_revenue = 10526.9
SELECT ROUND(SUM(line_total), 2) AS electronics_revenue FROM {{zone_name}}.skipping_demos.orders WHERE category = 'electronics';
