-- ============================================================================
-- Delta MERGE — Subquery & CTE Source Patterns — Educational Queries
-- ============================================================================
-- WHAT: MERGE INTO using a CTE (Common Table Expression) as the source,
--       combining deduplication and aggregation before the merge.
-- WHY:  In real analytics pipelines, raw event streams contain duplicates
--       (at-least-once delivery) and must be aggregated before upserting
--       into summary tables. A CTE lets you express this transformation
--       inline — no temp tables, no multi-step ETL, one atomic operation.
-- HOW:  The CTE first deduplicates raw events by order_id (GROUP BY keeps
--       the latest timestamp), then aggregates by product+date to compute
--       daily totals. The MERGE uses this aggregated result as its source,
--       updating existing summary rows and inserting new ones.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Daily Revenue Summary (Target Table)
-- ============================================================================
-- The daily_revenue table has 15 existing rows: 5 products x 3 days
-- (2024-03-01 through 2024-03-03). This is the target for our MERGE.

ASSERT ROW_COUNT = 15
SELECT product_id, product_name, sale_date, total_revenue, order_count,
       avg_order_value, last_updated
FROM {{zone_name}}.delta_demos.daily_revenue
ORDER BY product_id, sale_date;


-- ============================================================================
-- PREVIEW: Raw Order Events (Source Table)
-- ============================================================================
-- Today's incoming events: 40 rows total, including 5 duplicate events
-- from at-least-once delivery. Notice some order_ids appear twice with
-- slightly different timestamps (e.g., ORD-301 at 14:22:10 and 14:22:11).

ASSERT ROW_COUNT = 40
SELECT event_id, product_id, order_id, quantity, unit_price,
       quantity * unit_price AS line_total, event_timestamp, channel, region
FROM {{zone_name}}.delta_demos.order_events
ORDER BY event_timestamp, event_id;


-- ============================================================================
-- EXPLORE: Identify Duplicate Events
-- ============================================================================
-- At-least-once delivery means the same order can arrive multiple times.
-- Here we find the 5 order_ids that have duplicate events. Each appears
-- exactly twice — the real order and its duplicate.

ASSERT ROW_COUNT = 5
SELECT order_id, COUNT(*) AS event_count,
       MIN(event_timestamp) AS first_seen,
       MAX(event_timestamp) AS last_seen
FROM {{zone_name}}.delta_demos.order_events
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY order_id;


-- ============================================================================
-- LEARN: The CTE — Deduplicate Then Aggregate
-- ============================================================================
-- This is the transformation that will feed the MERGE. Two stages:
--
--   1. deduped: GROUP BY order_id + product fields + quantity + unit_price,
--      taking MAX(event_timestamp). This collapses duplicate events into
--      one row per unique order, keeping the latest timestamp.
--      Result: 40 events → 35 unique orders.
--
--   2. daily_agg: GROUP BY product_id + sale_date to compute daily totals.
--      Result: 35 orders → 10 product+date buckets
--      (5 products on 2024-03-03 + 5 products on 2024-03-04).
--
-- Run this standalone to see exactly what the MERGE source looks like:

ASSERT ROW_COUNT = 10
WITH deduped AS (
    SELECT order_id, product_id, product_name, quantity, unit_price,
           MAX(event_timestamp) AS event_timestamp
    FROM {{zone_name}}.delta_demos.order_events
    GROUP BY order_id, product_id, product_name, quantity, unit_price
),
daily_agg AS (
    SELECT product_id,
           product_name,
           SUBSTRING(event_timestamp, 1, 10) AS sale_date,
           ROUND(SUM(quantity * unit_price), 2) AS batch_revenue,
           COUNT(*) AS batch_orders,
           MAX(event_timestamp) AS latest_event
    FROM deduped
    GROUP BY product_id, product_name, SUBSTRING(event_timestamp, 1, 10)
)
SELECT * FROM daily_agg ORDER BY product_id, sale_date;


-- ============================================================================
-- MERGE: Upsert Daily Revenue from CTE
-- ============================================================================
-- The full MERGE statement embeds the same CTE as the USING source.
--
--   WHEN MATCHED (product+date already in summary):
--     - Add batch_revenue to existing total_revenue
--     - Add batch_orders to existing order_count
--     - Recalculate avg_order_value from new totals
--     - Update last_updated to the latest event timestamp
--
--   WHEN NOT MATCHED (new product+date combo):
--     - Insert a fresh summary row with batch totals
--
-- Result: 5 updates (2024-03-03 rows) + 5 inserts (2024-03-04 rows) = 10

ASSERT ROW_COUNT = 10
MERGE INTO {{zone_name}}.delta_demos.daily_revenue AS target
USING (
    WITH deduped AS (
        SELECT order_id, product_id, product_name, quantity, unit_price,
               MAX(event_timestamp) AS event_timestamp
        FROM {{zone_name}}.delta_demos.order_events
        GROUP BY order_id, product_id, product_name, quantity, unit_price
    ),
    daily_agg AS (
        SELECT product_id,
               product_name,
               SUBSTRING(event_timestamp, 1, 10) AS sale_date,
               ROUND(SUM(quantity * unit_price), 2) AS batch_revenue,
               COUNT(*) AS batch_orders,
               MAX(event_timestamp) AS latest_event
        FROM deduped
        GROUP BY product_id, product_name, SUBSTRING(event_timestamp, 1, 10)
    )
    SELECT * FROM daily_agg
) AS source
ON target.product_id = source.product_id AND target.sale_date = source.sale_date
WHEN MATCHED THEN
    UPDATE SET total_revenue = target.total_revenue + source.batch_revenue,
               order_count = target.order_count + source.batch_orders,
               avg_order_value = ROUND((target.total_revenue + source.batch_revenue) / (target.order_count + source.batch_orders), 2),
               last_updated = source.latest_event
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, sale_date, total_revenue, order_count, avg_order_value, last_updated)
    VALUES (source.product_id, source.product_name, source.sale_date, source.batch_revenue,
            source.batch_orders, ROUND(source.batch_revenue / source.batch_orders, 2), source.latest_event);


-- ============================================================================
-- EXPLORE: Verify Updated Summaries (2024-03-03)
-- ============================================================================
-- The MERGE updated 5 existing rows for 2024-03-03. Revenue was ADDED
-- (not replaced), so we can verify: new total = original + batch.
--
-- Expected values after update:
--   PROD-001: 319.94 + 199.97 = 519.91, orders: 5+2=7, avg: 74.27
--   PROD-002: 949.96 + 349.98 = 1299.94, orders: 5+2=7, avg: 185.71
--   PROD-003: 149.95 + 159.95 = 309.90, orders: 5+2=7, avg: 44.27
--   PROD-004: 269.97 + 269.97 = 539.94, orders: 3+2=5, avg: 107.99
--   PROD-005: 382.47 + 379.97 = 762.44, orders: 3+2=5, avg: 152.49

ASSERT ROW_COUNT = 5
ASSERT VALUE total_revenue = 519.91 WHERE product_id = 'PROD-001'
ASSERT VALUE total_revenue = 1299.94 WHERE product_id = 'PROD-002'
ASSERT VALUE total_revenue = 309.90 WHERE product_id = 'PROD-003'
ASSERT VALUE total_revenue = 539.94 WHERE product_id = 'PROD-004'
ASSERT VALUE total_revenue = 762.44 WHERE product_id = 'PROD-005'
SELECT product_id, product_name, total_revenue, order_count, avg_order_value, last_updated
FROM {{zone_name}}.delta_demos.daily_revenue
WHERE sale_date = '2024-03-03'
ORDER BY product_id;


-- ============================================================================
-- EXPLORE: Verify New Day Entries (2024-03-04)
-- ============================================================================
-- The MERGE inserted 5 new rows for 2024-03-04 — one per product.
-- These are fresh summary rows computed entirely from today's events.

ASSERT ROW_COUNT = 5
ASSERT VALUE total_revenue = 469.92 WHERE product_id = 'PROD-001'
ASSERT VALUE total_revenue = 1159.94 WHERE product_id = 'PROD-002'
ASSERT VALUE total_revenue = 389.88 WHERE product_id = 'PROD-003'
ASSERT VALUE total_revenue = 729.92 WHERE product_id = 'PROD-004'
ASSERT VALUE total_revenue = 904.93 WHERE product_id = 'PROD-005'
SELECT product_id, product_name, total_revenue, order_count, avg_order_value, last_updated
FROM {{zone_name}}.delta_demos.daily_revenue
WHERE sale_date = '2024-03-04'
ORDER BY product_id;


-- ============================================================================
-- EXPLORE: Verify Duplicates Were Handled
-- ============================================================================
-- Raw events had 40 rows but only 35 unique orders. The CTE deduplication
-- ensured that duplicate events were not double-counted. We can verify by
-- checking total order_count across all summary rows.
--
-- Original order_count across 15 rows: 6+7+5+4+3+5+6+7+5+4+5+3+4+6+3 = 73
-- Added from 2024-03-03 updates: 2+2+2+2+2 = 10
-- Added from 2024-03-04 inserts: 5+5+5+5+5 = 25
-- Expected total: 73 + 10 + 25 = 108
-- If duplicates were NOT handled, we would see 113 (5 extra orders)

ASSERT VALUE total_orders = 108
SELECT SUM(order_count) AS total_orders
FROM {{zone_name}}.delta_demos.daily_revenue;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify row_count: 15 original + 5 new (2024-03-04) = 20 total rows
ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.delta_demos.daily_revenue;

-- Verify no_duplicate_combos: each product+date appears exactly once
ASSERT VALUE max_dupes = 1
SELECT MAX(cnt) AS max_dupes
FROM (SELECT product_id, sale_date, COUNT(*) AS cnt
      FROM {{zone_name}}.delta_demos.daily_revenue
      GROUP BY product_id, sale_date);

-- Verify prod001_mar03_revenue: original 319.94 + batch 199.97 = 519.91
ASSERT VALUE total_revenue = 519.91
SELECT total_revenue FROM {{zone_name}}.delta_demos.daily_revenue
WHERE product_id = 'PROD-001' AND sale_date = '2024-03-03';

-- Verify prod002_mar03_revenue: original 949.96 + batch 349.98 = 1299.94
ASSERT VALUE total_revenue = 1299.94
SELECT total_revenue FROM {{zone_name}}.delta_demos.daily_revenue
WHERE product_id = 'PROD-002' AND sale_date = '2024-03-03';

-- Verify prod003_mar04_revenue: new insert = 389.88
ASSERT VALUE total_revenue = 389.88
SELECT total_revenue FROM {{zone_name}}.delta_demos.daily_revenue
WHERE product_id = 'PROD-003' AND sale_date = '2024-03-04';

-- Verify prod005_mar04_orders: 5 orders from new day
ASSERT VALUE order_count = 5
SELECT order_count FROM {{zone_name}}.delta_demos.daily_revenue
WHERE product_id = 'PROD-005' AND sale_date = '2024-03-04';

-- Verify untouched_rows: 2024-03-01 and 2024-03-02 rows unchanged (10 rows)
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.daily_revenue
WHERE sale_date IN ('2024-03-01', '2024-03-02') AND last_updated = '2024-03-04 00:00:00';

-- Verify total_orders_no_duplicates: 108 total (not 113)
ASSERT VALUE total_orders = 108
SELECT SUM(order_count) AS total_orders FROM {{zone_name}}.delta_demos.daily_revenue;

-- Verify mar04_all_products: all 5 products have entries for the new day
ASSERT VALUE product_count = 5
SELECT COUNT(DISTINCT product_id) AS product_count
FROM {{zone_name}}.delta_demos.daily_revenue
WHERE sale_date = '2024-03-04';
