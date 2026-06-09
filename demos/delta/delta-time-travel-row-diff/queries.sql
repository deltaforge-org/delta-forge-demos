-- ============================================================================
-- Delta Time Travel — Row-Level Change Detection — Educational Queries
-- ============================================================================
-- WHAT: Detect exactly which rows were modified, deleted, and inserted across
--       Delta table versions using self-JOINs and LEFT JOINs on VERSION AS OF.
-- WHY:  Change Data Feed (CDF) must be enabled *before* changes happen. When
--       it wasn't, time travel is your only tool for answering "what changed?"
--       This is critical for incident investigation, ETL validation, and
--       regulatory audits.
-- HOW:  Self-JOIN across two versions on the primary key. Mismatched columns
--       reveal modifications. LEFT JOINs with NULL checks reveal deletions
--       and insertions.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Current State After All Changes
-- ============================================================================
-- The order table has been through multiple ETL runs. Let's see where things
-- stand now — 3 status groups (cancelled orders were purged in V5).

ASSERT VALUE order_count = 8 WHERE status = 'delivered'
ASSERT VALUE order_count = 9 WHERE status = 'shipped'
ASSERT VALUE order_count = 5 WHERE status = 'pending'
ASSERT ROW_COUNT = 3
SELECT status,
       COUNT(*) AS order_count,
       ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.ecom_orders
GROUP BY status
ORDER BY status;


-- ============================================================================
-- LEARN: Detecting Modified Rows — Self-JOIN Across Versions
-- ============================================================================
-- The core technique: JOIN the same table at two different versions on the
-- primary key, then filter for rows where any column differs. This reveals
-- every row the ETL batch touched.
--
-- V1 = after initial INSERT (20 orders, original statuses/prices)
-- V2 = after first UPDATE batch (status changes began)
-- We JOIN V1 to V4 (after all updates completed) to catch all modifications.

ASSERT VALUE old_status = 'shipped' WHERE order_id = 3
ASSERT VALUE new_status = 'delivered' WHERE order_id = 3
ASSERT VALUE old_price = 45.0 WHERE order_id = 3
ASSERT VALUE new_price = 49.99 WHERE order_id = 3
ASSERT VALUE old_status = 'pending' WHERE order_id = 5
ASSERT VALUE new_status = 'shipped' WHERE order_id = 5
ASSERT ROW_COUNT = 7
SELECT v1.order_id,
       v1.status    AS old_status,
       v4.status    AS new_status,
       v1.unit_price AS old_price,
       v4.unit_price AS new_price
FROM {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 1 v1
JOIN {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 4 v4
  ON v1.order_id = v4.order_id
WHERE v1.status != v4.status
   OR v1.unit_price != v4.unit_price
ORDER BY v1.order_id;


-- ============================================================================
-- LEARN: Detecting Deleted Rows — LEFT JOIN with NULL Check
-- ============================================================================
-- A LEFT JOIN from an older version to a newer one, filtering for NULL on the
-- right side, reveals rows that existed before but were removed. Here we find
-- the 3 cancelled orders purged between V4 and V5.

ASSERT VALUE order_id = 16 WHERE customer = 'Paul King'
ASSERT VALUE order_id = 17 WHERE customer = 'Quinn Wright'
ASSERT VALUE order_id = 18 WHERE customer = 'Rita Scott'
ASSERT ROW_COUNT = 3
SELECT v4.order_id, v4.customer, v4.product, v4.status
FROM {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 4 v4
LEFT JOIN {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 5 v5
  ON v4.order_id = v5.order_id
WHERE v5.order_id IS NULL
ORDER BY v4.order_id;


-- ============================================================================
-- LEARN: Detecting Inserted Rows — Reverse LEFT JOIN
-- ============================================================================
-- Flip the JOIN direction: LEFT JOIN from the newer version to the older one.
-- Rows with NULL on the older side are new insertions. These are the 5 new
-- pending orders added in V6.

ASSERT VALUE customer = 'Uma Patel' WHERE order_id = 21
ASSERT ROW_COUNT = 5
SELECT v6.order_id, v6.customer, v6.product, v6.status
FROM {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 6 v6
LEFT JOIN {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 5 v5
  ON v6.order_id = v5.order_id
WHERE v5.order_id IS NULL
ORDER BY v6.order_id;


-- ============================================================================
-- EXPLORE: Change Summary — All Mutations at a Glance
-- ============================================================================
-- Combine all three detection techniques into one result set. This is the
-- kind of query an analyst runs first to scope the damage after a bad ETL run.

ASSERT VALUE row_count = 7 WHERE change_type = 'Modified (V1 vs V4)'
ASSERT VALUE row_count = 3 WHERE change_type = 'Deleted (V4 vs V5)'
ASSERT VALUE row_count = 5 WHERE change_type = 'Inserted (V5 vs V6)'
ASSERT ROW_COUNT = 3
SELECT 'Modified (V1 vs V4)' AS change_type, COUNT(*) AS row_count
FROM {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 1 v1
JOIN {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 4 v4
  ON v1.order_id = v4.order_id
WHERE v1.status != v4.status OR v1.unit_price != v4.unit_price
UNION ALL
SELECT 'Deleted (V4 vs V5)', COUNT(*)
FROM {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 4 v4
LEFT JOIN {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 5 v5
  ON v4.order_id = v5.order_id
WHERE v5.order_id IS NULL
UNION ALL
SELECT 'Inserted (V5 vs V6)', COUNT(*)
FROM {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 6 v6
LEFT JOIN {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 5 v5
  ON v6.order_id = v5.order_id
WHERE v5.order_id IS NULL;


-- ============================================================================
-- EXPLORE: Revenue Impact — Before vs. After
-- ============================================================================
-- The ultimate question: did the ETL changes affect the bottom line?
-- Compare total revenue at V1 (original 20 orders) vs current state
-- (22 orders after deletions and insertions).

-- Non-deterministic: ROUND(SUM(DOUBLE*INT), 2) may vary ±0.01 due to floating-point accumulation
ASSERT WARNING VALUE v1_revenue BETWEEN 3587.00 AND 3588.00
ASSERT WARNING VALUE current_revenue BETWEEN 3551.00 AND 3552.00
ASSERT ROW_COUNT = 1
SELECT ROUND(v1.total, 2)      AS v1_revenue,
       ROUND(cur.total, 2)     AS current_revenue,
       ROUND(cur.total - v1.total, 2) AS revenue_delta
FROM (SELECT SUM(quantity * unit_price) AS total
      FROM {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 1) v1,
     (SELECT SUM(quantity * unit_price) AS total
      FROM {{zone_name}}.delta_demos.ecom_orders) cur;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify current row count is 22 (20 - 3 deleted + 5 new)
ASSERT ROW_COUNT = 22
SELECT * FROM {{zone_name}}.delta_demos.ecom_orders;

-- Verify V1 had 20 rows
ASSERT VALUE v1_count = 20
SELECT COUNT(*) AS v1_count FROM {{zone_name}}.delta_demos.ecom_orders VERSION AS OF 1;

-- Verify order 3 price was corrected to 49.99
ASSERT VALUE unit_price = 49.99
SELECT unit_price FROM {{zone_name}}.delta_demos.ecom_orders WHERE order_id = 3;

-- Verify cancelled orders were removed
ASSERT VALUE cancelled_count = 0
SELECT COUNT(*) AS cancelled_count FROM {{zone_name}}.delta_demos.ecom_orders WHERE status = 'cancelled';

-- Verify 5 new orders exist (ids > 20)
ASSERT VALUE new_count = 5
SELECT COUNT(*) AS new_count FROM {{zone_name}}.delta_demos.ecom_orders WHERE order_id > 20;

-- Verify 3 statuses remain
ASSERT VALUE status_count = 3
SELECT COUNT(DISTINCT status) AS status_count FROM {{zone_name}}.delta_demos.ecom_orders;
