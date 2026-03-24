-- ============================================================================
-- Delta DML Patterns — INSERT, UPDATE & DELETE — Queries
-- ============================================================================
-- WHAT: Delta Lake enables INSERT, UPDATE, and DELETE on Parquet-based tables
--       by recording file-level add/remove actions in the transaction log.
-- WHY:  Traditional data lakes (raw Parquet, ORC) are append-only — you cannot
--       update a price, delete a cancelled order, or fix a typo without
--       rewriting entire files manually. Delta makes these operations atomic.
-- HOW:  UPDATE and DELETE read affected data files, apply changes, write new
--       files, and atomically commit "remove old file + add new file" actions
--       to the Delta log. Readers see a consistent snapshot at every version.
-- ============================================================================
--
-- This script performs 6 DML operations on the 60-row order_history table,
-- with SELECT queries between each to observe the effects:
--   1. INSERT INTO...SELECT — archive cancelled orders to order_archive (8 archived)
--   2. DELETE — purge those archived cancelled orders (8 removed → 52)
--   3. UPDATE — bulk fulfillment: pending us-east orders → shipped (6 updated)
--   4. UPDATE — tiered price discount using CASE: 15%/10%/5% for electronics (10 updated)
--   5. DELETE — archive old completed orders before 2024-01-01 (5 removed → 47)
--   6. UPDATE — zero-match: attempt to update non-existent region (0 updated)
--
-- Final row count: 60 - 8 - 5 = 47
-- ============================================================================


-- ============================================================================
-- BASELINE: Inspect order_history before any DML
-- ============================================================================
-- 60 rows across 4 regions, 4 statuses. Let's see the starting distribution.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_value = 4488.96 WHERE status = 'completed'
SELECT status, COUNT(*) AS order_count,
       ROUND(SUM(price * qty), 2) AS total_value
FROM {{zone_name}}.delta_demos.order_history
GROUP BY status
ORDER BY total_value DESC;


-- ============================================================================
-- DML 1: INSERT INTO...SELECT — Archive cancelled orders before purging
-- ============================================================================
-- Before deleting old cancelled orders, we archive them to order_archive.
-- This is a common real-world pattern: preserve data for auditing or
-- compliance before removing it from the operational table.
--
-- INSERT INTO...SELECT is Delta's way of copying rows between tables
-- atomically. The source query can include any valid SQL — filters, joins,
-- aggregations — not just literal VALUES.
--
-- Archives 8 rows: ids 1-8 (cancelled with order_date < 2024-06-01)

ASSERT ROW_COUNT = 8
INSERT INTO {{zone_name}}.delta_demos.order_archive
SELECT * FROM {{zone_name}}.delta_demos.order_history
WHERE status = 'cancelled' AND order_date < '2024-06-01';

-- Confirm: 8 rows in archive, order_history still has all 60
ASSERT VALUE archive_count = 8
SELECT COUNT(*) AS archive_count FROM {{zone_name}}.delta_demos.order_archive;

ASSERT VALUE source_count = 60
SELECT COUNT(*) AS source_count FROM {{zone_name}}.delta_demos.order_history;


-- ============================================================================
-- DML 2: DELETE — Purge cancelled orders older than 2024-06-01
-- ============================================================================
-- Multi-predicate DELETE: status='cancelled' AND order_date < '2024-06-01'.
-- Now that these orders are safely archived, we can purge them from the
-- operational table. This is more surgical than deleting all cancelled
-- orders — recent cancellations might still be useful for analytics.
--
-- In the Delta log, this DELETE scans data files for matching rows, rewrites
-- only the affected files (omitting the deleted rows), and records remove/add
-- actions atomically.
--
-- Removes 8 rows: ids 1-8
-- Running total: 60 → 52

ASSERT ROW_COUNT = 8
DELETE FROM {{zone_name}}.delta_demos.order_history
WHERE status = 'cancelled' AND order_date < '2024-06-01';

-- Confirm the old cancelled orders are gone while recent ones remain.
-- 12 original cancelled - 8 old = 4 recent cancelled remain
ASSERT ROW_COUNT = 4
SELECT id, customer, product, status, order_date
FROM {{zone_name}}.delta_demos.order_history
WHERE status = 'cancelled'
ORDER BY order_date;

-- Running total: 52
ASSERT VALUE total = 52
SELECT COUNT(*) AS total FROM {{zone_name}}.delta_demos.order_history;


-- ============================================================================
-- DML 3: UPDATE — Bulk fulfillment for pending us-east orders
-- ============================================================================
-- All pending orders in us-east are bulk-updated to 'shipped'. This pattern
-- simulates a warehouse fulfillment event where an entire region's backlog
-- is shipped at once.
--
-- Because Delta commits are atomic, either all 6 orders are updated or none
-- are — there is no risk of a partial update leaving some orders in an
-- inconsistent state.
--
-- Updates 6 rows: ids 14, 15, 16, 17, 18, 19

ASSERT ROW_COUNT = 6
UPDATE {{zone_name}}.delta_demos.order_history
SET status = 'shipped'
WHERE status = 'pending' AND region = 'us-east';

-- Confirm all us-east orders that were pending are now shipped.
-- Verify no pending orders remain in us-east
ASSERT VALUE pending_us_east = 0
SELECT COUNT(*) AS pending_us_east FROM {{zone_name}}.delta_demos.order_history WHERE status = 'pending' AND region = 'us-east';

ASSERT ROW_COUNT = 6
SELECT id, customer, product, status, region
FROM {{zone_name}}.delta_demos.order_history
WHERE region = 'us-east' AND status = 'shipped'
ORDER BY id;

-- Running total: still 52 (UPDATE doesn't change row count)
ASSERT VALUE total = 52
SELECT COUNT(*) AS total FROM {{zone_name}}.delta_demos.order_history;


-- ============================================================================
-- DML 4: UPDATE — Tiered price discount for electronics using CASE
-- ============================================================================
-- Electronics products: Laptop, Monitor, Tablet, Headphones, Smartwatch.
-- Instead of a flat discount, we apply tiered pricing based on the original
-- price — a common pattern in real-world pricing adjustments:
--
--   price > 500   → 15% off (premium tier)
--   price >= 200  → 10% off (mid-range tier)
--   price < 200   →  5% off (accessories tier)
--
-- This CASE-based UPDATE touches rows across multiple regions and statuses,
-- showing that Delta DML predicates can span the entire table.
--
-- Updates 10 rows: ids 14, 15, 19, 20, 21, 22, 23, 24, 25, 26
--   Laptop     999.99 → 849.99  (15% off, premium)
--   Tablet     499.99 → 449.99  (10% off, mid-range)
--   Monitor    349.99 → 314.99  (10% off, mid-range)
--   Smartwatch 249.99 → 224.99  (10% off, mid-range)
--   Headphones 199.99 → 189.99  ( 5% off, accessories)

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.order_history
SET price = CASE
    WHEN price > 500  THEN ROUND(price * 0.85, 2)
    WHEN price >= 200 THEN ROUND(price * 0.90, 2)
    ELSE                   ROUND(price * 0.95, 2)
END
WHERE product IN ('Laptop', 'Monitor', 'Tablet', 'Headphones', 'Smartwatch');

-- Verify one price from each tier:
--   Premium (15%):     Laptop     999.99 → 849.99
--   Mid-range (10%):   Monitor    349.99 → 314.99
--   Accessories (5%):  Headphones 199.99 → 189.99
ASSERT VALUE price = 849.99
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 20;

ASSERT VALUE price = 314.99
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 21;

ASSERT VALUE price = 189.99
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 24;

-- Show all electronics with their discount tier
ASSERT ROW_COUNT = 10
SELECT id, customer, product, price,
       CASE
           WHEN product = 'Laptop'     THEN 999.99
           WHEN product = 'Monitor'    THEN 349.99
           WHEN product = 'Tablet'     THEN 499.99
           WHEN product = 'Headphones' THEN 199.99
           WHEN product = 'Smartwatch' THEN 249.99
       END AS original_price,
       CASE
           WHEN product = 'Laptop' THEN '15% (premium)'
           WHEN product = 'Headphones' THEN '5% (accessories)'
           ELSE '10% (mid-range)'
       END AS discount_tier
FROM {{zone_name}}.delta_demos.order_history
WHERE product IN ('Laptop', 'Monitor', 'Tablet', 'Headphones', 'Smartwatch')
ORDER BY product, id;


-- ============================================================================
-- DML 5: DELETE — Archive old completed orders before 2024-01-01
-- ============================================================================
-- Removes completed orders with order_date < '2024-01-01'. In a production
-- system these rows would be archived first (as we did with cancelled orders
-- in DML 1); here we simply delete to demonstrate a second DELETE pattern.
--
-- Removes 5 rows: ids 9, 10, 11, 12, 13
-- Running total: 52 → 47

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.order_history
WHERE status = 'completed' AND order_date < '2024-01-01';

-- Confirm: no completed orders before 2024 remain
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.order_history
WHERE status = 'completed' AND order_date < '2024-01-01';

-- Running total: 47
ASSERT VALUE total = 47
SELECT COUNT(*) AS total FROM {{zone_name}}.delta_demos.order_history;


-- ============================================================================
-- DML 6: UPDATE — Zero-match edge case
-- ============================================================================
-- What happens when an UPDATE predicate matches no rows? Delta handles this
-- gracefully — no data files are rewritten, and no new commit is created.
-- This is important to understand: zero-match DML is a no-op, not an error.

ASSERT ROW_COUNT = 0
UPDATE {{zone_name}}.delta_demos.order_history
SET status = 'flagged'
WHERE region = 'ap-north';

-- Confirm: still exactly 47 rows, and no 'flagged' status exists
ASSERT VALUE total = 47
SELECT COUNT(*) AS total FROM {{zone_name}}.delta_demos.order_history;

ASSERT VALUE flagged = 0
SELECT COUNT(*) AS flagged FROM {{zone_name}}.delta_demos.order_history WHERE status = 'flagged';


-- ============================================================================
-- EXPLORE: Regional Order Summary After All DML Operations
-- ============================================================================
-- After all 6 DML operations, let's see how orders are distributed across
-- the 4 regions. All regions should still be represented, even though
-- DELETEs removed rows from each. Final: 60 - 8 - 5 = 47 rows.

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 13 WHERE region = 'eu-west'
SELECT region,
       COUNT(*) AS order_count,
       SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
       SUM(CASE WHEN status = 'shipped' THEN 1 ELSE 0 END) AS shipped,
       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
FROM {{zone_name}}.delta_demos.order_history
GROUP BY region
ORDER BY region;


-- ============================================================================
-- EXPLORE: Non-Electronics Products Are Unaffected
-- ============================================================================
-- The tiered price discount UPDATE only targeted electronics. Non-electronics
-- products should retain their original prices, demonstrating that Delta
-- UPDATE predicates are precise — only matching rows are modified.

ASSERT ROW_COUNT = 10
ASSERT VALUE min_price = 150.00 WHERE product = 'Desk'
SELECT product, COUNT(*) AS orders,
       MIN(price) AS min_price, MAX(price) AS max_price
FROM {{zone_name}}.delta_demos.order_history
WHERE product NOT IN ('Laptop', 'Monitor', 'Tablet', 'Headphones', 'Smartwatch')
GROUP BY product
ORDER BY product;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 60 - 8 - 5 = 47 rows remain
ASSERT ROW_COUNT = 47
SELECT * FROM {{zone_name}}.delta_demos.order_history;

-- Verify archive_populated: 8 cancelled orders were archived before deletion
ASSERT VALUE cnt = 8
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.order_archive;

-- Verify cancelled_old_gone: cancelled orders before 2024-06-01 were purged
ASSERT VALUE cnt = 0
SELECT COUNT(*) FILTER (WHERE status = 'cancelled' AND order_date < '2024-06-01') AS cnt FROM {{zone_name}}.delta_demos.order_history;

-- Verify us_east_shipped: 6 pending us-east orders were bulk-shipped
ASSERT VALUE cnt = 6
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.order_history WHERE status = 'shipped' AND region = 'us-east';

-- Verify tiered_premium: id=20 Laptop discounted 15% to 849.99
ASSERT VALUE price = 849.99
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 20;

-- Verify tiered_midrange: id=21 Monitor discounted 10% to 314.99
ASSERT VALUE price = 314.99
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 21;

-- Verify tiered_accessories: id=24 Headphones discounted 5% to 189.99
ASSERT VALUE price = 189.99
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 24;

-- Verify old_completed_gone: completed orders before 2024-01-01 were archived
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.order_history WHERE status = 'completed' AND order_date < '2024-01-01';

-- Verify zero_match: no 'flagged' status was introduced by the no-op UPDATE
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.order_history WHERE status = 'flagged';

-- Verify region_count: all 4 regions still represented
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT region) AS cnt FROM {{zone_name}}.delta_demos.order_history;

-- Verify remaining_pending: 8 pending orders remain after bulk shipment
ASSERT VALUE cnt = 8
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.order_history WHERE status = 'pending';

-- Verify non_electronics_unchanged: non-electronics price unaffected
ASSERT VALUE price = 120.00
SELECT price FROM {{zone_name}}.delta_demos.order_history WHERE id = 28;
