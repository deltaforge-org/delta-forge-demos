-- ============================================================================
-- Delta VACUUM — Storage Cost Savings — Educational Queries
-- ============================================================================
-- WHAT: VACUUM RETAIN 0 HOURS removes ALL orphaned Parquet files immediately,
--       bypassing the default 7-day retention period.
-- WHY:  Copy-on-write DML (UPDATE, DELETE) orphans old files on every mutation.
--       On cloud storage (S3, ADLS, GCS), these orphans silently inflate costs.
-- HOW:  Use DESCRIBE DETAIL to measure file counts before and after VACUUM,
--       quantifying the exact storage reclaimed — while proving every row of
--       data remains perfectly intact.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Pre-VACUUM storage footprint — DESCRIBE DETAIL
-- ============================================================================
-- The table has been through 5 DML operations (INSERT, 2 UPDATEs, DELETE,
-- INSERT) since creation. Each operation created new Parquet files via
-- copy-on-write, orphaning old versions. DESCRIBE DETAIL reveals how many
-- files currently exist on disk — including those no longer referenced.

-- Non-deterministic: num_files depends on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.billing_transactions;


-- ============================================================================
-- EXPLORE: Revenue by plan — snapshot before VACUUM
-- ============================================================================
-- Capture the billing summary BEFORE VACUUM runs. After VACUUM, these exact
-- numbers must be identical — VACUUM only affects physical storage, never
-- logical data. This is the "before" half of our integrity proof.

ASSERT VALUE transaction_count = 14 WHERE plan = 'Enterprise'
ASSERT VALUE transaction_count = 9 WHERE plan = 'Pro'
ASSERT VALUE transaction_count = 9 WHERE plan = 'Starter'
ASSERT ROW_COUNT = 3
SELECT plan,
       COUNT(*) AS transaction_count,
       ROUND(SUM(amount), 2) AS total_revenue,
       ROUND(AVG(amount), 2) AS avg_amount
FROM {{zone_name}}.delta_demos.billing_transactions
GROUP BY plan
ORDER BY plan;


-- ============================================================================
-- EXPLORE: Monthly revenue breakdown — the financial snapshot
-- ============================================================================
-- Three months of billing with different mutation patterns:
--   January: 9 transactions (1 deleted, 5 refunded)
--   February: 8 transactions (2 deleted)
--   March: 15 transactions (10 original + 5 late additions)

ASSERT VALUE transactions = 9 WHERE billing_month = '2025-01'
ASSERT VALUE transactions = 8 WHERE billing_month = '2025-02'
ASSERT VALUE transactions = 15 WHERE billing_month = '2025-03'
ASSERT ROW_COUNT = 3
SELECT billing_month,
       COUNT(*) AS transactions,
       ROUND(SUM(amount), 2) AS revenue
FROM {{zone_name}}.delta_demos.billing_transactions
GROUP BY billing_month
ORDER BY billing_month;


-- ============================================================================
-- EXPLORE: Refunded transactions — status mutations created orphans
-- ============================================================================
-- Each refund UPDATE rewrote the Parquet file containing that row, orphaning
-- the old file with the 'active' status. Five refunds = at least 5 orphaned
-- file versions (possibly fewer if rows shared the same file).

ASSERT ROW_COUNT = 5
ASSERT VALUE total_refunded = 285.0
SELECT id, customer, plan, amount, status,
       SUM(amount) OVER () AS total_refunded
FROM {{zone_name}}.delta_demos.billing_transactions
WHERE status = 'refunded'
ORDER BY id;


-- ============================================================================
-- VACUUM RETAIN 0 HOURS — reclaim all orphaned storage immediately
-- ============================================================================
-- Default retention is 7 days, which protects time-travel queries to recent
-- versions. RETAIN 0 HOURS overrides this, removing ALL files not referenced
-- by the current table version. Use this when you explicitly choose storage
-- savings over time-travel capability.

VACUUM {{zone_name}}.delta_demos.billing_transactions RETAIN 0 HOURS;


-- ============================================================================
-- LEARN: Post-VACUUM storage footprint — orphans are gone
-- ============================================================================
-- DESCRIBE DETAIL after VACUUM shows the reduced file count. The logical
-- table version has not changed — VACUUM is a physical-only operation.
-- Only the files referenced by the current version remain on disk.

-- Non-deterministic: num_files depends on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.billing_transactions;


-- ============================================================================
-- LEARN: Post-VACUUM data integrity — identical to pre-VACUUM
-- ============================================================================
-- The most important proof: VACUUM changed zero rows. Total transactions,
-- distinct customers, and total revenue are exactly the same as before.

ASSERT VALUE total_transactions = 32
ASSERT VALUE distinct_customers = 15
ASSERT VALUE total_revenue = 9185.9
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_transactions,
       COUNT(DISTINCT customer) AS distinct_customers,
       ROUND(SUM(amount), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.billing_transactions;


-- ============================================================================
-- LEARN: Plan breakdown unchanged — the "after" half of integrity proof
-- ============================================================================
-- Compare this directly to Query 2. Every count and revenue total is
-- identical. VACUUM only removed unreferenced physical files.

ASSERT VALUE transaction_count = 14 WHERE plan = 'Enterprise'
ASSERT VALUE transaction_count = 9 WHERE plan = 'Pro'
ASSERT VALUE transaction_count = 9 WHERE plan = 'Starter'
ASSERT ROW_COUNT = 3
SELECT plan,
       COUNT(*) AS transaction_count,
       ROUND(SUM(amount), 2) AS total_revenue,
       ROUND(AVG(amount), 2) AS avg_amount
FROM {{zone_name}}.delta_demos.billing_transactions
GROUP BY plan
ORDER BY plan;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 32
ASSERT ROW_COUNT = 32
SELECT * FROM {{zone_name}}.delta_demos.billing_transactions;

-- Verify Acme Corp Enterprise price after 15% increase
ASSERT VALUE amount = 573.85
SELECT amount FROM {{zone_name}}.delta_demos.billing_transactions WHERE id = 1;

-- Verify Coral Inc was refunded
ASSERT VALUE status = 'refunded'
SELECT status FROM {{zone_name}}.delta_demos.billing_transactions WHERE id = 3;

-- Verify cancelled transactions (ids 4, 14, 19) are gone
ASSERT VALUE cancelled_count = 0
SELECT COUNT(*) AS cancelled_count FROM {{zone_name}}.delta_demos.billing_transactions WHERE id IN (4, 14, 19);

-- Verify active transaction count
ASSERT VALUE active_count = 27
SELECT COUNT(*) AS active_count FROM {{zone_name}}.delta_demos.billing_transactions WHERE status = 'active';

-- Verify refunded transaction count
ASSERT VALUE refunded_count = 5
SELECT COUNT(*) AS refunded_count FROM {{zone_name}}.delta_demos.billing_transactions WHERE status = 'refunded';

-- Verify late addition Kilo Systems present with Enterprise price
ASSERT VALUE amount = 573.85
SELECT amount FROM {{zone_name}}.delta_demos.billing_transactions WHERE id = 31;

-- Verify 3 distinct plans
ASSERT VALUE plan_count = 3
SELECT COUNT(DISTINCT plan) AS plan_count FROM {{zone_name}}.delta_demos.billing_transactions;
