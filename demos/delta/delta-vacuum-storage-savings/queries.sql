-- ============================================================================
-- Delta VACUUM — Storage Cost Savings — Educational Queries
-- ============================================================================
-- WHAT: VACUUM RETAIN 0 HOURS removes ALL orphaned Parquet files immediately,
--       bypassing the default 7-day retention period.
-- WHY:  Copy-on-write DML (UPDATE, DELETE) orphans old files on every mutation.
--       On cloud storage (S3, ADLS, GCS), these orphans silently inflate costs.
--       This table disables deletion vectors so mutations truly rewrite whole
--       files, producing the orphans that make the reclaim real and non-zero.
-- HOW:  VACUUM DRY RUN previews the reclaim, then VACUUM RETAIN 0 HOURS frees
--       it. For a VACUUM, ASSERT ROW_COUNT equals the number of files reclaimed,
--       so we assert exactly 3 files on the preview and the real run (the direct
--       proof storage was reclaimed), then 0 on a final VACUUM (idempotent),
--       while every data check proves not one row was lost.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Pre-VACUUM storage footprint — DESCRIBE DETAIL
-- ============================================================================
-- The table has been through 5 DML operations (INSERT, 2 UPDATEs, DELETE,
-- INSERT) since creation. Each operation created new Parquet files via
-- copy-on-write, orphaning old versions. DESCRIBE DETAIL reveals how many
-- files currently exist on disk — including those no longer referenced.

-- DESCRIBE DETAIL emits one row per detail property. Its num_files property
-- counts only ACTIVE (referenced) files, so it is context here, not the
-- savings proof. VACUUM reclaims orphans, which never appear in this count.
-- The real proof is the VACUUM files-reclaimed count asserted below.
ASSERT ROW_COUNT >= 1
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
-- PREVIEW: VACUUM DRY RUN, how much can we reclaim?
-- ============================================================================
-- DRY RUN inspects the table WITHOUT deleting anything and reports the reclaim
-- it WOULD perform. Because the table forces copy-on-write (deletion vectors are
-- disabled), the three whole-file rewrites above (2 UPDATEs + 1 DELETE) each
-- orphaned the previous file version, so exactly 3 dead files sit on disk. For a
-- VACUUM statement ASSERT ROW_COUNT equals the number of files reclaimed, so
-- ROW_COUNT = 3 here is the headline proof that VACUUM has genuine storage to
-- free. (The 5-row late INSERT added a fresh file that is NOT orphaned.)

ASSERT ROW_COUNT = 3
VACUUM {{zone_name}}.delta_demos.billing_transactions RETAIN 0 HOURS DRY RUN;


-- ============================================================================
-- VACUUM RETAIN 0 HOURS — reclaim all orphaned storage immediately
-- ============================================================================
-- Default retention is 7 days, which protects time-travel queries to recent
-- versions. RETAIN 0 HOURS overrides this, physically deleting ALL files not
-- referenced by the current table version. Use this when you explicitly choose
-- storage savings over time-travel capability.
--
-- This time VACUUM actually deletes the files. ROW_COUNT again equals the number
-- of files reclaimed, so ROW_COUNT = 3 proves the three orphaned Parquet files
-- were physically removed from disk, not merely previewed. The logical table is
-- untouched (proven by the integrity checks below).

ASSERT ROW_COUNT = 3
VACUUM {{zone_name}}.delta_demos.billing_transactions RETAIN 0 HOURS;


-- ============================================================================
-- LEARN: Post-VACUUM storage footprint — orphans are gone
-- ============================================================================
-- DESCRIBE DETAIL after VACUUM shows the reduced file count. The logical
-- table version has not changed — VACUUM is a physical-only operation.
-- Only the files referenced by the current version remain on disk.

-- DESCRIBE DETAIL returns a single metadata row for the table. Its num_files
-- column counts only ACTIVE (referenced) files, so it is context here, not the
-- savings proof. VACUUM reclaims orphans, which never appear in this count.
-- The real proof is the VACUUM files-reclaimed count asserted just above.
ASSERT ROW_COUNT >= 1
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
-- LEARN: VACUUM is idempotent: a second pass reclaims nothing
-- ============================================================================
-- After the first VACUUM removed every orphan, only files referenced by the
-- current version remain. Running VACUUM again therefore reclaims ZERO files.
-- ROW_COUNT = 0 is the closing proof that the earlier reclaim was real: the
-- dead weight is gone and there is nothing left to delete.

ASSERT ROW_COUNT = 0
VACUUM {{zone_name}}.delta_demos.billing_transactions RETAIN 0 HOURS;


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
