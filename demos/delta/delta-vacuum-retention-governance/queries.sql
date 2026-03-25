-- ============================================================================
-- Delta VACUUM Retention Governance — Educational Queries
-- ============================================================================
-- WHAT: VACUUM's RETAIN parameter controls how much history survives on disk.
--       The audit trail in DESCRIBE HISTORY is permanent (log-based), but
--       the ability to actually READ old versions via VERSION AS OF depends
--       on whether VACUUM left the underlying Parquet files intact.
-- WHY:  Regulators may demand 90-day lookback on settlement data. If VACUUM
--       runs with RETAIN 0 HOURS, every old version's files are deleted and
--       VERSION AS OF queries fail. The governance decision: which retention
--       period balances storage cost against compliance requirements?
-- HOW:  Build a 4-version settlement lifecycle, then compare two VACUUM
--       strategies. DESCRIBE HISTORY proves the metadata audit trail survives
--       both strategies — only the data file availability differs.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — 35 pending settlements (V1)
-- ============================================================================
-- All settlements start as 'pending'. Five counterparties, five instrument
-- types, five traders. This is the initial snapshot that auditors might
-- request weeks from now.

ASSERT ROW_COUNT = 5
ASSERT VALUE settlement_count = 7 WHERE counterparty = 'Apex Capital'
ASSERT VALUE settlement_count = 7 WHERE counterparty = 'Vanguard Partners'
ASSERT VALUE total_exposure = 10600000.0 WHERE counterparty = 'Apex Capital'
SELECT counterparty,
       COUNT(*) AS settlement_count,
       ROUND(SUM(amount), 2) AS total_exposure
FROM {{zone_name}}.delta_demos.settlement_records
GROUP BY counterparty
ORDER BY counterparty;


-- ============================================================================
-- LEARN: Version 1 audit snapshot via DESCRIBE HISTORY
-- ============================================================================
-- DESCRIBE HISTORY shows every commit in the transaction log. At this point
-- there are only two entries: V0 (CREATE TABLE) and V1 (initial INSERT).
-- This metadata is PERMANENT — VACUUM never touches the transaction log.

-- Non-deterministic: DESCRIBE HISTORY may include extra internal versions
ASSERT WARNING ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.delta_demos.settlement_records;


-- ============================================================================
-- V2: UPDATE — 10 settlements move to 'settled' (ids 1-10)
-- ============================================================================
-- The first batch of settlements clears: 7 from Apex Capital and 3 from
-- Meridian Trust. Each UPDATE creates new Parquet files (copy-on-write)
-- and orphans the old files containing the pre-update rows.

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.settlement_records
SET status = 'settled'
WHERE id BETWEEN 1 AND 10;


-- ============================================================================
-- EXPLORE: Settlement status after first clearing
-- ============================================================================
-- 10 settled, 25 still pending. The settled total is the first batch of
-- confirmed trades that downstream systems can rely on.

ASSERT ROW_COUNT = 2
ASSERT VALUE rec_count = 10 WHERE status = 'settled'
ASSERT VALUE rec_count = 25 WHERE status = 'pending'
ASSERT VALUE total_amount = 16070000.0 WHERE status = 'settled'
SELECT status,
       COUNT(*) AS rec_count,
       ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.delta_demos.settlement_records
GROUP BY status
ORDER BY status;


-- ============================================================================
-- V3: UPDATE — 3 settlements marked 'failed' with 5% penalty (ids 24, 27, 30)
-- ============================================================================
-- Three settlements failed clearing. Per the counterparty agreement, failed
-- settlements incur a 5% reduction in settlement amount. This creates
-- another round of orphaned files from the copy-on-write rewrite.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.settlement_records
SET status = 'failed',
    amount = ROUND(amount * 0.95, 2)
WHERE id IN (24, 27, 30);


-- ============================================================================
-- EXPLORE: Three-way status breakdown after failures
-- ============================================================================
-- The portfolio now has three statuses: settled, failed, and pending.
-- Failed settlements have reduced amounts from the 5% penalty.

ASSERT ROW_COUNT = 3
ASSERT VALUE rec_count = 3 WHERE status = 'failed'
ASSERT VALUE rec_count = 22 WHERE status = 'pending'
ASSERT VALUE rec_count = 10 WHERE status = 'settled'
ASSERT VALUE total_amount = 10355000.0 WHERE status = 'failed'
SELECT status,
       COUNT(*) AS rec_count,
       ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.delta_demos.settlement_records
GROUP BY status
ORDER BY status;


-- ============================================================================
-- V4: DELETE — 5 cancelled Vanguard Partners settlements (ids 31-35)
-- ============================================================================
-- Vanguard Partners withdrew 5 settlements. The DELETE creates CDF records
-- (if enabled) and orphans the files containing these rows.

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.settlement_records
WHERE id BETWEEN 31 AND 35;


-- ============================================================================
-- EXPLORE: Final state — 30 active settlements
-- ============================================================================
-- After all transitions: 10 settled, 3 failed, 17 pending. The 5 cancelled
-- Vanguard entries are gone. This is the current "truth" that VACUUM must
-- preserve regardless of retention settings.

ASSERT ROW_COUNT = 3
ASSERT VALUE rec_count = 17 WHERE status = 'pending'
ASSERT VALUE rec_count = 10 WHERE status = 'settled'
ASSERT VALUE rec_count = 3 WHERE status = 'failed'
SELECT status,
       COUNT(*) AS rec_count,
       ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.delta_demos.settlement_records
GROUP BY status
ORDER BY status;


-- ============================================================================
-- LEARN: Pre-VACUUM audit trail — 5 versions of history
-- ============================================================================
-- The transaction log now records: V0 (CREATE), V1 (INSERT 35), V2 (UPDATE
-- 10 settled), V3 (UPDATE 3 failed), V4 (DELETE 5). Every version is fully
-- queryable via VERSION AS OF — the old data files are still on disk.

-- Non-deterministic: DESCRIBE HISTORY may include extra internal versions
ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.delta_demos.settlement_records;


-- ============================================================================
-- LEARN: Time travel to V1 — the original 35 pending settlements
-- ============================================================================
-- Before VACUUM, VERSION AS OF 1 returns the full original dataset. An
-- auditor requesting "show me the book as of the initial load" gets the
-- exact snapshot: 35 records, all pending, total exposure $62.58M.

ASSERT VALUE v1_total = 35
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS v1_total
FROM {{zone_name}}.delta_demos.settlement_records VERSION AS OF 1;


-- ============================================================================
-- ACTION: Safe VACUUM — default retention (168 hours / 7 days)
-- ============================================================================
-- With the default 7-day retention, VACUUM only removes orphaned files
-- OLDER than 7 days. Since all our versions were created moments ago,
-- nothing is old enough to qualify. This is the "safe" strategy: VACUUM
-- runs regularly but never removes files that might be needed for audit.
-- Result: zero files removed, all time travel versions still accessible.

VACUUM {{zone_name}}.delta_demos.settlement_records;


-- ============================================================================
-- LEARN: After safe VACUUM — time travel still works
-- ============================================================================
-- VERSION AS OF 1 still returns the original 35 rows. The safe retention
-- window preserved all historical data files. Auditors can still query
-- any version. This proves that VACUUM with default retention is
-- non-destructive for recently-created versions.

ASSERT VALUE v1_count = 35
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS v1_count
FROM {{zone_name}}.delta_demos.settlement_records VERSION AS OF 1;


-- ============================================================================
-- LEARN: Current data is also intact after safe VACUUM
-- ============================================================================
-- Regardless of retention settings, VACUUM never touches the current
-- version's files. The latest snapshot is always preserved.

ASSERT VALUE current_total = 30
ASSERT VALUE total_exposure = 53685000.0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS current_total,
       ROUND(SUM(amount), 2) AS total_exposure
FROM {{zone_name}}.delta_demos.settlement_records;


-- ============================================================================
-- ACTION: Aggressive VACUUM — RETAIN 0 HOURS
-- ============================================================================
-- This is the storage-first strategy: remove ALL orphaned files regardless
-- of age. After this command:
--   - Files for V0, V1, V2, V3 are deleted from disk
--   - Only V4's Parquet files survive
--   - VERSION AS OF 1..3 will fail (data files gone)
--   - The transaction log entries REMAIN (metadata is permanent)
--   - Current data is identical — only old versions are affected
--
-- In production, this is appropriate ONLY when:
--   - Compliance does not require historical lookback
--   - Storage costs outweigh audit capability
--   - The current snapshot is the only truth that matters

VACUUM {{zone_name}}.delta_demos.settlement_records RETAIN 0 HOURS;


-- ============================================================================
-- LEARN: Current data survives aggressive VACUUM
-- ============================================================================
-- The latest version is always untouched by VACUUM. Every settlement
-- record, every status transition, every amount adjustment is preserved
-- exactly as committed. Aggressive VACUUM destroys HISTORY, not CURRENT.

ASSERT VALUE total_records = 30
ASSERT VALUE total_exposure = 53685000.0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_records,
       ROUND(SUM(amount), 2) AS total_exposure
FROM {{zone_name}}.delta_demos.settlement_records;


-- ============================================================================
-- LEARN: DESCRIBE HISTORY survives aggressive VACUUM — permanent audit trail
-- ============================================================================
-- This is the critical governance insight: VACUUM removes DATA FILES, not
-- LOG ENTRIES. The transaction log still records every version — who did
-- what and when. An auditor can see that version 2 settled 10 trades and
-- version 3 marked 3 as failed, even though the data for those versions
-- is no longer queryable. The log serves as a permanent, immutable audit
-- trail independent of the data retention policy.

-- Non-deterministic: DESCRIBE HISTORY may include extra internal versions
ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.delta_demos.settlement_records;


-- ============================================================================
-- LEARN: Specific settlement integrity — key records preserved
-- ============================================================================
-- Spot-check critical records: a settled trade (id=1), a failed trade
-- with the 5% penalty (id=24), and a pending trade (id=15). These
-- survived both VACUUM passes because they are in the current version.

ASSERT VALUE status = 'settled' WHERE id = 1
ASSERT VALUE status = 'failed' WHERE id = 24
ASSERT VALUE status = 'pending' WHERE id = 15
ASSERT VALUE amount = 2500000.0 WHERE id = 1
ASSERT VALUE amount = 2660000.0 WHERE id = 24
SELECT id, trade_ref, counterparty, amount, status
FROM {{zone_name}}.delta_demos.settlement_records
WHERE id IN (1, 15, 24)
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total settlement count is 30 (35 - 5 cancelled)
ASSERT VALUE total_settlements = 30
SELECT COUNT(*) AS total_settlements FROM {{zone_name}}.delta_demos.settlement_records;

-- Verify total exposure is 53685000.0
ASSERT VALUE total_exposure = 53685000.0
SELECT ROUND(SUM(amount), 2) AS total_exposure FROM {{zone_name}}.delta_demos.settlement_records;

-- Verify 10 settled
ASSERT VALUE settled_count = 10
SELECT COUNT(*) AS settled_count FROM {{zone_name}}.delta_demos.settlement_records WHERE status = 'settled';

-- Verify 3 failed
ASSERT VALUE failed_count = 3
SELECT COUNT(*) AS failed_count FROM {{zone_name}}.delta_demos.settlement_records WHERE status = 'failed';

-- Verify 17 pending
ASSERT VALUE pending_count = 17
SELECT COUNT(*) AS pending_count FROM {{zone_name}}.delta_demos.settlement_records WHERE status = 'pending';

-- Verify cancelled Vanguard records are gone (ids 31-35)
ASSERT VALUE cancelled_count = 0
SELECT COUNT(*) AS cancelled_count FROM {{zone_name}}.delta_demos.settlement_records WHERE id BETWEEN 31 AND 35;

-- Verify 5 distinct counterparties remain
ASSERT VALUE counterparty_count = 5
SELECT COUNT(DISTINCT counterparty) AS counterparty_count FROM {{zone_name}}.delta_demos.settlement_records;

-- Verify failed settlement penalty applied (id=24: 2800000 * 0.95 = 2660000)
ASSERT VALUE amount = 2660000.0
SELECT amount FROM {{zone_name}}.delta_demos.settlement_records WHERE id = 24;
