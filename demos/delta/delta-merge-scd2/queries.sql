-- ============================================================================
-- Delta MERGE — SCD Type 2 (Slowly Changing Dimensions) — Educational Queries
-- ============================================================================
-- WHAT: SCD Type 2 preserves full history of dimension changes by expiring
--       old rows and inserting new versions, rather than updating in place.
-- WHY:  Insurance, finance, and regulated industries require complete audit
--       trails. SCD2 lets you answer "what was the policy state on date X?"
--       without losing any historical data.
-- HOW:  Two-pass approach — (1) MERGE to expire current rows that have
--       changes (is_current -> 0, valid_to -> day before effective date),
--       (2) INSERT to add new current versions from the changes table.
--       This is the standard real-world SCD2 pattern.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Current Policy Dimension
-- ============================================================================
-- All 15 policies start as current (is_current=1, valid_to='9999-12-31').
-- Each row represents the active version of a policy loaded on 2024-01-01.

ASSERT ROW_COUNT = 15
ASSERT VALUE is_current = 1 WHERE policy_id = 'POL-1001'
SELECT surrogate_key, policy_id, holder_name, coverage_type,
       annual_premium, region, risk_score, valid_from, valid_to, is_current
FROM {{zone_name}}.delta_demos.policy_dim
ORDER BY surrogate_key;


-- ============================================================================
-- PREVIEW: Incoming Policy Changes
-- ============================================================================
-- 8 policy modifications arriving on 2025-01-15. These include coverage
-- upgrades, region changes, premium adjustments, and loyalty discounts.

ASSERT ROW_COUNT = 8
SELECT policy_id, holder_name, coverage_type, annual_premium,
       region, risk_score, effective_date
FROM {{zone_name}}.delta_demos.policy_changes
ORDER BY policy_id;


-- ============================================================================
-- MERGE: SCD2 Pass 1 — Expire Current Rows That Have Changes
-- ============================================================================
-- For each policy_id in the changes table, find the current version
-- (is_current=1) in the dimension table and expire it:
--   - Set valid_to to '2025-01-14' (day before the change takes effect)
--   - Set is_current to 0
--
-- This preserves the old row as a historical record. The MERGE matches on
-- both policy_id AND is_current=1 to ensure we only expire the active
-- version, not any already-expired historical rows.

ASSERT ROW_COUNT = 8
MERGE INTO {{zone_name}}.delta_demos.policy_dim AS target
USING {{zone_name}}.delta_demos.policy_changes AS source
ON target.policy_id = source.policy_id AND target.is_current = 1
WHEN MATCHED THEN
    UPDATE SET
        valid_to   = '2025-01-14',
        is_current = 0;


-- ============================================================================
-- MERGE: SCD2 Pass 2 — Insert New Current Versions
-- ============================================================================
-- Insert a new row for each changed policy with:
--   - New surrogate_key (15 + ROW_NUMBER based on policy_id order)
--   - The updated attributes from policy_changes
--   - valid_from = effective_date ('2025-01-15')
--   - valid_to = '9999-12-31' (open-ended, meaning "current")
--   - is_current = 1
--
-- This is the second half of the SCD2 pattern. Together with Pass 1,
-- we now have both the expired historical row and the new current row
-- for each changed policy.

INSERT INTO {{zone_name}}.delta_demos.policy_dim
SELECT 15 + ROW_NUMBER() OVER (ORDER BY policy_id),
       policy_id, holder_name, coverage_type, annual_premium, region, risk_score,
       effective_date, '9999-12-31', 1
FROM {{zone_name}}.delta_demos.policy_changes;


-- ============================================================================
-- EXPLORE: Full History After SCD2
-- ============================================================================
-- The dimension table now contains 23 rows: 15 original + 8 new versions.
-- For each changed policy you can see two rows — the expired historical
-- version and the new current version. Unchanged policies still have
-- exactly one row with is_current=1.

ASSERT ROW_COUNT = 23
SELECT surrogate_key, policy_id, holder_name, coverage_type,
       annual_premium, region, risk_score, valid_from, valid_to, is_current
FROM {{zone_name}}.delta_demos.policy_dim
ORDER BY policy_id, valid_from;


-- ============================================================================
-- LEARN: Expired vs Current Records
-- ============================================================================
-- After SCD2, we should have:
--   - 15 current records (7 untouched originals + 8 newly inserted)
--   - 8 expired records (the old versions of changed policies)

ASSERT ROW_COUNT = 2
ASSERT VALUE record_count = 15 WHERE record_status = 'current'
ASSERT VALUE record_count = 8 WHERE record_status = 'expired'
SELECT CASE WHEN is_current = 1 THEN 'current' ELSE 'expired' END AS record_status,
       COUNT(*) AS record_count
FROM {{zone_name}}.delta_demos.policy_dim
GROUP BY is_current
ORDER BY is_current DESC;


-- ============================================================================
-- EXPLORE: History Trail for a Specific Policy
-- ============================================================================
-- POL-1001 (Alice Johnson) upgraded from standard to premium coverage.
-- The dimension table now has two rows for this policy:
--   1. Expired row: standard coverage, valid 2024-01-01 to 2025-01-14
--   2. Current row: premium coverage, valid from 2025-01-15 onward

ASSERT ROW_COUNT = 2
ASSERT VALUE coverage_type = 'standard' WHERE valid_to = '2025-01-14'
ASSERT VALUE coverage_type = 'premium' WHERE valid_to = '9999-12-31'
SELECT surrogate_key, policy_id, holder_name, coverage_type,
       annual_premium, region, risk_score, valid_from, valid_to, is_current
FROM {{zone_name}}.delta_demos.policy_dim
WHERE policy_id = 'POL-1001'
ORDER BY valid_from;


-- ============================================================================
-- EXPLORE: Current Portfolio Summary
-- ============================================================================
-- Summary of the current policy portfolio by coverage type.
-- Only current records (is_current=1) are included — this is how you
-- would build a reporting view on top of an SCD2 dimension.

ASSERT ROW_COUNT = 4
ASSERT VALUE policy_count = 2 WHERE coverage_type = 'basic'
ASSERT VALUE policy_count = 5 WHERE coverage_type = 'standard'
ASSERT VALUE policy_count = 5 WHERE coverage_type = 'premium'
ASSERT VALUE policy_count = 3 WHERE coverage_type = 'platinum'
ASSERT VALUE total_premium = 2700.00 WHERE coverage_type = 'basic'
ASSERT VALUE total_premium = 11800.00 WHERE coverage_type = 'standard'
ASSERT VALUE total_premium = 23700.00 WHERE coverage_type = 'premium'
ASSERT VALUE total_premium = 21800.00 WHERE coverage_type = 'platinum'
SELECT coverage_type,
       COUNT(*) AS policy_count,
       ROUND(SUM(annual_premium), 2) AS total_premium,
       ROUND(AVG(annual_premium), 2) AS avg_premium
FROM {{zone_name}}.delta_demos.policy_dim
WHERE is_current = 1
GROUP BY coverage_type
ORDER BY avg_premium;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 15 original + 8 new versions = 23
ASSERT ROW_COUNT = 23
SELECT * FROM {{zone_name}}.delta_demos.policy_dim;

-- Verify expired_count: 8 rows expired by the MERGE
ASSERT VALUE cnt = 8
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.policy_dim WHERE is_current = 0;

-- Verify current_count: 7 untouched + 8 new = 15 current
ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.policy_dim WHERE is_current = 1;

-- Verify no_invalid_current: no current record should have a closed valid_to
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.policy_dim WHERE is_current = 1 AND valid_to != '9999-12-31';

-- Verify new_records_date: all 8 new versions have valid_from = '2025-01-15'
ASSERT VALUE cnt = 8
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.policy_dim WHERE valid_from = '2025-01-15';

-- Verify alice_upgrade: Alice upgraded from standard to premium
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.policy_dim WHERE policy_id = 'POL-1001' AND coverage_type = 'premium' AND is_current = 1;

-- Verify alice_history: Alice has exactly 2 rows (expired + current)
ASSERT VALUE cnt = 2
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.policy_dim WHERE policy_id = 'POL-1001';

-- Verify untouched_policy: POL-1002 (Bob) was not changed, still has 1 row
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.policy_dim WHERE policy_id = 'POL-1002' AND is_current = 1 AND valid_to = '9999-12-31';

-- Verify expired_valid_to: all expired rows have valid_to = '2025-01-14'
ASSERT VALUE cnt = 8
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.policy_dim WHERE is_current = 0 AND valid_to = '2025-01-14';

-- Verify surrogate_keys: new rows have surrogate_keys 16-23
ASSERT VALUE cnt = 8
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.policy_dim WHERE surrogate_key BETWEEN 16 AND 23;
