-- ============================================================================
-- Avro Insurance Claims — Verification Queries
-- ============================================================================
-- Each query verifies a specific feature: schema evolution with NULL filling,
-- mixed codecs, file_filter, max_rows, file_metadata, and analytics.
-- ============================================================================


-- ============================================================================
-- 1. FULL SCAN — 90 claims across 3 Avro files
-- ============================================================================

ASSERT ROW_COUNT = 90
SELECT *
FROM {{zone_name}}.avro_insurance.all_claims;


-- ============================================================================
-- 2. SCHEMA EVOLUTION — NULL counts in v2-only columns
-- ============================================================================
-- adjuster_name and settlement_date are NULL for all 60 v1 rows.
-- settlement_date is also NULL for Pending/Under Review v2 rows (14 total).
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE null_adjuster = 60
ASSERT VALUE null_settlement = 74
SELECT COUNT(*) - COUNT(adjuster_name) AS null_adjuster,
       COUNT(*) - COUNT(settlement_date) AS null_settlement
FROM {{zone_name}}.avro_insurance.all_claims;


-- ============================================================================
-- 3. AUTO CLAIMS FILTER — 60 rows from 2 auto files
-- ============================================================================

ASSERT ROW_COUNT = 60
SELECT *
FROM {{zone_name}}.avro_insurance.auto_claims_only;


-- ============================================================================
-- 4. CLAIM TYPE BREAKDOWN — count and avg amount by type
-- ============================================================================

ASSERT ROW_COUNT = 8
ASSERT VALUE claim_count = 16 WHERE claim_type = 'Collision'
ASSERT VALUE claim_count = 16 WHERE claim_type = 'Comprehensive'
ASSERT VALUE claim_count = 14 WHERE claim_type = 'Liability'
ASSERT VALUE claim_count = 14 WHERE claim_type = 'Theft'
ASSERT VALUE claim_count = 8 WHERE claim_type = 'Property Damage'
ASSERT VALUE claim_count = 8 WHERE claim_type = 'Water Damage'
ASSERT VALUE claim_count = 7 WHERE claim_type = 'Fire'
ASSERT VALUE claim_count = 7 WHERE claim_type = 'Wind'
SELECT claim_type,
       COUNT(*) AS claim_count,
       ROUND(AVG(amount_claimed), 2) AS avg_claimed
FROM {{zone_name}}.avro_insurance.all_claims
GROUP BY claim_type
ORDER BY claim_type;


-- ============================================================================
-- 5. STATUS DISTRIBUTION — count and sum approved by status
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE claim_count = 24 WHERE status = 'Approved'
ASSERT VALUE claim_count = 24 WHERE status = 'Denied'
ASSERT VALUE claim_count = 21 WHERE status = 'Pending'
ASSERT VALUE claim_count = 21 WHERE status = 'Under Review'
-- Non-deterministic: SUM over doubles may vary at the sub-cent level across engines; use range
ASSERT WARNING VALUE sum_approved BETWEEN 502164.47 AND 502164.49 WHERE status = 'Approved'
ASSERT VALUE sum_approved = 0.0 WHERE status = 'Denied'
SELECT status,
       COUNT(*) AS claim_count,
       ROUND(SUM(amount_approved), 2) AS sum_approved
FROM {{zone_name}}.avro_insurance.all_claims
GROUP BY status
ORDER BY status;


-- ============================================================================
-- 6. APPROVAL RATE — approved vs denied statistics
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE approved_count = 24
ASSERT VALUE denied_count = 24
ASSERT VALUE approval_rate = 50.0
SELECT SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) AS approved_count,
       SUM(CASE WHEN status = 'Denied' THEN 1 ELSE 0 END) AS denied_count,
       ROUND(SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) * 100.0
             / NULLIF(SUM(CASE WHEN status IN ('Approved','Denied') THEN 1 ELSE 0 END), 0), 1) AS approval_rate
FROM {{zone_name}}.avro_insurance.all_claims;


-- ============================================================================
-- 7. SAMPLED CLAIMS — 45 rows (15 per file x 3 files)
-- ============================================================================

ASSERT ROW_COUNT = 45
SELECT *
FROM {{zone_name}}.avro_insurance.sampled_claims;


-- ============================================================================
-- VERIFY: Grand totals
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 90
-- Non-deterministic: SUM over doubles may vary at the sub-cent level across engines; use range
ASSERT WARNING VALUE sum_claimed BETWEEN 1824426.88 AND 1824426.90
-- Non-deterministic: SUM over doubles may vary at the sub-cent level across engines; use range
ASSERT WARNING VALUE sum_approved BETWEEN 502164.47 AND 502164.49
ASSERT VALUE distinct_statuses = 4
ASSERT VALUE null_adjuster_count = 60
SELECT COUNT(*) AS total_rows,
       ROUND(SUM(amount_claimed), 2) AS sum_claimed,
       ROUND(SUM(amount_approved), 2) AS sum_approved,
       COUNT(DISTINCT status) AS distinct_statuses,
       COUNT(*) - COUNT(adjuster_name) AS null_adjuster_count
FROM {{zone_name}}.avro_insurance.all_claims;
