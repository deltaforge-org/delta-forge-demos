-- ============================================================================
-- Demo: Regulatory Compliance Recovery — RESTORE with UniForm
-- ============================================================================
-- Simulates an accidental deletion on a compliance table, then uses RESTORE
-- to recover. Proves the Iceberg metadata is rebuilt correctly after restore.

-- ============================================================================
-- Query 1: Baseline — All 20 Compliance Records (Version 1)
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.compliance_records ORDER BY record_id;

-- ============================================================================
-- Query 2: Baseline Status Distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE record_count = 10 WHERE compliance_status = 'compliant'
ASSERT VALUE record_count = 5 WHERE compliance_status = 'non_compliant'
ASSERT VALUE record_count = 5 WHERE compliance_status = 'partial'
ASSERT VALUE total_risk = 174 WHERE compliance_status = 'compliant'
ASSERT VALUE total_risk = 360 WHERE compliance_status = 'non_compliant'
ASSERT VALUE total_risk = 231 WHERE compliance_status = 'partial'
SELECT
    compliance_status,
    COUNT(*) AS record_count,
    SUM(risk_score) AS total_risk
FROM {{zone_name}}.iceberg_demos.compliance_records
GROUP BY compliance_status
ORDER BY compliance_status;

-- ============================================================================
-- Query 3: Per-Entity Risk Profile
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_risk = 16.25 WHERE entity_name = 'Acme Corp'
ASSERT VALUE avg_risk = 58.25 WHERE entity_name = 'Beta Inc'
ASSERT VALUE avg_risk = 36.25 WHERE entity_name = 'Delta Co'
ASSERT VALUE avg_risk = 67.00 WHERE entity_name = 'Epsilon SA'
ASSERT VALUE avg_risk = 13.50 WHERE entity_name = 'Gamma LLC'
ASSERT VALUE total_risk = 65 WHERE entity_name = 'Acme Corp'
ASSERT VALUE total_risk = 233 WHERE entity_name = 'Beta Inc'
ASSERT VALUE total_risk = 145 WHERE entity_name = 'Delta Co'
ASSERT VALUE total_risk = 268 WHERE entity_name = 'Epsilon SA'
ASSERT VALUE total_risk = 54 WHERE entity_name = 'Gamma LLC'
SELECT
    entity_name,
    COUNT(*) AS record_count,
    SUM(risk_score) AS total_risk,
    ROUND(AVG(risk_score), 2) AS avg_risk
FROM {{zone_name}}.iceberg_demos.compliance_records
GROUP BY entity_name
ORDER BY entity_name;

-- ============================================================================
-- LEARN: Mutation 1 — UPDATE non_compliant to under_review (Version 2)
-- ============================================================================
-- A compliance officer escalates all non-compliant findings for review.

UPDATE {{zone_name}}.iceberg_demos.compliance_records
SET compliance_status = 'under_review'
WHERE compliance_status = 'non_compliant';

-- ============================================================================
-- Query 4: Post-Update Status Distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE record_count = 10 WHERE compliance_status = 'compliant'
ASSERT VALUE record_count = 5 WHERE compliance_status = 'partial'
ASSERT VALUE record_count = 5 WHERE compliance_status = 'under_review'
SELECT
    compliance_status,
    COUNT(*) AS record_count
FROM {{zone_name}}.iceberg_demos.compliance_records
GROUP BY compliance_status
ORDER BY compliance_status;

-- ============================================================================
-- LEARN: Mutation 2 — Accidental DELETE of high-risk records (Version 3)
-- ============================================================================
-- A junior analyst accidentally deletes all records with risk_score > 50.
-- This removes 7 critical records that must be recovered.

DELETE FROM {{zone_name}}.iceberg_demos.compliance_records
WHERE risk_score > 50;

-- ============================================================================
-- Query 5: Post-Delete — Only 13 Records Remain
-- ============================================================================

ASSERT ROW_COUNT = 13
SELECT * FROM {{zone_name}}.iceberg_demos.compliance_records ORDER BY record_id;

-- ============================================================================
-- Query 6: Damage Assessment — Missing High-Risk Records
-- ============================================================================
-- All records with risk > 50 are gone. Critical compliance data lost.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.compliance_records
WHERE risk_score > 50;

-- ============================================================================
-- LEARN: RESTORE — Recover to Version 2 (After UPDATE, Before DELETE)
-- ============================================================================
-- RESTORE reverts the table to the state at version 2, recovering all 20
-- records with the updated statuses. The Iceberg metadata must be rebuilt
-- to reflect the restored state.

RESTORE {{zone_name}}.iceberg_demos.compliance_records TO VERSION 2;

-- ============================================================================
-- Query 7: Post-Restore — All 20 Records Recovered
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.compliance_records ORDER BY record_id;

-- ============================================================================
-- Query 8: Post-Restore Status — under_review Preserved (Not Original)
-- ============================================================================
-- RESTORE went to version 2, which has the UPDATE applied. The 5 originally
-- non_compliant records should show as under_review, not non_compliant.

ASSERT ROW_COUNT = 3
ASSERT VALUE record_count = 10 WHERE compliance_status = 'compliant'
ASSERT VALUE record_count = 5 WHERE compliance_status = 'partial'
ASSERT VALUE record_count = 5 WHERE compliance_status = 'under_review'
SELECT
    compliance_status,
    COUNT(*) AS record_count
FROM {{zone_name}}.iceberg_demos.compliance_records
GROUP BY compliance_status
ORDER BY compliance_status;

-- ============================================================================
-- Query 9: Post-Restore — High-Risk Records Are Back
-- ============================================================================

ASSERT ROW_COUNT = 7
SELECT record_id, entity_name, regulation, risk_score
FROM {{zone_name}}.iceberg_demos.compliance_records
WHERE risk_score > 50
ORDER BY risk_score DESC;

-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.compliance_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.compliance_iceberg
USING ICEBERG
LOCATION 'compliance_records';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.compliance_iceberg TO USER {{current_user}};

-- ============================================================================
-- Iceberg Verify 1: Row Count — 20 Records After Restore
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.compliance_iceberg ORDER BY record_id;

-- ============================================================================
-- Iceberg Verify 2: Status Distribution via Iceberg — Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE record_count = 10 WHERE compliance_status = 'compliant'
ASSERT VALUE record_count = 5 WHERE compliance_status = 'partial'
ASSERT VALUE record_count = 5 WHERE compliance_status = 'under_review'
SELECT
    compliance_status,
    COUNT(*) AS record_count
FROM {{zone_name}}.iceberg_demos.compliance_iceberg
GROUP BY compliance_status
ORDER BY compliance_status;

-- ============================================================================
-- Iceberg Verify 3: High-Risk Spot-Check
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE entity_name = 'Epsilon SA' WHERE record_id = 15
ASSERT VALUE risk_score = 80 WHERE record_id = 15
SELECT *
FROM {{zone_name}}.iceberg_demos.compliance_iceberg
WHERE record_id = 15;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_records = 20
ASSERT VALUE total_risk = 765
ASSERT VALUE avg_risk = 38.25
ASSERT VALUE entity_count = 5
ASSERT VALUE regulation_count = 4
SELECT
    COUNT(*) AS total_records,
    SUM(risk_score) AS total_risk,
    ROUND(AVG(risk_score), 2) AS avg_risk,
    COUNT(DISTINCT entity_name) AS entity_count,
    COUNT(DISTINCT regulation) AS regulation_count
FROM {{zone_name}}.iceberg_demos.compliance_records;
