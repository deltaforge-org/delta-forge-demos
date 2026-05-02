-- ============================================================================
-- Demo: ORC Insurance Claims — Policy Cross-Reference
-- ============================================================================
-- Proves complex JOINs and subqueries work on ORC-backed external tables.
-- Covers INNER JOIN, LEFT JOIN, anti-join, correlated subquery, EXISTS, IN.

-- ============================================================================
-- Query 1: Full Scan — policies and claims baseline
-- ============================================================================

ASSERT ROW_COUNT = 80
SELECT *
FROM {{zone_name}}.orc_insurance.policies;

ASSERT ROW_COUNT = 200
SELECT *
FROM {{zone_name}}.orc_insurance.claims;

-- ============================================================================
-- Query 2: INNER JOIN — claims matched to their policies
-- ============================================================================
-- 180 of 200 claims have a matching policy. 20 are orphans.

ASSERT ROW_COUNT = 180
SELECT c.claim_id, c.policy_id, c.claim_amount, c.status,
       p.holder_name, p.policy_type, p.region
FROM {{zone_name}}.orc_insurance.claims c
INNER JOIN {{zone_name}}.orc_insurance.policies p
    ON c.policy_id = p.policy_id;

-- ============================================================================
-- Query 3: LEFT JOIN — all claims with optional policy info
-- ============================================================================
-- All 200 claims returned; 20 orphans have NULL policy columns.

ASSERT ROW_COUNT = 1
ASSERT VALUE orphan_count = 20
SELECT COUNT(*) AS orphan_count FROM (
    SELECT c.claim_id
    FROM {{zone_name}}.orc_insurance.claims c
    LEFT JOIN {{zone_name}}.orc_insurance.policies p
        ON c.policy_id = p.policy_id
    WHERE p.policy_id IS NULL
) sub;

-- ============================================================================
-- Query 4: Anti-Join — policies with zero claims
-- ============================================================================
-- 4 policies have no claims filed against them.

ASSERT ROW_COUNT = 4
SELECT p.policy_id, p.holder_name, p.policy_type, p.annual_premium
FROM {{zone_name}}.orc_insurance.policies p
LEFT JOIN {{zone_name}}.orc_insurance.claims c
    ON p.policy_id = c.policy_id
WHERE c.claim_id IS NULL
ORDER BY p.policy_id;

-- ============================================================================
-- Query 5: Claims by policy type — aggregation after JOIN
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE claim_count = 47 WHERE policy_type = 'Travel'
ASSERT VALUE claim_count = 40 WHERE policy_type = 'Health'
ASSERT VALUE claim_count = 38 WHERE policy_type = 'Home'
ASSERT VALUE claim_count = 30 WHERE policy_type = 'Auto'
ASSERT VALUE claim_count = 25 WHERE policy_type = 'Life'
SELECT p.policy_type,
       COUNT(*) AS claim_count,
       ROUND(AVG(c.claim_amount), 2) AS avg_claim
FROM {{zone_name}}.orc_insurance.claims c
INNER JOIN {{zone_name}}.orc_insurance.policies p
    ON c.policy_id = p.policy_id
GROUP BY p.policy_type
ORDER BY claim_count DESC;

-- ============================================================================
-- Query 6: EXISTS — policies that have at least one Denied claim
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT p.policy_id, p.holder_name, p.policy_type
FROM {{zone_name}}.orc_insurance.policies p
WHERE EXISTS (
    SELECT 1 FROM {{zone_name}}.orc_insurance.claims c
    WHERE c.policy_id = p.policy_id AND c.status = 'Denied'
)
ORDER BY p.policy_id;

-- ============================================================================
-- Query 7: IN subquery — claims filed against Auto policies
-- ============================================================================

ASSERT ROW_COUNT = 30
SELECT c.claim_id, c.policy_id, c.claim_amount, c.status
FROM {{zone_name}}.orc_insurance.claims c
WHERE c.policy_id IN (
    SELECT p.policy_id FROM {{zone_name}}.orc_insurance.policies p
    WHERE p.policy_type = 'Auto'
)
ORDER BY c.claim_id;

-- ============================================================================
-- Query 8: Claim status distribution
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE claim_count = 55 WHERE status = 'Approved'
ASSERT VALUE claim_count = 55 WHERE status = 'Denied'
ASSERT VALUE claim_count = 45 WHERE status = 'Pending'
ASSERT VALUE claim_count = 45 WHERE status = 'Under Review'
SELECT status,
       COUNT(*) AS claim_count,
       ROUND(SUM(claim_amount), 2) AS total_amount,
       ROUND(AVG(claim_amount), 2) AS avg_amount
FROM {{zone_name}}.orc_insurance.claims
GROUP BY status
ORDER BY claim_count DESC;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'policies_80'
ASSERT VALUE result = 'PASS' WHERE check_name = 'claims_200'
ASSERT VALUE result = 'PASS' WHERE check_name = 'matched_180'
ASSERT VALUE result = 'PASS' WHERE check_name = 'orphans_20'
ASSERT VALUE result = 'PASS' WHERE check_name = 'no_claims_policies_4'
ASSERT VALUE result = 'PASS' WHERE check_name = 'denied_policies_40'
SELECT check_name, result FROM (

    SELECT 'policies_80' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_insurance.policies) = 80
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'claims_200' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_insurance.claims) = 200
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'matched_180' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_insurance.claims c
               INNER JOIN {{zone_name}}.orc_insurance.policies p ON c.policy_id = p.policy_id
           ) = 180 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'orphans_20' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_insurance.claims c
               LEFT JOIN {{zone_name}}.orc_insurance.policies p ON c.policy_id = p.policy_id
               WHERE p.policy_id IS NULL
           ) = 20 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'no_claims_policies_4' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_insurance.policies p
               LEFT JOIN {{zone_name}}.orc_insurance.claims c ON p.policy_id = c.policy_id
               WHERE c.claim_id IS NULL
           ) = 4 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'denied_policies_40' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_insurance.policies p
               WHERE EXISTS (
                   SELECT 1 FROM {{zone_name}}.orc_insurance.claims c
                   WHERE c.policy_id = p.policy_id AND c.status = 'Denied'
               )
           ) = 40 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
