-- ============================================================================
-- Insurance Claim Classification — Educational Queries
-- ============================================================================
-- WHAT: CASE expressions for conditional logic in SQL analytics
-- WHY:  Real-world classification pipelines rely on CASE to categorize,
--       flag, and compute tiered values without procedural code
-- HOW:  Searched CASE (CASE WHEN), simple CASE (CASE expr WHEN val),
--       nested CASE, CASE in aggregation, and CASE in ORDER BY
-- ============================================================================


-- ============================================================================
-- EXPLORE: Dataset Overview
-- ============================================================================
-- Verify the insurance_claims table has 35 rows with the expected mix of
-- claim types, statuses, and value ranges.

ASSERT ROW_COUNT = 35
SELECT *
FROM {{zone_name}}.delta_demos.insurance_claims
ORDER BY claim_id;


-- ============================================================================
-- LEARN: Searched CASE — Severity Tier Classification
-- ============================================================================
-- CASE WHEN <condition> THEN <result> evaluates each condition top-to-bottom,
-- returning the result of the first match. This is the workhorse of row-level
-- classification: here we bucket claim amounts into severity tiers that drive
-- downstream SLA and adjuster assignment rules.
--
-- Tiers:  <1000 = Minor, 1000-9999 = Moderate, 10000-49999 = Major, >=50000 = Catastrophic

ASSERT ROW_COUNT = 4
ASSERT VALUE claim_count = 6 WHERE severity_tier = 'Minor'
ASSERT VALUE claim_count = 11 WHERE severity_tier = 'Moderate'
ASSERT VALUE claim_count = 10 WHERE severity_tier = 'Major'
ASSERT VALUE claim_count = 8 WHERE severity_tier = 'Catastrophic'
SELECT
    CASE
        WHEN claim_amount < 1000 THEN 'Minor'
        WHEN claim_amount < 10000 THEN 'Moderate'
        WHEN claim_amount < 50000 THEN 'Major'
        ELSE 'Catastrophic'
    END AS severity_tier,
    COUNT(*) AS claim_count,
    ROUND(MIN(claim_amount), 2) AS min_amount,
    ROUND(MAX(claim_amount), 2) AS max_amount,
    ROUND(AVG(claim_amount), 2) AS avg_amount
FROM {{zone_name}}.delta_demos.insurance_claims
GROUP BY
    CASE
        WHEN claim_amount < 1000 THEN 'Minor'
        WHEN claim_amount < 10000 THEN 'Moderate'
        WHEN claim_amount < 50000 THEN 'Major'
        ELSE 'Catastrophic'
    END
ORDER BY MIN(claim_amount);


-- ============================================================================
-- LEARN: Simple CASE — Department Routing
-- ============================================================================
-- CASE <expression> WHEN <value> THEN <result> compares a single expression
-- against a list of literal values. This is cleaner than searched CASE when
-- all conditions test equality on the same column. Here we route each claim
-- to its handling department based on claim_type.

ASSERT ROW_COUNT = 35
ASSERT VALUE department = 'Auto Claims Dept' WHERE claim_id = 1
ASSERT VALUE department = 'Property Dept' WHERE claim_id = 2
ASSERT VALUE department = 'Health Benefits' WHERE claim_id = 3
ASSERT VALUE department = 'Life & Annuity' WHERE claim_id = 5
SELECT
    claim_id,
    policy_holder,
    claim_type,
    CASE claim_type
        WHEN 'auto'   THEN 'Auto Claims Dept'
        WHEN 'home'   THEN 'Property Dept'
        WHEN 'health' THEN 'Health Benefits'
        WHEN 'life'   THEN 'Life & Annuity'
        ELSE 'Unknown'
    END AS department,
    claim_amount
FROM {{zone_name}}.delta_demos.insurance_claims
ORDER BY claim_id;


-- ============================================================================
-- LEARN: Nested CASE — Risk Classification with NULL Handling
-- ============================================================================
-- CASE expressions can be nested to create multi-dimensional classification.
-- The outer CASE handles NULL fraud_score (IS NULL check must come first or
-- the inner conditions would fail). The inner CASE cross-references fraud_score
-- with claim_amount to separate truly dangerous claims from low-value anomalies.
--
-- Logic:
--   fraud_score IS NULL           → 'Unscored'
--   fraud_score > 0.7 AND > 10000 → 'High Risk'
--   fraud_score > 0.7             → 'Watch'        (high score, low amount)
--   else                          → 'Normal'

ASSERT ROW_COUNT = 35
ASSERT VALUE risk_flag = 'Unscored' WHERE claim_id = 3
ASSERT VALUE risk_flag = 'High Risk' WHERE claim_id = 5
ASSERT VALUE risk_flag = 'Normal' WHERE claim_id = 1
SELECT
    claim_id,
    policy_holder,
    claim_amount,
    fraud_score,
    CASE
        WHEN fraud_score IS NULL THEN 'Unscored'
        WHEN fraud_score > 0.7 THEN
            CASE
                WHEN claim_amount > 10000 THEN 'High Risk'
                ELSE 'Watch'
            END
        ELSE 'Normal'
    END AS risk_flag
FROM {{zone_name}}.delta_demos.insurance_claims
ORDER BY claim_id;


-- ============================================================================
-- LEARN: CASE in Aggregation — Conditional SUM and COUNT
-- ============================================================================
-- CASE inside aggregate functions is the SQL equivalent of a pivot or
-- conditional counter. SUM(CASE WHEN ... THEN amount ELSE 0 END) totals
-- only matching rows, while COUNT(CASE WHEN ... THEN 1 END) counts them
-- (COUNT ignores NULLs, so omitting ELSE produces a conditional count).
-- This gives a per-claim-type financial summary in a single pass.

ASSERT ROW_COUNT = 4
ASSERT VALUE approved_total = 77100.00 WHERE claim_type = 'auto'
ASSERT VALUE approved_total = 147800.00 WHERE claim_type = 'home'
ASSERT VALUE approved_total = 17600.00 WHERE claim_type = 'health'
ASSERT VALUE approved_total = 55000.00 WHERE claim_type = 'life'
ASSERT VALUE denied_count = 1 WHERE claim_type = 'auto'
ASSERT VALUE denied_count = 2 WHERE claim_type = 'health'
SELECT
    claim_type,
    COUNT(*) AS total_claims,
    ROUND(SUM(CASE WHEN status = 'approved' THEN claim_amount ELSE 0 END), 2) AS approved_total,
    COUNT(CASE WHEN status = 'denied' THEN 1 END) AS denied_count,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_count,
    COUNT(CASE WHEN status = 'under_review' THEN 1 END) AS review_count
FROM {{zone_name}}.delta_demos.insurance_claims
GROUP BY claim_type
ORDER BY claim_type;


-- ============================================================================
-- LEARN: CASE in ORDER BY — Custom Priority Sorting
-- ============================================================================
-- ORDER BY accepts CASE expressions to define non-alphabetical sort orders.
-- Insurance adjusters need pending claims first, then under_review, then by
-- amount descending within each priority band. This eliminates the need for
-- a separate priority column or application-side re-sorting.

ASSERT ROW_COUNT = 35
ASSERT VALUE status = 'pending' WHERE claim_id = 5
ASSERT VALUE status = 'pending' WHERE claim_id = 10
SELECT
    claim_id,
    policy_holder,
    status,
    claim_amount,
    CASE status
        WHEN 'pending'      THEN 1
        WHEN 'under_review' THEN 2
        WHEN 'approved'     THEN 3
        WHEN 'denied'       THEN 4
        ELSE 5
    END AS priority
FROM {{zone_name}}.delta_demos.insurance_claims
ORDER BY
    CASE status
        WHEN 'pending'      THEN 1
        WHEN 'under_review' THEN 2
        WHEN 'approved'     THEN 3
        WHEN 'denied'       THEN 4
        ELSE 5
    END,
    claim_amount DESC;


-- ============================================================================
-- LEARN: Combined — Full Classification Pipeline
-- ============================================================================
-- This query assembles the entire classification pipeline in one statement:
-- severity tier, department routing, risk flag, and net payout calculation.
-- The payout uses CASE to zero out denied claims (amount - deductible for
-- all other statuses). This is the kind of multi-CASE query that replaces
-- hundreds of lines of procedural code in production ETL.

ASSERT ROW_COUNT = 35
ASSERT VALUE net_payout = 2000.00 WHERE claim_id = 1
ASSERT VALUE net_payout = 0.00 WHERE claim_id = 17
ASSERT VALUE net_payout = 55000.00 WHERE claim_id = 31
SELECT
    claim_id,
    policy_holder,
    CASE
        WHEN claim_amount < 1000 THEN 'Minor'
        WHEN claim_amount < 10000 THEN 'Moderate'
        WHEN claim_amount < 50000 THEN 'Major'
        ELSE 'Catastrophic'
    END AS severity_tier,
    CASE claim_type
        WHEN 'auto'   THEN 'Auto Claims Dept'
        WHEN 'home'   THEN 'Property Dept'
        WHEN 'health' THEN 'Health Benefits'
        WHEN 'life'   THEN 'Life & Annuity'
        ELSE 'Unknown'
    END AS department,
    CASE
        WHEN fraud_score IS NULL THEN 'Unscored'
        WHEN fraud_score > 0.7 THEN
            CASE
                WHEN claim_amount > 10000 THEN 'High Risk'
                ELSE 'Watch'
            END
        ELSE 'Normal'
    END AS risk_flag,
    ROUND(
        CASE
            WHEN status = 'denied' THEN 0
            ELSE claim_amount - deductible
        END, 2
    ) AS net_payout,
    status
FROM {{zone_name}}.delta_demos.insurance_claims
ORDER BY claim_id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Summary verification ensuring the dataset and classification logic produce
-- expected results across all CASE expression patterns.

-- Verify total row count
ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.delta_demos.insurance_claims;

-- Verify distinct claim types
ASSERT VALUE distinct_types = 4
SELECT COUNT(DISTINCT claim_type) AS distinct_types
FROM {{zone_name}}.delta_demos.insurance_claims;

-- Verify status distribution
ASSERT VALUE approved_count = 20
ASSERT VALUE denied_count = 5
ASSERT VALUE pending_count = 5
ASSERT VALUE review_count = 5
SELECT
    COUNT(CASE WHEN status = 'approved' THEN 1 END) AS approved_count,
    COUNT(CASE WHEN status = 'denied' THEN 1 END) AS denied_count,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_count,
    COUNT(CASE WHEN status = 'under_review' THEN 1 END) AS review_count
FROM {{zone_name}}.delta_demos.insurance_claims;

-- Verify severity tier counts sum to 35
ASSERT VALUE tier_total = 35
SELECT
    SUM(CASE WHEN claim_amount < 1000 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN claim_amount >= 1000 AND claim_amount < 10000 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN claim_amount >= 10000 AND claim_amount < 50000 THEN 1 ELSE 0 END) +
    SUM(CASE WHEN claim_amount >= 50000 THEN 1 ELSE 0 END) AS tier_total
FROM {{zone_name}}.delta_demos.insurance_claims;

-- Verify high-risk claim count (fraud_score > 0.7 AND amount > 10000)
ASSERT VALUE high_risk_count = 7
SELECT COUNT(*) AS high_risk_count
FROM {{zone_name}}.delta_demos.insurance_claims
WHERE fraud_score > 0.7 AND claim_amount > 10000;

-- Verify total approved amount across all types
ASSERT VALUE total_approved = 297500.00
SELECT ROUND(SUM(CASE WHEN status = 'approved' THEN claim_amount ELSE 0 END), 2) AS total_approved
FROM {{zone_name}}.delta_demos.insurance_claims;

-- Verify NULL fraud_score count (unscored claims)
ASSERT VALUE unscored_count = 4
SELECT COUNT(*) AS unscored_count
FROM {{zone_name}}.delta_demos.insurance_claims
WHERE fraud_score IS NULL;

-- Verify NULL adjuster_id count (unassigned claims)
ASSERT VALUE unassigned_count = 7
SELECT COUNT(*) AS unassigned_count
FROM {{zone_name}}.delta_demos.insurance_claims
WHERE adjuster_id IS NULL;
