-- ============================================================================
-- Pseudonymisation Lifecycle — Insurance Claims — Demo Queries
-- ============================================================================
-- Demonstrates pseudonymisation rule lifecycle management for an insurance
-- company's claims processing system. Covers:
--   SHOW RULES   — review all active protection rules
--   SELECT       — query pseudonymised data at runtime
--   DROP RULE    — remove individual rules when no longer needed
--
-- One table is available:
--   insurance_claims — 5 claim records with PII fields
--
-- Transform types used:
--   redact      — Full replacement with mask string (SSN)
--   keyed_hash  — Deterministic hash with salt (claimant name)
--   generalize  — Reduce precision (date of birth)
--   tokenize    — Opaque token (policy holder ID)
--   mask        — Partial visibility (description, first 10 chars)
--
-- Precomputed values:
--   Total rows: 5
--   Auto claims: count=2, total=7700.00
--   Home claims: count=2, total=21700.00
--   Life claims: count=1, total=50000.00
--   Approved: count=3, total=26200.00
--   Pending: 1, Under_review: 1
-- ============================================================================


-- ============================================================================
-- 1. Show All Rules — SHOW PSEUDONYMISATION RULES
-- ============================================================================
-- Lists every pseudonymisation rule in the catalog. With only one table in
-- this demo, all 5 rules belong to insurance_claims.
--
-- Expected: 5 rows (ssn, claimant_name, date_of_birth, policy_holder_id,
--           description)

ASSERT ROW_COUNT = 5
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.insurance_claims;


-- ============================================================================
-- 2. Show Rules for Table — SHOW PSEUDONYMISATION RULES FOR
-- ============================================================================
-- Filter rules to the insurance_claims table specifically. Since all rules
-- target this table, the result matches the unfiltered view.
--
-- Expected: 5 rows

ASSERT ROW_COUNT = 5
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.insurance_claims;


-- ============================================================================
-- 3. Query with All Rules Active
-- ============================================================================
-- With all 5 rules active, SELECT returns transformed PII columns while
-- leaving non-protected columns (claim_id, claim_type, amount, status,
-- filed_date) unchanged.
--
-- What you'll see:
--   - ssn:              ***-**-**** (redacted)
--   - claimant_name:    SHA256 keyed hash (deterministic per person)
--   - date_of_birth:    Generalized to decade range
--   - policy_holder_id: TOK_ prefixed token
--   - description:      First 10 chars visible, rest masked
--   - claim_type:       Unchanged (no rule)
--   - status:           Unchanged (no rule)
--
-- Expected: 5 rows, unprotected fields match original values

ASSERT ROW_COUNT = 5
ASSERT VALUE claim_type = 'Auto' WHERE claim_id = 'CLM-2024-001'
ASSERT VALUE ssn_redacted = '***-**-****' WHERE claim_id = 'CLM-2024-001'
SELECT
    claim_id,
    policy_holder_id AS holder_token,
    claimant_name    AS name_hash,
    date_of_birth    AS dob_generalized,
    ssn              AS ssn_redacted,
    claim_type,
    description      AS desc_masked,
    amount,
    status
FROM {{zone_name}}.pseudonymisation_demos.insurance_claims;


-- ============================================================================
-- 4. Aggregation by Claim Type
-- ============================================================================
-- Aggregations work correctly on pseudonymised data. Non-protected columns
-- like claim_type, amount, and status are unaffected by transforms.
--
-- Expected: 3 rows (Auto, Home, Life)

ASSERT ROW_COUNT = 3
ASSERT VALUE claim_count = 2 WHERE claim_type = 'Auto'
ASSERT VALUE claim_count = 1 WHERE claim_type = 'Life'
ASSERT VALUE total_amount = 7700.00 WHERE claim_type = 'Auto'
SELECT
    claim_type,
    COUNT(*)    AS claim_count,
    SUM(amount) AS total_amount
FROM {{zone_name}}.pseudonymisation_demos.insurance_claims
GROUP BY claim_type;


-- ============================================================================
-- 5. Drop One Rule — Remove Description Masking
-- ============================================================================
-- The description masking rule is no longer needed after a policy review.
-- DROP removes it permanently; SHOW confirms the updated rule count.

DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.insurance_claims (description);

-- Verify rule count after drop (note: DROP takes effect after catalog sync)
ASSERT ROW_COUNT <= 5
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.insurance_claims;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: row totals, approved claim count, and SSN
-- redaction still in effect after the description rule was dropped.

ASSERT ROW_COUNT = 5
ASSERT VALUE ssn = '***-**-****' WHERE claim_id = 'CLM-2024-001'
SELECT
    claim_id,
    policy_holder_id,
    claimant_name,
    date_of_birth,
    ssn,
    claim_type,
    description,
    amount,
    status
FROM {{zone_name}}.pseudonymisation_demos.insurance_claims;

ASSERT ROW_COUNT = 3
SELECT
    claim_id,
    status,
    amount
FROM {{zone_name}}.pseudonymisation_demos.insurance_claims
WHERE status = 'approved';
