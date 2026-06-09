-- ============================================================================
-- Pseudonymisation Quickstart — Banking KYC — Demo Queries
-- ============================================================================
-- Queries demonstrating the core pseudonymisation concept: data is protected
-- at query time without modifying it on disk. Four transform types are shown:
--   redact       — SSN fully replaced with ***-**-****
--   mask         — Phone number partially visible
--   keyed_hash   — Last name hashed for deterministic linkage
--   generalize   — Date of birth rounded to decade
--
-- Table available:
--   bank_customers — 6 retail bank customer records with PII
--
-- Key insight: aggregations (COUNT, SUM, AVG) still produce correct results
-- because pseudonymisation transforms display values, not stored data.
-- ============================================================================


-- ============================================================================
-- 1. Review Rules — SHOW PSEUDONYMISATION RULES
-- ============================================================================
-- Lists all 4 pseudonymisation rules on the bank_customers table.
-- Each row shows column, transform type, scope, and parameters.
--
-- Expected: 4 rules (ssn/redact, phone/mask, last_name/keyed_hash, date_of_birth/generalize)

ASSERT ROW_COUNT = 4
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.bank_customers;


-- ============================================================================
-- 2. Query Pseudonymised Customers — SELECT All
-- ============================================================================
-- With rules active, SELECT returns transformed values at runtime.
-- Original data remains untouched on disk.
--
-- What you'll see:
--   - ssn:            Redacted to ***-**-**** for every row
--   - phone:          Partially masked (last 5 chars visible)
--   - last_name:      Keyed SHA256 hash (deterministic pseudonym)
--   - date_of_birth:  Generalized to decade
--   - first_name, email, account_tier, balance: Unchanged (no rules)
--
-- Expected: 6 rows, C001 SSN = '***-**-****', C001 tier = 'Premium'

ASSERT ROW_COUNT = 6
ASSERT VALUE ssn_redacted = '***-**-****' WHERE customer_id = 'C001'
ASSERT VALUE account_tier = 'Premium' WHERE customer_id = 'C001'
SELECT
    customer_id,
    first_name,
    last_name      AS last_name_hashed,
    date_of_birth  AS dob_generalized,
    email,
    phone          AS phone_masked,
    ssn            AS ssn_redacted,
    account_tier,
    balance,
    active
FROM {{zone_name}}.pseudonymisation_demos.bank_customers;


-- ============================================================================
-- 3. Aggregation Still Works — GROUP BY Account Tier
-- ============================================================================
-- Pseudonymisation does not affect numeric aggregations. COUNT, SUM, and AVG
-- operate on the original stored values, producing accurate results.
--
-- Expected:
--   Premium:  count=3, avg_balance ~ 141750.17
--   Standard: count=3, avg_balance ~ 48367.17
--
-- Non-deterministic: float aggregation may vary

ASSERT ROW_COUNT = 2
ASSERT VALUE customer_count = 3 WHERE account_tier = 'Premium'
ASSERT VALUE customer_count = 3 WHERE account_tier = 'Standard'
ASSERT WARNING VALUE avg_balance BETWEEN 141750.0 AND 141751.0 WHERE account_tier = 'Premium'
ASSERT WARNING VALUE avg_balance BETWEEN 48367.0 AND 48368.0 WHERE account_tier = 'Standard'
SELECT
    account_tier,
    COUNT(*)           AS customer_count,
    ROUND(AVG(balance), 2) AS avg_balance,
    ROUND(SUM(balance), 2) AS total_balance
FROM {{zone_name}}.pseudonymisation_demos.bank_customers
GROUP BY account_tier
ORDER BY account_tier;


-- ============================================================================
-- 4. Verification — All Checks
-- ============================================================================
-- Final verification query confirming row count, unprotected columns are
-- readable, and protected columns are transformed.
--
-- Expected: 6 rows, C001 first_name = 'Alice', C001 ssn = '***-**-****'

ASSERT ROW_COUNT = 6
ASSERT VALUE first_name = 'Alice' WHERE customer_id = 'C001'
ASSERT VALUE ssn_redacted = '***-**-****' WHERE customer_id = 'C001'
SELECT
    customer_id,
    first_name,
    ssn AS ssn_redacted,
    account_tier,
    active
FROM {{zone_name}}.pseudonymisation_demos.bank_customers;
