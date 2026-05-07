-- ============================================================================
-- Delta GDPR Data Erasure — Right to Erasure Lifecycle — Educational Queries
-- ============================================================================
-- WHAT: GDPR Article 17 ("right to erasure") requires organizations to
--       permanently delete personal data on request. Delta Lake makes erasure
--       atomic and auditable — but old versions still contain PII until
--       VACUUM physically purges the orphaned Parquet files.
-- WHY:  Banks hold sensitive PII (SSN, phone, mailing address). When
--       customers close accounts or invoke their GDPR rights, the bank must
--       prove that PII was removed — not just logically, but physically.
-- HOW:  1. UPDATE SET NULL erases PII columns for targeted customers
--       2. DESCRIBE HISTORY creates an auditable change trail
--       3. VERSION AS OF proves old versions still expose PII (the risk)
--       4. VACUUM physically purges old files, completing the lifecycle
-- ============================================================================


-- ============================================================================
-- EXPLORE: Customer Accounts Before Erasure
-- ============================================================================
-- The table has 11 columns including PII: ssn, phone, mailing_address.
-- All 30 accounts have full PII. Sample 4 accounts:

ASSERT ROW_COUNT = 4
ASSERT VALUE account_holder = 'Alice Monroe' WHERE id = 1
ASSERT VALUE ssn = '123-45-6789' WHERE id = 1
SELECT id, account_holder, ssn, phone, mailing_address, account_type
FROM {{zone_name}}.delta_demos.gdpr_customer_accounts
WHERE id IN (1, 2, 10, 11)
ORDER BY id;


-- ============================================================================
-- STEP 1: GDPR ERASURE — NULL Out PII for Accounts 1-10
-- ============================================================================
-- Customers 1-10 have submitted GDPR "right to erasure" requests.
-- We NULL out all three PII columns in a single atomic UPDATE.
-- Delta writes a new version — the old version still has the data.

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.gdpr_customer_accounts
SET ssn = NULL, phone = NULL, mailing_address = NULL
WHERE id BETWEEN 1 AND 10;


-- ============================================================================
-- EXPLORE: Before vs After — Erased and Intact Accounts Side by Side
-- ============================================================================
-- Account 1 (erased): ssn, phone, mailing_address are all NULL.
-- Account 11 (intact): all PII columns remain populated.

ASSERT ROW_COUNT = 2
ASSERT VALUE ssn IS NULL WHERE id = 1
SELECT id, account_holder, ssn, phone, mailing_address
FROM {{zone_name}}.delta_demos.gdpr_customer_accounts
WHERE id IN (1, 11)
ORDER BY id;


-- ============================================================================
-- LEARN: Erasure Summary — How Many Accounts Were Affected?
-- ============================================================================
-- 30 total accounts. 10 had all PII erased. 20 remain fully intact.

ASSERT VALUE total_accounts = 30
ASSERT VALUE erased_ssn = 10
ASSERT VALUE erased_phone = 10
ASSERT VALUE erased_address = 10
ASSERT VALUE fully_intact = 20
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) AS total_accounts,
    COUNT(*) FILTER (WHERE ssn IS NULL) AS erased_ssn,
    COUNT(*) FILTER (WHERE phone IS NULL) AS erased_phone,
    COUNT(*) FILTER (WHERE mailing_address IS NULL) AS erased_address,
    COUNT(*) FILTER (WHERE ssn IS NOT NULL AND phone IS NOT NULL AND mailing_address IS NOT NULL) AS fully_intact
FROM {{zone_name}}.delta_demos.gdpr_customer_accounts;


-- ============================================================================
-- STEP 2: AUDIT TRAIL — DESCRIBE HISTORY Shows Every Change
-- ============================================================================
-- Delta's transaction log records every operation. A compliance officer can
-- see exactly when the erasure happened, who did it, and what changed.
-- Versions: 0=create, 1=insert batch 1, 2=insert batch 2, 3=UPDATE (erasure)

ASSERT WARNING ROW_COUNT >= 3
DESCRIBE HISTORY {{zone_name}}.delta_demos.gdpr_customer_accounts;


-- ============================================================================
-- STEP 3: TIME TRAVEL — Old Versions Still Expose PII (The Risk)
-- ============================================================================
-- Even after the UPDATE, version 2 (before erasure) still has all PII.
-- This is the key GDPR risk: time travel can recover "deleted" data.
-- Until VACUUM runs, the old Parquet files remain on disk.

ASSERT ROW_COUNT = 1
ASSERT VALUE ssn = '123-45-6789' WHERE id = 1
SELECT id, account_holder, ssn, phone
FROM {{zone_name}}.delta_demos.gdpr_customer_accounts VERSION AS OF 2
WHERE id = 1;


-- ============================================================================
-- LEARN: How Many Records Are Exposed in the Old Version?
-- ============================================================================
-- All 30 accounts had SSN before erasure. Every one is still readable
-- through time travel. This proves VACUUM is mandatory for true compliance.

ASSERT VALUE exposed_records = 30
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS exposed_records
FROM {{zone_name}}.delta_demos.gdpr_customer_accounts VERSION AS OF 2
WHERE ssn IS NOT NULL;


-- ============================================================================
-- STEP 4: VACUUM — Physically Purge Old Data Files
-- ============================================================================
-- VACUUM removes Parquet files no longer referenced by the current version.
-- After VACUUM, time travel to pre-erasure versions becomes impossible —
-- the physical files containing PII are gone from disk.
-- (Retention period set to 0 hours for demo purposes.)

VACUUM {{zone_name}}.delta_demos.gdpr_customer_accounts RETAIN 0 HOURS;


-- ============================================================================
-- LEARN: Compliance Verification — Grouped Erasure Check
-- ============================================================================
-- A compliance officer verifies: erased accounts (1-10) have zero PII,
-- intact accounts (11-30) are untouched.

ASSERT ROW_COUNT = 2
ASSERT VALUE has_ssn = 0 WHERE account_group = 'Erased (ids 1-10)'
ASSERT VALUE has_phone = 0 WHERE account_group = 'Erased (ids 1-10)'
ASSERT VALUE has_address = 0 WHERE account_group = 'Erased (ids 1-10)'
ASSERT VALUE has_ssn = 20 WHERE account_group = 'Intact (ids 11-30)'
SELECT
    CASE
        WHEN id BETWEEN 1 AND 10 THEN 'Erased (ids 1-10)'
        ELSE 'Intact (ids 11-30)'
    END AS account_group,
    COUNT(*) AS accounts,
    COUNT(ssn) AS has_ssn,
    COUNT(phone) AS has_phone,
    COUNT(mailing_address) AS has_address
FROM {{zone_name}}.delta_demos.gdpr_customer_accounts
GROUP BY CASE WHEN id BETWEEN 1 AND 10 THEN 'Erased (ids 1-10)' ELSE 'Intact (ids 11-30)' END
ORDER BY account_group;


-- ============================================================================
-- EXPLORE: Non-PII Data Preserved After Erasure
-- ============================================================================
-- GDPR erasure targets only PII. The customer's non-sensitive data
-- (account_holder, email, account_type, branch_city, balance) is intact.

ASSERT VALUE account_holder = 'Alice Monroe'
ASSERT VALUE balance = 15420.50
ASSERT ROW_COUNT = 1
SELECT id, account_holder, email, account_type, branch_city, balance
FROM {{zone_name}}.delta_demos.gdpr_customer_accounts
WHERE id = 1;


-- ============================================================================
-- EXPLORE: Analytics Still Work — Balance by Account Type
-- ============================================================================
-- Non-PII analytics remain fully functional after erasure.

ASSERT ROW_COUNT = 3
SELECT account_type, COUNT(*) AS accounts, SUM(balance) AS total_balance
FROM {{zone_name}}.delta_demos.gdpr_customer_accounts
GROUP BY account_type
ORDER BY total_balance DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 30
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.gdpr_customer_accounts;

-- Verify 10 SSNs were NULLed (GDPR erasure)
ASSERT VALUE null_ssn_count = 10
SELECT COUNT(*) AS null_ssn_count FROM {{zone_name}}.delta_demos.gdpr_customer_accounts WHERE ssn IS NULL;

-- Verify 10 phones were NULLed
ASSERT VALUE null_phone_count = 10
SELECT COUNT(*) AS null_phone_count FROM {{zone_name}}.delta_demos.gdpr_customer_accounts WHERE phone IS NULL;

-- Verify 20 accounts fully intact
ASSERT VALUE intact_count = 20
SELECT COUNT(*) AS intact_count FROM {{zone_name}}.delta_demos.gdpr_customer_accounts WHERE ssn IS NOT NULL AND phone IS NOT NULL AND mailing_address IS NOT NULL;

-- Verify intact user SSN preserved
ASSERT VALUE ssn = '111-22-3333'
SELECT ssn FROM {{zone_name}}.delta_demos.gdpr_customer_accounts WHERE id = 11;

-- Verify erased user SSN is NULL
ASSERT VALUE ssn IS NULL
SELECT ssn FROM {{zone_name}}.delta_demos.gdpr_customer_accounts WHERE id = 1;

-- Verify non-PII preserved for erased user
ASSERT VALUE account_holder = 'Alice Monroe'
SELECT account_holder FROM {{zone_name}}.delta_demos.gdpr_customer_accounts WHERE id = 1;

-- Verify 24 distinct countries
ASSERT VALUE distinct_countries = 24
SELECT COUNT(DISTINCT country) AS distinct_countries FROM {{zone_name}}.delta_demos.gdpr_customer_accounts;

-- Verify total balance unchanged by erasure
ASSERT VALUE total_balance = 3804072.50
SELECT SUM(balance) AS total_balance FROM {{zone_name}}.delta_demos.gdpr_customer_accounts;

-- Verify 3 account types
ASSERT VALUE account_types = 3
SELECT COUNT(DISTINCT account_type) AS account_types FROM {{zone_name}}.delta_demos.gdpr_customer_accounts;
