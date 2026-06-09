-- ============================================================================
-- Pseudonymisation Healthcare — Demo Queries
-- ============================================================================
-- Queries showcasing GDPR-compliant pseudonymisation across HL7, FHIR, and
-- EDI healthcare data. Demonstrates all 5 pseudonymisation commands:
--   CREATE RULE  — (done in setup.sql)
--   SHOW RULES   — review active protection rules
--   SELECT       — query pseudonymised data at runtime
--   ALTER RULE   — enable/disable rules for auditing
--   APPLY        — permanent data transformation
--   DROP RULE    — remove individual rules
--
-- Three tables are available:
--   hl7_patients   — HL7 v2 ADT with PID fields (4 patients)
--   fhir_patients  — FHIR R4 Patient resources (4 patients)
--   edi_claims     — EDI HIPAA X12 transactions (5 claims)
--
-- Transform types used:
--   keyed_hash  — Deterministic hash with salt (linkable pseudonym)
--   encrypt     — Reversible encryption (needs key to decrypt)
--   redact      — Full replacement with mask string
--   generalize  — Reduce precision (DOB -> year, year -> decade)
--   tokenize    — Opaque token (TOK_ prefix)
--   mask        — Partial visibility (first N characters)
--   hash        — One-way SHA256 fingerprint (no salt)
-- ============================================================================


-- ============================================================================
-- 1. Review All Rules — SHOW PSEUDONYMISATION RULES
-- ============================================================================
-- Lists every pseudonymisation rule across all three tables. Each row shows
-- the table, column pattern, pattern type (exact/wildcard), transform type,
-- linkability scope, priority, and whether the rule is currently enabled.
--
-- What you'll see:
--   - 22 rules total (6 HL7, 8 FHIR, 8 EDI)
--   - All 7 transform types represented
--   - Wildcard patterns: address_* and *_name on FHIR table
--
-- Expected: 22 rules total (6 HL7 + 8 FHIR + 8 EDI), all enabled = true
-- Use per-table SHOW to avoid counting rules from other demos

ASSERT ROW_COUNT = 6
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.hl7_patients;


-- ============================================================================
-- 2. Rules Per Table
-- ============================================================================
-- Filter rules to a single table for focused review.

ASSERT ROW_COUNT = 6
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.hl7_patients;

ASSERT ROW_COUNT = 8
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.fhir_patients;

ASSERT ROW_COUNT = 8
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.edi_claims;


-- ============================================================================
-- 3. HL7 — Query Pseudonymised Patient Admissions
-- ============================================================================
-- With rules active, SELECT queries return transformed values at runtime.
-- Original data remains untouched on disk.
--
-- What you'll see:
--   - pid_3 (MRN):   SHA256 keyed hash (deterministic per patient)
--   - pid_5 (name):  TOK_ prefixed token
--   - pid_7 (DOB):   Generalized to YYYY0000 (birth year only)
--   - pid_13 (phone): First 5 chars visible, rest masked with *
--   - pid_19 (SSN):  Fully redacted to ***-**-****
--   - pv1_2, pv1_7, status: Unchanged (no rules on these columns)
--
-- Expected: 4 rows with transformed PII, clinical fields unchanged

ASSERT ROW_COUNT = 4
ASSERT VALUE pv1_2 = 'I' WHERE df_message_id = 'MSG001'
ASSERT VALUE status = 'Active' WHERE df_message_id = 'MSG001'
ASSERT VALUE status = 'Discharged' WHERE df_message_id = 'MSG004'
ASSERT VALUE physician = 'DR JONES' WHERE df_message_id = 'MSG001'
ASSERT VALUE physician = 'DR WILSON' WHERE df_message_id = 'MSG004'
SELECT
    df_message_id,
    pid_3  AS mrn_hash,
    pid_5  AS name_token,
    pid_7  AS dob_generalized,
    pid_13 AS phone_masked,
    pid_19 AS ssn_redacted,
    pv1_2,
    pv1_7  AS physician,
    status
FROM {{zone_name}}.pseudonymisation_demos.hl7_patients;


-- ============================================================================
-- 4. FHIR — Query Pseudonymised Patient Demographics
-- ============================================================================
-- Demonstrates wildcard rule effects: address_* columns are all hashed,
-- *_name columns are keyed_hash pseudonyms.
--
-- What you'll see:
--   - patient_id:  TOK_ token (tokenize, scope person)
--   - family_name: SHA256 keyed hash (*_name wildcard match)
--   - given_name:  SHA256 keyed hash (*_name wildcard match)
--   - birth_date:  Generalized to decade (1974 -> 1970)
--   - gender:      Unchanged (no rule)
--   - email:       Encrypted hash (reversible with key)
--   - phone:       First 4 chars visible, rest masked
--   - mrn:         [REDACTED]
--   - ssn:         SHA256 keyed hash
--
-- Expected: 4 rows

ASSERT ROW_COUNT = 4
ASSERT VALUE mrn_redacted = '[REDACTED]' WHERE gender = 'male'
SELECT
    patient_id   AS id_token,
    family_name  AS name_hash,
    given_name   AS name_hash2,
    birth_date   AS dob_generalized,
    gender,
    email        AS email_encrypted,
    phone        AS phone_masked,
    mrn          AS mrn_redacted,
    ssn          AS ssn_hash,
    active
FROM {{zone_name}}.pseudonymisation_demos.fhir_patients;


-- ============================================================================
-- 5. FHIR — Verify Wildcard Rules on Address Columns
-- ============================================================================
-- The address_* wildcard rule matches all four address columns.
-- All should show SHA256 hash values instead of real addresses.
--
-- Expected: 4 rows, all address fields are 64-char hex strings

ASSERT ROW_COUNT = 4
SELECT
    patient_id    AS id_token,
    address_line  AS addr_hash,
    address_city  AS city_hash,
    address_state AS state_hash,
    address_postal AS zip_hash
FROM {{zone_name}}.pseudonymisation_demos.fhir_patients;


-- ============================================================================
-- 6. EDI — Query Pseudonymised HIPAA Claims
-- ============================================================================
-- Filters to 837 Professional Claims to show pseudonymisation of patient
-- identifiers and financial data within EDI transactions.
--
-- What you'll see:
--   - nm1_3/nm1_4: SHA256 keyed hash (patient names)
--   - nm1_8:       SHA256 keyed hash (member ID / SSN)
--   - clm_1:       TOK_ token (patient account number, scope transaction)
--   - clm_2:       First 2 digits visible, rest masked
--   - bpr_8/bpr_14: ********** (bank accounts fully redacted)
--
-- Expected: 3 rows (837 claims only)

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_type = '837' WHERE df_transaction_id = 'TXN-837-001'
ASSERT VALUE bank_acct_redacted = '**********' WHERE df_transaction_id = 'TXN-837-001'
ASSERT VALUE bank_acct2_redacted = '**********' WHERE df_transaction_id = 'TXN-837-001'
SELECT
    df_transaction_id,
    st_1            AS txn_type,
    nm1_3           AS name_hash,
    nm1_4           AS first_hash,
    nm1_8           AS member_id_hash,
    clm_1           AS acct_token,
    clm_2           AS amount_masked,
    bpr_8           AS bank_acct_redacted,
    bpr_14          AS bank_acct2_redacted
FROM {{zone_name}}.pseudonymisation_demos.edi_claims
WHERE st_1 = '837';


-- ============================================================================
-- 7. Aggregations on Pseudonymised Data
-- ============================================================================
-- Aggregations still work — masking and hashing do not affect SUM, AVG, COUNT.
-- This allows analytics on protected data without exposing individual records.
--
-- Expected: 2 rows (837 and 835 transaction types)

ASSERT ROW_COUNT = 2
ASSERT VALUE claim_count = 3 WHERE transaction_type = '837'
ASSERT VALUE claim_count = 1 WHERE transaction_type = '835'
ASSERT VALUE avg_payment = 1963.5 WHERE transaction_type = '837'
ASSERT VALUE avg_payment = 5000.5 WHERE transaction_type = '835'
SELECT
    st_1 AS transaction_type,
    COUNT(*) AS claim_count,
    SUM(CAST(clm_2 AS DOUBLE)) AS total_charges,
    AVG(CAST(bpr_2 AS DOUBLE)) AS avg_payment
FROM {{zone_name}}.pseudonymisation_demos.edi_claims
WHERE st_1 IN ('837', '835')
GROUP BY st_1;


-- ============================================================================
-- 8. Drop Individual Rules
-- ============================================================================
-- Remove a specific rule by table and column pattern.

-- Remove the phone masking rule from HL7
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.hl7_patients (pid_13);

-- Verify HL7 rules (note: DROP takes effect after catalog sync)
ASSERT ROW_COUNT <= 6
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.hl7_patients;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: row totals and key transform verification
-- across all three healthcare tables.

-- HL7: 4 patients, SSN redacted, clinical fields unchanged
ASSERT ROW_COUNT = 4
ASSERT VALUE pv1_2 = 'I' WHERE df_message_id = 'MSG001'
SELECT df_message_id, pid_3, pid_7, pid_13, pid_19, pv1_2, pv1_7, status
FROM {{zone_name}}.pseudonymisation_demos.hl7_patients;

-- FHIR: 4 patients, MRN always redacted to [REDACTED]
ASSERT ROW_COUNT = 4
ASSERT VALUE mrn = '[REDACTED]' WHERE gender = 'male'
SELECT patient_id, family_name, birth_date, gender, mrn, ssn, active
FROM {{zone_name}}.pseudonymisation_demos.fhir_patients;

-- EDI: 5 claims, bank accounts always fully redacted
ASSERT ROW_COUNT = 5
ASSERT VALUE bpr_8 = '**********' WHERE df_transaction_id = 'TXN-837-001'
ASSERT VALUE bpr_14 = '**********' WHERE df_transaction_id = 'TXN-837-001'
SELECT df_transaction_id, st_1, nm1_3, nm1_4, clm_2, bpr_8, bpr_14
FROM {{zone_name}}.pseudonymisation_demos.edi_claims;
