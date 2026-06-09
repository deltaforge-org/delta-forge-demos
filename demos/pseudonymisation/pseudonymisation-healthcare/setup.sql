-- ============================================================================
-- Pseudonymisation Healthcare — Setup Script
-- ============================================================================
-- Creates three healthcare tables (HL7, FHIR, EDI) with sample patient data
-- and applies GDPR-compliant pseudonymisation rules to sensitive columns.
--
-- Tables created:
--   1. hl7_patients   — HL7 v2 ADT patient admissions (materialized PID fields)
--   2. fhir_patients  — FHIR R4 Patient resources with demographics
--   3. edi_claims     — EDI HIPAA X12 transactions (materialized NM1/CLM/BPR)
--
-- Each table receives targeted pseudonymisation rules demonstrating all 7
-- transform types (keyed_hash, encrypt, redact, generalize, tokenize, mask,
-- hash) and all 3 scopes (person, relationship, transaction).
--
-- Compliance context:
--   HIPAA Safe Harbor  — De-identification of 18 identifier types
--   GDPR Article 4(5)  — Pseudonymisation as a safeguard measure
--   HITECH Act         — Breach notification safe harbor for encrypted PHI
--
-- Variables (auto-injected by DeltaForge):
--   zone_name     — Target zone name (defaults to 'external')
--   data_path     — Root path where demo data files are stored
--   current_user  — Username of the current logged-in user
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.pseudonymisation_demos
    COMMENT 'Pseudonymisation demo — healthcare data with protection rules';


-- ============================================================================
-- TABLE 1: hl7_patients — HL7 v2 ADT Patient Admissions
-- ============================================================================
-- Modeled on the materialized view of HL7 ADT messages. PID fields contain
-- the most common HIPAA identifiers: name, DOB, SSN, address, phone.
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation_demos.hl7_patients (
    df_message_id   VARCHAR,
    pid_3           VARCHAR,
    pid_5           VARCHAR,
    pid_7           VARCHAR,
    pid_8           VARCHAR,
    pid_11          VARCHAR,
    pid_13          VARCHAR,
    pid_19          VARCHAR,
    pv1_2           VARCHAR,
    pv1_3           VARCHAR,
    pv1_7           VARCHAR,
    evn_1           VARCHAR,
    status          VARCHAR
) LOCATION 'pseudonymisation-healthcare/hl7_patients';


DELETE FROM {{zone_name}}.pseudonymisation_demos.hl7_patients WHERE 1=1;

INSERT INTO {{zone_name}}.pseudonymisation_demos.hl7_patients VALUES
    ('MSG001', 'MRN-10045', 'SMITH^WILLIAM^A', '19610615', 'M', '1200 N ELM STREET^^JERUSALEM^TN^99999', '(999)999-1212', '123-45-6789', 'I', 'W4-R201-B1', 'DR JONES', 'A01', 'Active'),
    ('MSG002', 'MRN-10046', 'DOE^JANE^M', '19850322', 'F', '456 OAK AVE^^BIRMINGHAM^AL^35209', '(555)123-4567', '234-56-7890', 'O', 'CLINIC-A', 'DR PATEL', 'A04', 'Active'),
    ('MSG003', 'MRN-10047', 'KLEINSAMPLE^BARRY^Q', '19480203', 'M', '260 GOODWIN CREST^^BIRMINGHAM^AL^35209', '(555)987-6543', '345-67-8901', 'E', 'ER-BAY3', 'DR CHEN', 'A01', 'Active'),
    ('MSG004', 'MRN-10048', 'JOHNSON^ALICE^R', '19901114', 'F', '789 PINE RD^^CHICAGO^IL^60601', '(312)555-0199', '456-78-9012', 'I', 'W2-R105-B2', 'DR WILSON', 'A01', 'Discharged');


-- ============================================================================
-- TABLE 2: fhir_patients — FHIR R4 Patient Resources
-- ============================================================================
-- Modeled on flattened FHIR Patient resources. Human-readable column names
-- make wildcard patterns (address_*, *_name) practical for broad protection.
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation_demos.fhir_patients (
    patient_id      VARCHAR,
    family_name     VARCHAR,
    given_name      VARCHAR,
    birth_date      DATE,
    gender          VARCHAR,
    email           VARCHAR,
    phone           VARCHAR,
    address_line    VARCHAR,
    address_city    VARCHAR,
    address_state   VARCHAR,
    address_postal  VARCHAR,
    mrn             VARCHAR,
    ssn             VARCHAR,
    marital_status  VARCHAR,
    active          BOOLEAN
) LOCATION 'pseudonymisation-healthcare/fhir_patients';


DELETE FROM {{zone_name}}.pseudonymisation_demos.fhir_patients WHERE 1=1;

INSERT INTO {{zone_name}}.pseudonymisation_demos.fhir_patients VALUES
    ('pt-fhir-001', 'Chalmers', 'Peter', '1974-12-25', 'male', 'peter.chalmers@example.com', '(03) 5555 6473', '534 Erewhon St', 'PleasantVille', 'VT', '05401', 'MRN-20001', '111-22-3333', 'M', true),
    ('pt-fhir-002', 'Solo', 'Leia', '1995-10-12', 'female', 'leia.solo@hospital.org', '(555) 867-5309', '100 Galaxy Way', 'Alderaan', 'CA', '90210', 'MRN-20002', '222-33-4444', 'S', true),
    ('pt-fhir-003', 'Duck', 'Donald', '1934-06-09', 'male', 'dduck@duckburg.net', '(555) 382-5633', '1313 Webfoot Walk', 'Duckburg', 'CA', '95501', 'MRN-20003', '333-44-5555', 'M', true),
    ('pt-fhir-004', 'Doe', 'Jane', '1988-03-15', 'female', 'jdoe@clinic.net', '(555) 246-8101', '42 Unknown St', 'Springfield', 'IL', '62704', 'MRN-20004', '444-55-6666', 'S', false);


-- ============================================================================
-- TABLE 3: edi_claims — EDI HIPAA X12 Transactions
-- ============================================================================
-- Modeled on materialized HIPAA X12 transactions. Contains patient identifiers
-- (NM1), financial data (BPR), and claim details (CLM).
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation_demos.edi_claims (
    df_transaction_id VARCHAR,
    st_1            VARCHAR,
    bht_2           VARCHAR,
    nm1_1           VARCHAR,
    nm1_3           VARCHAR,
    nm1_4           VARCHAR,
    nm1_8           VARCHAR,
    dmg_1           VARCHAR,
    dmg_2           VARCHAR,
    clm_1           VARCHAR,
    clm_2           DOUBLE,
    bpr_1           VARCHAR,
    bpr_2           DOUBLE,
    bpr_8           VARCHAR,
    bpr_14          VARCHAR
) LOCATION 'pseudonymisation-healthcare/edi_claims';


DELETE FROM {{zone_name}}.pseudonymisation_demos.edi_claims WHERE 1=1;

INSERT INTO {{zone_name}}.pseudonymisation_demos.edi_claims VALUES
    ('TXN-837-001', '837', '00', 'IL', 'SMITH', 'FRED', '123456789A', '12101930', 'M', 'ACCT-5001', 1250.00, 'C', 1250.00, '9876543210', '1234567890'),
    ('TXN-837-002', '837', '00', 'IL', 'JONES', 'MARY', '234567890A', '05151985', 'F', 'ACCT-5002', 3750.50, 'C', 3750.50, '8765432109', '2345678901'),
    ('TXN-835-001', '835', '08', '85', 'GENERAL HOSPITAL', NULL, '987654321', NULL, NULL, NULL, NULL, 'H', 5000.50, '7654321098', '3456789012'),
    ('TXN-270-001', '270', '13', 'IL', 'MANN', 'JOHN', '345678901', '07041990', 'M', NULL, NULL, NULL, NULL, NULL, NULL),
    ('TXN-837-003', '837', '00', 'IL', 'WILLIAMS', 'CAROL', '456789012A', '11301978', 'F', 'ACCT-5003', 890.00, 'C', 890.00, '6543210987', '4567890123');


-- ============================================================================
-- STEP 2: HL7 Pseudonymisation Rules
-- ============================================================================
-- HL7 messages contain HIPAA identifiers in PID segments. These rules
-- protect patient identity while preserving clinical utility.

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.hl7_patients (pid_3)
    TRANSFORM keyed_hash
    SCOPE person
    PRIORITY 10
    PARAMS (salt = 'hl7_mrn_salt_2024');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.hl7_patients (pid_19)
    TRANSFORM redact
    PRIORITY 20
    PARAMS (mask = '***-**-****');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.hl7_patients (pid_7)
    TRANSFORM generalize
    SCOPE relationship
    PARAMS (range = 10000);

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.hl7_patients (pid_13)
    TRANSFORM mask
    PARAMS (show = 5);

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.hl7_patients (pid_11)
    TRANSFORM hash;

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.hl7_patients (pid_5)
    TRANSFORM tokenize
    SCOPE person
    PRIORITY 5;


-- ============================================================================
-- STEP 3: FHIR Pseudonymisation Rules
-- ============================================================================
-- FHIR resources use human-readable field names. Wildcard patterns
-- efficiently protect multiple columns matching a naming convention.

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.fhir_patients (email)
    TRANSFORM encrypt
    SCOPE person
    PRIORITY 10
    PARAMS (algorithm = 'AES256');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.fhir_patients (patient_id)
    TRANSFORM tokenize
    SCOPE person
    PRIORITY 10;

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.fhir_patients (ssn)
    TRANSFORM keyed_hash
    SCOPE person
    PARAMS (salt = 'fhir_ssn_salt_2024');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.fhir_patients (phone)
    TRANSFORM mask
    PRIORITY 5
    PARAMS (show = 4);

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.fhir_patients (mrn)
    TRANSFORM redact
    PARAMS (mask = '[REDACTED]');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.fhir_patients (address_*)
    TRANSFORM hash
    PRIORITY 1;

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.fhir_patients (birth_date)
    TRANSFORM generalize
    SCOPE relationship
    PARAMS (range = 10);

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.fhir_patients (*_name)
    TRANSFORM keyed_hash
    SCOPE person
    PRIORITY 3
    PARAMS (salt = 'fhir_name_salt_2024');


-- ============================================================================
-- STEP 4: EDI / HIPAA Pseudonymisation Rules
-- ============================================================================
-- EDI X12 segments contain patient identifiers (NM1), financial data (BPR),
-- and claim details (CLM). Pseudonymisation must satisfy HIPAA Privacy Rule
-- while preserving enough structure for claims processing analytics.

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.edi_claims (nm1_8)
    TRANSFORM keyed_hash
    SCOPE person
    PRIORITY 20
    PARAMS (salt = 'edi_member_id_salt_2024');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.edi_claims (bpr_8)
    TRANSFORM redact
    PRIORITY 20
    PARAMS (mask = '**********');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.edi_claims (bpr_14)
    TRANSFORM redact
    PRIORITY 20
    PARAMS (mask = '**********');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.edi_claims (clm_1)
    TRANSFORM tokenize
    SCOPE transaction
    PRIORITY 10;

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.edi_claims (clm_2)
    TRANSFORM mask
    PARAMS (show = 2);

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.edi_claims (nm1_3)
    TRANSFORM keyed_hash
    SCOPE person
    PARAMS (salt = 'edi_name_salt_2024');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.edi_claims (nm1_4)
    TRANSFORM keyed_hash
    SCOPE person
    PARAMS (salt = 'edi_name_salt_2024');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.edi_claims (dmg_1)
    TRANSFORM generalize
    SCOPE relationship
    PARAMS (range = 10000);
