-- ============================================================================
-- Demo: Healthcare Interoperability Onboarding - Queries
-- ============================================================================
-- One bronze landing zone, three healthcare standards, each in its own folder:
--   hl7/   6 HL7 v2 ADT messages
--   fhir/  20 FHIR Patient resources (NDJSON bulk export)
--   edi/   4 X12 EDI transactions (3x 837 claims + 1x 835 remittance)
-- For each standard: DISCOVER auto-detects and registers it, we query the
-- bronze feed on the fly, then convert it into a governed silver Delta table.
-- Every assertion value below was precomputed from the data files.
-- ============================================================================


-- ############################################################################
-- PART 1: HL7 v2 ADT messages
-- ############################################################################

-- ============================================================================
-- Query 1: DISCOVER the HL7 folder in PRINT mode - review the generated DDL
-- ============================================================================
-- PRINT shows the CREATE EXTERNAL TABLE DISCOVER would run, without registering
-- anything. Detection reads the bytes, recognises the HL7 MSH segment header,
-- and registers USING HL7 (MSH header fields plus the full message as JSON).

DISCOVER {{zone_name}}.healthcare.adt_bronze
    PATH 'discover-healthcare-interop/hl7'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER the HL7 folder in EXECUTE mode - register adt_bronze
-- ============================================================================

DISCOVER {{zone_name}}.healthcare.adt_bronze
    PATH 'discover-healthcare-interop/hl7'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: Query the HL7 bronze feed on the fly
-- ============================================================================
-- msh_3 (sending application), msh_9 (message type), msh_12 (HL7 version) are
-- first-class columns. Three EHR systems, three HL7 versions, six messages.

ASSERT ROW_COUNT = 3
ASSERT VALUE messages = 2 WHERE hl7_version = '2.3'
ASSERT VALUE messages = 2 WHERE hl7_version = '2.5.1'
ASSERT VALUE messages = 2 WHERE hl7_version = '2.6'
SELECT
    msh_12 AS hl7_version,
    COUNT(*) AS messages
FROM {{zone_name}}.healthcare.adt_bronze
GROUP BY msh_12
ORDER BY hl7_version;

-- ============================================================================
-- Query 4: Convert HL7 bronze into a typed silver Delta table
-- ============================================================================
-- The MSH header fields become first-class typed columns in a managed Delta
-- table the analytics warehouse owns.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.healthcare.hl7_admissions (
    sending_app  STRING,
    facility     STRING,
    message_type STRING,
    control_id   STRING,
    hl7_version  STRING
) LOCATION 'discover-healthcare-interop/silver/hl7_admissions';

-- ============================================================================
-- Query 5: Populate the HL7 silver table from the discovered bronze feed
-- ============================================================================

INSERT INTO {{zone_name}}.healthcare.hl7_admissions
SELECT
    msh_3  AS sending_app,
    msh_4  AS facility,
    msh_9  AS message_type,
    msh_10 AS control_id,
    msh_12 AS hl7_version
FROM {{zone_name}}.healthcare.adt_bronze;

-- ============================================================================
-- Query 6: Read the HL7 silver Delta table
-- ============================================================================
-- Same six admissions, now governed Delta rows. Grouping by the ADT trigger
-- event family (the first seven characters of message_type, e.g. ADT^A01).

ASSERT ROW_COUNT = 3
ASSERT VALUE admissions = 3 WHERE event_family = 'ADT^A01'
ASSERT VALUE admissions = 2 WHERE event_family = 'ADT^A03'
ASSERT VALUE admissions = 1 WHERE event_family = 'ADT^A08'
SELECT
    SUBSTRING(message_type FROM 1 FOR 7) AS event_family,
    COUNT(*) AS admissions
FROM {{zone_name}}.healthcare.hl7_admissions
GROUP BY SUBSTRING(message_type FROM 1 FOR 7)
ORDER BY event_family;


-- ############################################################################
-- PART 2: FHIR Patient resources (NDJSON bulk export)
-- ############################################################################

-- ============================================================================
-- Query 7: DISCOVER the FHIR folder in PRINT mode
-- ============================================================================
-- Detection recognises the FHIR content (newline-delimited bulk export) and
-- registers USING FHIR: resourcetype and id as columns, the full resource as
-- df_resource_json.

DISCOVER {{zone_name}}.healthcare.patients_bronze
    PATH 'discover-healthcare-interop/fhir'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 8: DISCOVER the FHIR folder in EXECUTE mode - register patients_bronze
-- ============================================================================

DISCOVER {{zone_name}}.healthcare.patients_bronze
    PATH 'discover-healthcare-interop/fhir'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 9: Query the FHIR bronze cohort on the fly
-- ============================================================================
-- resourcetype is a first-class column; clinical fields are read out of the
-- resource JSON with json_extract_path_text. Every row is a Patient.

ASSERT ROW_COUNT = 1
ASSERT VALUE patients = 20 WHERE resourcetype = 'Patient'
SELECT
    resourcetype,
    COUNT(*) AS patients
FROM {{zone_name}}.healthcare.patients_bronze
GROUP BY resourcetype;

-- ============================================================================
-- Query 10: Convert FHIR bronze into a typed silver Delta table
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.healthcare.fhir_patients (
    patient_id  STRING,
    family_name STRING,
    given_name  STRING,
    gender      STRING,
    birth_date  STRING,
    active      STRING
) LOCATION 'discover-healthcare-interop/silver/fhir_patients';

-- ============================================================================
-- Query 11: Populate the FHIR silver table, extracting fields from the resource
-- ============================================================================

INSERT INTO {{zone_name}}.healthcare.fhir_patients
SELECT
    id                                                            AS patient_id,
    json_extract_path_text(df_resource_json, 'name', '0', 'family')   AS family_name,
    json_extract_path_text(df_resource_json, 'name', '0', 'given', '0') AS given_name,
    json_extract_path_text(df_resource_json, 'gender')            AS gender,
    json_extract_path_text(df_resource_json, 'birthDate')         AS birth_date,
    json_extract_path_text(df_resource_json, 'active')            AS active
FROM {{zone_name}}.healthcare.patients_bronze;

-- ============================================================================
-- Query 12: Read the FHIR silver Delta table
-- ============================================================================
-- The gender distribution, now typed Delta columns rather than nested JSON.

ASSERT ROW_COUNT = 3
ASSERT VALUE patients = 7 WHERE gender = 'male'
ASSERT VALUE patients = 7 WHERE gender = 'female'
ASSERT VALUE patients = 6 WHERE gender = 'other'
SELECT
    gender,
    COUNT(*) AS patients
FROM {{zone_name}}.healthcare.fhir_patients
GROUP BY gender
ORDER BY patients DESC, gender;

-- ============================================================================
-- Query 13: Before protection, the silver table holds real patient identifiers
-- ============================================================================
-- The MRN-style id and the patient name are sensitive PHI. Right now they are
-- in the clear.

ASSERT ROW_COUNT = 1
ASSERT VALUE family_name = 'Family01'
ASSERT VALUE given_name = 'Given01'
SELECT patient_id, family_name, given_name, gender
FROM {{zone_name}}.healthcare.fhir_patients
WHERE patient_id = 'pat-001';

-- ============================================================================
-- Query 14: Add data-protection rules on the fly (query-time pseudonymisation)
-- ============================================================================
-- Tokenize the patient identifier and encrypt the name. The rules transform
-- matching columns transparently on every read; no data is rewritten here.

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.healthcare.fhir_patients (patient_id)
    TRANSFORM tokenize
    SCOPE PERSON
    PARAMS (format = 'TOK');

-- ============================================================================
-- Query 15: Encrypt every name column with one wildcard rule
-- ============================================================================

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.healthcare.fhir_patients (*_name)
    TRANSFORM encrypt
    SCOPE PERSON;

-- ============================================================================
-- Query 16: After protection, every read is tokenized and encrypted
-- ============================================================================
-- Same 20 patients, but the identifier comes back as an opaque TOK_ token and
-- the names are ciphertext. No raw name leaks. Counts still work because the
-- transform applies to output, not to aggregates.

ASSERT ROW_COUNT = 1
ASSERT VALUE patients = 20
ASSERT VALUE tokenized_ids = 20
ASSERT VALUE encrypted_names = 20
ASSERT VALUE raw_names_leaked = 0
SELECT
    COUNT(*)                                              AS patients,
    COUNT(*) FILTER (WHERE patient_id LIKE 'TOK%')      AS tokenized_ids,
    COUNT(*) FILTER (WHERE family_name NOT LIKE 'Family%') AS encrypted_names,
    COUNT(*) FILTER (WHERE family_name LIKE 'Family%')    AS raw_names_leaked
FROM {{zone_name}}.healthcare.fhir_patients;

-- ============================================================================
-- Query 17: Share to the cloud warehouse without the raw PHI
-- ============================================================================
-- Build the shareable table from the PROTECTED read. Because the read is
-- already tokenized and encrypted, the stored bytes of this table never
-- contain the original identifiers.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.healthcare.fhir_patients_shared (
    patient_id  STRING,
    family_name STRING,
    given_name  STRING,
    gender      STRING,
    birth_date  STRING
) LOCATION 'discover-healthcare-interop/silver/fhir_patients_shared';

-- ============================================================================
-- Query 18: Populate the shareable table from the protected read
-- ============================================================================

INSERT INTO {{zone_name}}.healthcare.fhir_patients_shared
SELECT patient_id, family_name, given_name, gender, birth_date
FROM {{zone_name}}.healthcare.fhir_patients;

-- ============================================================================
-- Query 19: The shared table itself carries no protection rule, yet stores no PHI
-- ============================================================================
-- It has no pseudonymisation rule of its own, but every identifier in it is
-- already a token and every name is already ciphertext: the raw PHI never
-- reached the stored bytes.

ASSERT ROW_COUNT = 1
ASSERT VALUE total = 20
ASSERT VALUE tokenized_ids = 20
ASSERT VALUE raw_names_leaked = 0
SELECT
    COUNT(*)                                              AS total,
    COUNT(*) FILTER (WHERE patient_id LIKE 'TOK%')      AS tokenized_ids,
    COUNT(*) FILTER (WHERE family_name LIKE 'Family%')    AS raw_names_leaked
FROM {{zone_name}}.healthcare.fhir_patients_shared;


-- ############################################################################
-- PART 3: X12 EDI claims and remittance
-- ############################################################################

-- ============================================================================
-- Query 13: DISCOVER the EDI folder in PRINT mode
-- ============================================================================
-- Detection recognises the X12 ISA envelope and registers USING EDI: per-
-- segment columns (st_1 carries the transaction set type) plus the full
-- transaction as JSON.

DISCOVER {{zone_name}}.healthcare.claims_bronze
    PATH 'discover-healthcare-interop/edi'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 14: DISCOVER the EDI folder in EXECUTE mode - register claims_bronze
-- ============================================================================

DISCOVER {{zone_name}}.healthcare.claims_bronze
    PATH 'discover-healthcare-interop/edi'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 15: Query the EDI bronze feed on the fly
-- ============================================================================
-- st_1 is the X12 transaction set type: 837 is a health care claim, 835 is the
-- remittance advice. Three claims, one remittance.

ASSERT ROW_COUNT = 2
ASSERT VALUE transactions = 3 WHERE transaction_type = '837'
ASSERT VALUE transactions = 1 WHERE transaction_type = '835'
SELECT
    st_1 AS transaction_type,
    COUNT(*) AS transactions
FROM {{zone_name}}.healthcare.claims_bronze
GROUP BY st_1
ORDER BY transactions DESC, transaction_type;

-- ============================================================================
-- Query 16: Convert EDI bronze into a typed silver Delta table
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.healthcare.edi_claims (
    transaction_type STRING,
    source_file      STRING
) LOCATION 'discover-healthcare-interop/silver/edi_claims';

-- ============================================================================
-- Query 17: Populate the EDI silver table from the discovered bronze feed
-- ============================================================================

INSERT INTO {{zone_name}}.healthcare.edi_claims
SELECT
    st_1         AS transaction_type,
    df_file_name AS source_file
FROM {{zone_name}}.healthcare.claims_bronze;

-- ============================================================================
-- Query 18: Read the EDI silver Delta table
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE transaction_type = '835' WHERE source_file LIKE '%remittance_835%'
SELECT
    transaction_type,
    source_file
FROM {{zone_name}}.healthcare.edi_claims
ORDER BY source_file;


-- ############################################################################
-- VERIFY: All Checks
-- ############################################################################

-- ============================================================================
-- Query 19: Cross-format onboarding summary
-- ============================================================================
-- One DISCOVER-driven flow turned three healthcare standards into three
-- governed Delta tables: 6 HL7 admissions, 20 FHIR patients, 4 EDI transactions.

ASSERT ROW_COUNT = 1
ASSERT VALUE hl7_admissions = 6
ASSERT VALUE fhir_patients = 20
ASSERT VALUE edi_transactions = 4
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.healthcare.hl7_admissions) AS hl7_admissions,
    (SELECT COUNT(*) FROM {{zone_name}}.healthcare.fhir_patients)  AS fhir_patients,
    (SELECT COUNT(*) FROM {{zone_name}}.healthcare.edi_claims)     AS edi_transactions;
