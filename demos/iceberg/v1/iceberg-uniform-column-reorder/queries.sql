-- ============================================================================
-- Iceberg UniForm Column Reorder — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH COLUMN REORDERING
-- ----------------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- When ALTER TABLE ALTER COLUMN ... FIRST or ... AFTER runs, Delta Forge:
--   1. Updates the Delta schema in _delta_log/ with new column positions
--   2. Adds a new schema entry to metadata.json's "schemas" array with
--      field IDs in the updated order (Iceberg uses column IDs, not names,
--      so physical data files do not need rewriting)
--
-- Column reordering is a metadata-only operation — no data files are
-- rewritten. The Iceberg metadata records position changes so that
-- Iceberg-compatible engines see the new column layout.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify column order in metadata with:
--   python3 verify_iceberg_metadata.py <table_data_path>/patient_records -v
-- ============================================================================
-- ============================================================================
-- EXPLORE: Baseline — 20 Patient Records (Version 1)
-- ============================================================================
-- Original column order: record_id, last_name, first_name, dob, mrn,
-- diagnosis_code, admission_date, discharge_date, attending_physician.

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.patient_records ORDER BY record_id;
-- ============================================================================
-- Query 1: Baseline — Per-Diagnosis-Code Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 12
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'I25.10'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'I48.0'
ASSERT VALUE patient_count = 1 WHERE diagnosis_code = 'I50.9'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'M17.11'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'S72.001'
ASSERT VALUE patient_count = 1 WHERE diagnosis_code = 'M54.5'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'G43.909'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'G20'
ASSERT VALUE patient_count = 1 WHERE diagnosis_code = 'G30.9'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'C34.90'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'C50.911'
ASSERT VALUE patient_count = 1 WHERE diagnosis_code = 'C18.9'
SELECT
    diagnosis_code,
    COUNT(*) AS patient_count
FROM {{zone_name}}.iceberg_demos.patient_records
GROUP BY diagnosis_code
ORDER BY diagnosis_code;
-- ============================================================================
-- Query 2: Baseline Summary Stats
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_records = 20
ASSERT VALUE distinct_codes = 12
ASSERT VALUE distinct_physicians = 8
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT diagnosis_code) AS distinct_codes,
    COUNT(DISTINCT attending_physician) AS distinct_physicians
FROM {{zone_name}}.iceberg_demos.patient_records;
-- ============================================================================
-- LEARN: Column Reorder Step 1 — Move MRN to First Position (Version 2)
-- ============================================================================
-- In HL7 FHIR standard ordering, the Medical Record Number (MRN) is the
-- primary patient identifier and should appear first. This is a
-- metadata-only operation — no data files are rewritten.

ALTER TABLE {{zone_name}}.iceberg_demos.patient_records ALTER COLUMN mrn FIRST;
-- ============================================================================
-- LEARN: Column Reorder Step 2 — Move first_name After MRN (Version 3)
-- ============================================================================
-- FHIR ordering: identifier → name (given, family) → demographics.

ALTER TABLE {{zone_name}}.iceberg_demos.patient_records ALTER COLUMN first_name AFTER mrn;
-- ============================================================================
-- LEARN: Column Reorder Step 3 — Move last_name After first_name (Version 4)
-- ============================================================================
-- Final FHIR-compliant order: mrn, first_name, last_name, record_id, dob,
-- diagnosis_code, admission_date, discharge_date, attending_physician.

ALTER TABLE {{zone_name}}.iceberg_demos.patient_records ALTER COLUMN last_name AFTER first_name;
-- ============================================================================
-- Query 3: Verify Column Order Changed but Data Intact
-- ============================================================================
-- All 20 rows should still be present with identical values.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_records = 20
ASSERT VALUE distinct_mrns = 20
ASSERT VALUE distinct_codes = 12
ASSERT VALUE distinct_physicians = 8
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT mrn) AS distinct_mrns,
    COUNT(DISTINCT diagnosis_code) AS distinct_codes,
    COUNT(DISTINCT attending_physician) AS distinct_physicians
FROM {{zone_name}}.iceberg_demos.patient_records;
-- ============================================================================
-- Query 4: Verify New Column Order — SELECT * Shows Reordered Layout
-- ============================================================================
-- The column order should now be: mrn, first_name, last_name, record_id,
-- dob, diagnosis_code, admission_date, discharge_date, attending_physician.

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.patient_records ORDER BY record_id;
-- ============================================================================
-- LEARN: Insert New Records in New Column Order (Version 5)
-- ============================================================================
-- New admissions inserted after reorder. One per specialty to keep balance.

INSERT INTO {{zone_name}}.iceberg_demos.patient_records VALUES
    ('MRN-1021', 'Andrew',   'Lee',      21, '1983-05-14', 'I25.10',  '2025-02-01', '2025-02-08', 'Dr. Chen'),
    ('MRN-1022', 'Sarah',    'Walker',   22, '1976-09-30', 'M54.5',   '2025-02-03', '2025-02-06', 'Dr. Lopez'),
    ('MRN-1023', 'George',   'Hall',     23, '1959-01-18', 'G30.9',   '2025-02-05', '2025-02-19', 'Dr. Singh'),
    ('MRN-1024', 'Margaret', 'Allen',    24, '1966-10-07', 'C18.9',   '2025-02-07', '2025-02-21', 'Dr. Reeves');
-- ============================================================================
-- Query 5: Verify Combined Data — Original + New Records
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_records = 24
ASSERT VALUE distinct_mrns = 24
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT mrn) AS distinct_mrns
FROM {{zone_name}}.iceberg_demos.patient_records;
-- ============================================================================
-- Query 6: Updated Per-Diagnosis-Code Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 12
ASSERT VALUE patient_count = 3 WHERE diagnosis_code = 'I25.10'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'M54.5'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'G30.9'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'C18.9'
SELECT
    diagnosis_code,
    COUNT(*) AS patient_count
FROM {{zone_name}}.iceberg_demos.patient_records
GROUP BY diagnosis_code
ORDER BY diagnosis_code;
-- ============================================================================
-- Query 7: Time Travel — Read Version 1 (Original Column Order)
-- ============================================================================
-- Reading the pre-reorder version shows the original column layout:
-- record_id, last_name, first_name, dob, mrn, ...

ASSERT ROW_COUNT = 20
SELECT
    record_id, last_name, first_name, dob, mrn,
    diagnosis_code, admission_date, discharge_date, attending_physician
FROM {{zone_name}}.iceberg_demos.patient_records VERSION AS OF 1
ORDER BY record_id;
-- ============================================================================
-- Query 8: Version History — Column Reorder Trail
-- ============================================================================

ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.patient_records;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_records = 24
ASSERT VALUE distinct_codes = 12
ASSERT VALUE distinct_physicians = 8
ASSERT VALUE distinct_mrns = 24
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT diagnosis_code) AS distinct_codes,
    COUNT(DISTINCT attending_physician) AS distinct_physicians,
    COUNT(DISTINCT mrn) AS distinct_mrns
FROM {{zone_name}}.iceberg_demos.patient_records;
-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents the reordered column layout
-- (mrn first, then first_name, last_name, etc.).
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.patient_records_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.patient_records_iceberg
USING ICEBERG
LOCATION '{{data_path}}/patient_records';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.patient_records_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Verify 1: Full Row Count and Aggregates
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_records = 24
ASSERT VALUE distinct_mrns = 24
ASSERT VALUE distinct_codes = 12
ASSERT VALUE distinct_physicians = 8
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT mrn) AS distinct_mrns,
    COUNT(DISTINCT diagnosis_code) AS distinct_codes,
    COUNT(DISTINCT attending_physician) AS distinct_physicians
FROM {{zone_name}}.iceberg_demos.patient_records_iceberg;
-- ============================================================================
-- Iceberg Verify 2a: Pre-Reorder Record — Patient #1 (Smith, John)
-- ============================================================================
-- Verify specific patient data from BEFORE column reorder survived intact.

ASSERT ROW_COUNT = 1
ASSERT VALUE mrn = 'MRN-1001'
ASSERT VALUE first_name = 'John'
ASSERT VALUE last_name = 'Smith'
ASSERT VALUE dob = '1955-03-12'
ASSERT VALUE diagnosis_code = 'I25.10'
ASSERT VALUE attending_physician = 'Dr. Chen'
SELECT * FROM {{zone_name}}.iceberg_demos.patient_records_iceberg WHERE record_id = 1;
-- ============================================================================
-- Iceberg Verify 2b: Pre-Reorder Record — Patient #10 (Taylor, Susan)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE mrn = 'MRN-1010'
ASSERT VALUE first_name = 'Susan'
ASSERT VALUE last_name = 'Taylor'
ASSERT VALUE diagnosis_code = 'S72.001'
ASSERT VALUE admission_date = '2025-01-18'
ASSERT VALUE discharge_date = '2025-02-01'
ASSERT VALUE attending_physician = 'Dr. Kim'
SELECT * FROM {{zone_name}}.iceberg_demos.patient_records_iceberg WHERE record_id = 10;
-- ============================================================================
-- Iceberg Verify 2c: Pre-Reorder Record — Patient #20 (Clark, Elizabeth)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE mrn = 'MRN-1020'
ASSERT VALUE first_name = 'Elizabeth'
ASSERT VALUE last_name = 'Clark'
ASSERT VALUE diagnosis_code = 'C50.911'
ASSERT VALUE attending_physician = 'Dr. Okafor'
SELECT * FROM {{zone_name}}.iceberg_demos.patient_records_iceberg WHERE record_id = 20;
-- ============================================================================
-- Iceberg Verify 3a: Post-Reorder Insert — Patient #21 (Lee, Andrew)
-- ============================================================================
-- Inserted AFTER column reorder. Verifies Iceberg metadata correctly maps
-- values written in the new column order.

ASSERT ROW_COUNT = 1
ASSERT VALUE mrn = 'MRN-1021'
ASSERT VALUE first_name = 'Andrew'
ASSERT VALUE last_name = 'Lee'
ASSERT VALUE diagnosis_code = 'I25.10'
ASSERT VALUE admission_date = '2025-02-01'
ASSERT VALUE attending_physician = 'Dr. Chen'
SELECT * FROM {{zone_name}}.iceberg_demos.patient_records_iceberg WHERE record_id = 21;
-- ============================================================================
-- Iceberg Verify 3b: Post-Reorder Insert — Patient #24 (Allen, Margaret)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE mrn = 'MRN-1024'
ASSERT VALUE first_name = 'Margaret'
ASSERT VALUE last_name = 'Allen'
ASSERT VALUE diagnosis_code = 'C18.9'
ASSERT VALUE discharge_date = '2025-02-21'
ASSERT VALUE attending_physician = 'Dr. Reeves'
SELECT * FROM {{zone_name}}.iceberg_demos.patient_records_iceberg WHERE record_id = 24;
-- ============================================================================
-- Iceberg Verify 4: Diagnosis Counts Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 12
ASSERT VALUE patient_count = 3 WHERE diagnosis_code = 'I25.10'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'I48.0'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'M54.5'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'G30.9'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'C18.9'
ASSERT VALUE patient_count = 2 WHERE diagnosis_code = 'C50.911'
SELECT
    diagnosis_code,
    COUNT(*) AS patient_count
FROM {{zone_name}}.iceberg_demos.patient_records_iceberg
GROUP BY diagnosis_code
ORDER BY diagnosis_code;
