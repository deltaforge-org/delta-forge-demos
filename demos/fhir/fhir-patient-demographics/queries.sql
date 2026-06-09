-- ============================================================================
-- FHIR Patient Demographics — Queries
-- ============================================================================
-- Explore FHIR R5 Patient resources ingested from both bulk NDJSON exports
-- and individual JSON files. These queries demonstrate how DeltaForge
-- flattens deeply nested FHIR structures into queryable columns.
-- ============================================================================


-- ============================================================================
-- 1. BULK PATIENT ROSTER
-- ============================================================================
-- The NDJSON bulk export contains 50 Patient resources, one per line. Each
-- record was flattened from a FHIR Patient resource into a flat row. The
-- "name" column contains the FHIR HumanName array as a JSON string, which
-- you can inspect or parse further.

ASSERT ROW_COUNT = 15
ASSERT VALUE gender = male WHERE patient_id = 'bulk-patient-1'
ASSERT VALUE birth_date = 1976-02-17 WHERE patient_id = 'bulk-patient-1'
ASSERT VALUE gender = female WHERE patient_id = 'bulk-patient-2'
SELECT patient_id, name, gender, birth_date
FROM {{zone_name}}.fhir_demos.patients_bulk
ORDER BY patient_id
LIMIT 15;


-- ============================================================================
-- 2. GENDER DISTRIBUTION — How many male vs female patients in the bulk set?
-- ============================================================================
-- FHIR Patient.gender uses a required ValueSet: male | female | other | unknown.
-- This query groups the 50 bulk patients by gender to show the distribution.

-- FHIR uses male|female|other|unknown; expect 2 gender groups in the bulk set
ASSERT ROW_COUNT = 2
ASSERT VALUE patient_count = 26 WHERE gender = 'male'
ASSERT VALUE patient_count = 24 WHERE gender = 'female'
SELECT gender, COUNT(*) AS patient_count
FROM {{zone_name}}.fhir_demos.patients_bulk
GROUP BY gender
ORDER BY patient_count DESC;


-- ============================================================================
-- 3. BIRTH DECADE ANALYSIS — Patient age distribution by decade
-- ============================================================================
-- Each Patient resource has a birthDate field (YYYY-MM-DD format). By
-- extracting the decade, we can visualize the age distribution of the
-- patient population — useful for population health analytics.

-- All 50 bulk patients have birth_date; 5 decades from 1950s to 1990s
ASSERT ROW_COUNT = 5
ASSERT VALUE patient_count = 13 WHERE birth_decade = '1950s'
ASSERT VALUE patient_count = 8 WHERE birth_decade = '1970s'
SELECT CONCAT(SUBSTRING(birth_date, 1, 3), '0s') AS birth_decade,
       COUNT(*) AS patient_count
FROM {{zone_name}}.fhir_demos.patients_bulk
WHERE birth_date IS NOT NULL
GROUP BY SUBSTRING(birth_date, 1, 3)
ORDER BY birth_decade;


-- ============================================================================
-- 4. DETAILED PATIENT RECORDS — Rich FHIR resources with nested data
-- ============================================================================
-- The individual Patient JSON files contain much richer data than the bulk
-- export: multiple name variants (official, usual, maiden), telecom contacts
-- (phone, email), full addresses, and managing organization references.
-- Notice how some fields are NULL for simpler patient files — this is FHIR
-- schema evolution in action.

ASSERT ROW_COUNT = 7
ASSERT VALUE gender = male WHERE patient_id = 'example'
ASSERT VALUE birth_date = 1974-12-25 WHERE patient_id = 'example'
ASSERT VALUE gender = male WHERE patient_id = 'f001'
ASSERT VALUE birth_date = 1944-11-17 WHERE patient_id = 'f001'
ASSERT VALUE gender = female WHERE patient_id = 'pat4'
ASSERT VALUE birth_date = 1960-03-13 WHERE patient_id = 'f201'
SELECT patient_id, gender, birth_date, active, is_deceased,
       name, telecom, address
FROM {{zone_name}}.fhir_demos.patients_detailed
ORDER BY patient_id;


-- ============================================================================
-- 5. SCHEMA EVOLUTION — Which patients have each optional field populated?
-- ============================================================================
-- FHIR resources have many optional fields. Different source systems populate
-- different subsets. This query shows which optional fields are present in
-- each patient file, demonstrating how DeltaForge handles schema variation
-- across files by unioning all discovered columns and filling gaps with NULL.

ASSERT ROW_COUNT = 7
ASSERT VALUE has_marital = Y WHERE patient_id = 'f001'
ASSERT VALUE has_marital = Y WHERE patient_id = 'f201'
ASSERT VALUE has_deceased = Y WHERE patient_id = 'pat4'
SELECT patient_id,
       CASE WHEN active IS NOT NULL THEN 'Y' ELSE '-' END AS has_active,
       CASE WHEN is_deceased IS NOT NULL THEN 'Y' ELSE '-' END AS has_deceased,
       CASE WHEN telecom IS NOT NULL THEN 'Y' ELSE '-' END AS has_telecom,
       CASE WHEN address IS NOT NULL THEN 'Y' ELSE '-' END AS has_address,
       CASE WHEN marital_status IS NOT NULL THEN 'Y' ELSE '-' END AS has_marital,
       CASE WHEN managing_org IS NOT NULL THEN 'Y' ELSE '-' END AS has_org,
       df_file_name
FROM {{zone_name}}.fhir_demos.patients_detailed
ORDER BY patient_id;


-- ============================================================================
-- 6. FILE PROVENANCE — Which source file did each patient come from?
-- ============================================================================
-- The file_metadata option injects df_file_name into every row, making it
-- easy to trace any record back to its original FHIR resource file. This is
-- critical for healthcare data lineage and audit trails.

ASSERT ROW_COUNT = 7
-- Each individual JSON file contains exactly one patient, so df_row_number = 1 for all detailed rows
ASSERT VALUE df_row_number = 1 WHERE patient_id = 'f001'
ASSERT VALUE gender = other WHERE patient_id = 'pat2'
SELECT df_file_name, df_row_number, patient_id, gender, birth_date
FROM {{zone_name}}.fhir_demos.patients_detailed
ORDER BY df_file_name;


-- ============================================================================
-- 7. BULK EXPORT SOURCE TRACKING — Verify all 50 records from one NDJSON file
-- ============================================================================
-- All 50 bulk patients come from Patient.ndjson. The df_row_number column
-- shows the line number within the NDJSON file (1-based), confirming correct
-- line-by-line parsing of the bulk export.

-- All 50 bulk patients come from a single NDJSON file
ASSERT ROW_COUNT = 1
ASSERT VALUE total_patients = 50
ASSERT VALUE first_row = 1
ASSERT VALUE last_row = 50
SELECT df_file_name,
       MIN(df_row_number) AS first_row,
       MAX(df_row_number) AS last_row,
       COUNT(*) AS total_patients
FROM {{zone_name}}.fhir_demos.patients_bulk
GROUP BY df_file_name;


-- ============================================================================
-- 8. COMBINED PATIENT COUNT — Total patients across both tables
-- ============================================================================
-- This query combines both data sources to show the total patient population:
-- 50 from the bulk NDJSON export + 7 from detailed individual files = 57
-- total patient records available for analysis.

-- 50 bulk + 7 detailed = 2 rows
ASSERT ROW_COUNT = 2
ASSERT VALUE count = 50 WHERE source = 'Bulk NDJSON'
ASSERT VALUE count = 7 WHERE source = 'Detailed JSON'
SELECT 'Bulk NDJSON' AS source, COUNT(*) AS count FROM {{zone_name}}.fhir_demos.patients_bulk
UNION ALL
SELECT 'Detailed JSON' AS source, COUNT(*) AS count FROM {{zone_name}}.fhir_demos.patients_detailed
ORDER BY source;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: table counts, column mapping, and file metadata.

ASSERT ROW_COUNT = 7
ASSERT NO_FAIL IN result
SELECT check_name, result FROM (

    -- Check 1: Bulk patient count = 50
    SELECT 'bulk_count_50' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.patients_bulk) = 50
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Detailed patient count = 7
    SELECT 'detailed_count_7' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.patients_detailed) = 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: All bulk patients have gender populated
    SELECT 'bulk_gender_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.patients_bulk WHERE gender IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: All bulk patients have birth_date
    SELECT 'bulk_birthdate_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.patients_bulk WHERE birth_date IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Column mapping applied — patient_id exists (not "id")
    SELECT 'column_mapping_patient_id' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.patients_bulk WHERE patient_id IS NOT NULL) = 50
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: File metadata populated for bulk
    SELECT 'file_metadata_bulk' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.patients_bulk WHERE df_file_name IS NOT NULL) = 50
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Detailed patients have name data
    SELECT 'detailed_names_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.patients_detailed WHERE name IS NOT NULL) > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
