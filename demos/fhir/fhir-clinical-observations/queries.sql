-- ============================================================================
-- FHIR Clinical Observations — Queries
-- ============================================================================
-- Explore FHIR R5 Observation resources covering vital signs, lab results,
-- and clinical assessments. These queries demonstrate how DeltaForge
-- flattens complex FHIR clinical data into analyst-friendly SQL tables.
-- ============================================================================


-- ============================================================================
-- 1. BULK HEART RATE OVERVIEW
-- ============================================================================
-- The bulk NDJSON file contains 100 heart rate observations (LOINC code
-- 8867-4). Each record links to a patient via the "subject" reference and
-- contains a valueQuantity with the heart rate in beats/minute. This is the
-- most common pattern in FHIR bulk data exports.

ASSERT ROW_COUNT = 15
ASSERT VALUE status = final WHERE observation_id = 'bulk-observation-87'
ASSERT VALUE status = final WHERE observation_id = 'bulk-observation-73'
SELECT observation_id, status, subject, effective_date, value_quantity
FROM {{zone_name}}.fhir_demos.observations_bulk
ORDER BY effective_date
LIMIT 15;


-- ============================================================================
-- 2. HEART RATE STATISTICS — Population-level vital signs analytics
-- ============================================================================
-- From 100 heart rate observations, compute basic statistics. In clinical
-- practice, heart rates typically range 60-100 bpm for adults at rest.
-- Values outside this range may indicate tachycardia (>100) or bradycardia
-- (<60). The FHIR valueQuantity stores value and unit together.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_readings = 100
ASSERT VALUE first_record = 1
ASSERT VALUE last_record = 100
SELECT COUNT(*) AS total_readings,
       MIN(df_row_number) AS first_record,
       MAX(df_row_number) AS last_record
FROM {{zone_name}}.fhir_demos.observations_bulk;


-- ============================================================================
-- 3. CLINICAL OBSERVATIONS OVERVIEW — All 14 individual observations
-- ============================================================================
-- Each file represents a different clinical measurement type. Notice how
-- the "code" column contains FHIR CodeableConcept JSON with LOINC/SNOMED
-- coding systems. The observation_id uniquely identifies each measurement,
-- and status indicates whether the observation is "final", "preliminary",
-- "amended", or "cancelled".

ASSERT ROW_COUNT = 14
ASSERT VALUE status = final WHERE observation_id = 'bmi'
ASSERT VALUE status = final WHERE observation_id = 'body-height'
ASSERT VALUE status = final WHERE observation_id = 'glasgow'
SELECT observation_id, status, code, effective_date, value_quantity
FROM {{zone_name}}.fhir_demos.observations_clinical
ORDER BY observation_id;


-- ============================================================================
-- 4. SCHEMA EVOLUTION ACROSS OBSERVATION TYPES
-- ============================================================================
-- Different FHIR Observation types populate different fields. Vital signs
-- use valueQuantity (a single number + unit), blood pressure uses component[]
-- (systolic + diastolic as sub-observations), and Glasgow Coma Scale also
-- uses component[]. Lab results often include referenceRange and
-- interpretation. This query shows which fields are populated per observation.

ASSERT ROW_COUNT = 14
ASSERT VALUE has_component = Y WHERE observation_id = 'blood-pressure'
ASSERT VALUE has_value = - WHERE observation_id = 'blood-pressure'
ASSERT VALUE has_ref_range = Y WHERE observation_id = 'f001'
ASSERT VALUE has_interp = Y WHERE observation_id = 'f001'
ASSERT VALUE has_category = Y WHERE observation_id = 'bmi'
ASSERT VALUE df_file_name LIKE '%observation-example-bloodpressure%' WHERE observation_id = 'blood-pressure'
SELECT observation_id,
       CASE WHEN value_quantity IS NOT NULL THEN 'Y' ELSE '-' END AS has_value,
       CASE WHEN value_string IS NOT NULL THEN 'Y' ELSE '-' END AS has_string,
       CASE WHEN component IS NOT NULL THEN 'Y' ELSE '-' END AS has_component,
       CASE WHEN reference_range IS NOT NULL THEN 'Y' ELSE '-' END AS has_ref_range,
       CASE WHEN interpretation IS NOT NULL THEN 'Y' ELSE '-' END AS has_interp,
       CASE WHEN category IS NOT NULL THEN 'Y' ELSE '-' END AS has_category,
       df_file_name
FROM {{zone_name}}.fhir_demos.observations_clinical
ORDER BY observation_id;


-- ============================================================================
-- 5. OBSERVATION STATUS DISTRIBUTION
-- ============================================================================
-- FHIR Observation.status is a required field with values: registered |
-- preliminary | final | amended | corrected | cancelled | entered-in-error |
-- unknown. Most clinical observations are "final" after verification. This
-- query shows the status distribution across all clinical observations.

ASSERT ROW_COUNT = 1
ASSERT VALUE status = final
ASSERT VALUE obs_count = 14
SELECT status, COUNT(*) AS obs_count
FROM {{zone_name}}.fhir_demos.observations_clinical
GROUP BY status
ORDER BY obs_count DESC;


-- ============================================================================
-- 6. FILE PROVENANCE — Source file tracking for clinical audit
-- ============================================================================
-- Every observation can be traced back to its source FHIR JSON file.
-- In healthcare data pipelines, this lineage is essential for regulatory
-- compliance (HIPAA, GDPR) and clinical data quality auditing.

ASSERT ROW_COUNT = 14
ASSERT VALUE df_file_name LIKE '%observation-example-bmi%' WHERE observation_id = 'bmi'
ASSERT VALUE df_file_name LIKE '%observation-example-heart-rate%' WHERE observation_id = 'heart-rate'
SELECT df_file_name, observation_id, status
FROM {{zone_name}}.fhir_demos.observations_clinical
ORDER BY df_file_name;


-- ============================================================================
-- 7. PATIENT REFERENCES — Which patients have observations?
-- ============================================================================
-- FHIR Observation.subject is a Reference to the Patient resource. The bulk
-- export links observations to specific patients, enabling per-patient
-- clinical analysis. This query shows how many observations each referenced
-- patient has.

ASSERT ROW_COUNT = 10
ASSERT VALUE observation_count = 6 WHERE subject = '{"reference":"Patient/bulk-patient-41"}'
SELECT subject, COUNT(*) AS observation_count
FROM {{zone_name}}.fhir_demos.observations_bulk
GROUP BY subject
ORDER BY observation_count DESC
LIMIT 10;


-- ============================================================================
-- 8. OBSERVATIONS PER DAY — Temporal distribution of heart rate readings
-- ============================================================================
-- Clinical observations have timestamps (effectiveDateTime). This query
-- shows the distribution of heart rate readings over time, useful for
-- identifying measurement patterns and data collection schedules.

ASSERT ROW_COUNT = 25
ASSERT VALUE readings = 5 WHERE observation_date = '2024-01-01'
ASSERT VALUE readings = 9 WHERE observation_date = '2024-01-14'
ASSERT VALUE readings = 2 WHERE observation_date = '2024-01-28'
SELECT SUBSTRING(effective_date, 1, 10) AS observation_date,
       COUNT(*) AS readings
FROM {{zone_name}}.fhir_demos.observations_bulk
WHERE effective_date IS NOT NULL
GROUP BY SUBSTRING(effective_date, 1, 10)
ORDER BY observation_date;


-- ============================================================================
-- 9. COMBINED DATA LANDSCAPE — Total observations across both sources
-- ============================================================================
-- Overview of the complete clinical observation dataset, combining bulk
-- export data with individual clinical files.

ASSERT ROW_COUNT = 2
ASSERT VALUE count = 100 WHERE source = 'Bulk NDJSON (Heart Rate)'
ASSERT VALUE count = 14 WHERE source = 'Clinical JSON (Mixed Types)'
SELECT 'Bulk NDJSON (Heart Rate)' AS source, COUNT(*) AS count
FROM {{zone_name}}.fhir_demos.observations_bulk
UNION ALL
SELECT 'Clinical JSON (Mixed Types)' AS source, COUNT(*) AS count
FROM {{zone_name}}.fhir_demos.observations_clinical
ORDER BY source;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: bulk count, clinical count, data completeness.

ASSERT ROW_COUNT = 8
ASSERT VALUE result = PASS WHERE check_name = 'bulk_count_100'
ASSERT VALUE result = PASS WHERE check_name = 'clinical_count_14'
ASSERT VALUE result = PASS WHERE check_name = 'bulk_status_populated'
ASSERT VALUE result = PASS WHERE check_name = 'bulk_subject_populated'
ASSERT VALUE result = PASS WHERE check_name = 'column_mapping_obs_id'
ASSERT VALUE result = PASS WHERE check_name = 'clinical_code_populated'
ASSERT VALUE result = PASS WHERE check_name = 'file_metadata_bulk'
ASSERT VALUE result = PASS WHERE check_name = 'clinical_multi_file'
SELECT check_name, result FROM (

    -- Check 1: Bulk observation count = 100
    SELECT 'bulk_count_100' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.observations_bulk) = 100
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Clinical observation count = 14
    SELECT 'clinical_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.observations_clinical) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: All bulk observations have status
    SELECT 'bulk_status_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.observations_bulk WHERE status IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: All bulk observations have subject (patient reference)
    SELECT 'bulk_subject_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.observations_bulk WHERE subject IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Column mapping — observation_id exists
    SELECT 'column_mapping_obs_id' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.observations_bulk WHERE observation_id IS NOT NULL) = 100
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Clinical observations have code
    SELECT 'clinical_code_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.observations_clinical WHERE code IS NOT NULL) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: File metadata on bulk
    SELECT 'file_metadata_bulk' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.observations_bulk WHERE df_file_name IS NOT NULL) = 100
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Multiple source files for clinical
    SELECT 'clinical_multi_file' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.fhir_demos.observations_clinical) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
