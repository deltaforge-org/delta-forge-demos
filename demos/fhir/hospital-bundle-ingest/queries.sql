-- ============================================================================
-- Hospital Interop — Daily Clinical Bundle Ingest (FHIR provider)
-- Queries
-- ============================================================================
-- Each query targets a specific guarantee of the dedicated `USING fhir`
-- provider when `unbundle = 'true'` and `include_bundle_metadata = 'true'`:
--
--   Q1   Resource-type row counts after unbundle + resource_types filter
--   Q2   df_full_url is preserved on every row (Bundle.entry.fullUrl)
--   Q3   bundle_id is preserved on every row (Bundle.id)
--   Q4   Distinct bundle count == file count
--   Q5   Nested CodeableConcept extraction (LOINC code lookup)
--   Q6   In-Bundle cross-reference resolution via df_full_url
--   Q7   Silver promotion table sanity
--   VERIFY  Cross-cutting summary
-- ============================================================================


-- ============================================================================
-- Q1: Row count by resource type (proves unbundle + resource_types filter)
-- ============================================================================
-- Bundles contain 4 Patient + 4 Encounter + 11 Observation + 5 MedicationRequest
-- entries. With `resource_types = 'Patient,Encounter,Observation,
-- MedicationRequest'` every entry is included (no filtering loss), so the
-- table holds 24 rows in total.

ASSERT ROW_COUNT = 4
ASSERT VALUE row_count = 4 WHERE resourcetype = 'Patient'
ASSERT VALUE row_count = 4 WHERE resourcetype = 'Encounter'
ASSERT VALUE row_count = 11 WHERE resourcetype = 'Observation'
ASSERT VALUE row_count = 5 WHERE resourcetype = 'MedicationRequest'
SELECT resourcetype, COUNT(*) AS row_count
FROM {{zone_name}}.fhir_bronze.bundle_resources
GROUP BY resourcetype
ORDER BY resourcetype;


-- ============================================================================
-- Q2: df_full_url is populated on every unbundled row
-- ============================================================================
-- This is the regression check for the Bundle-entry fullUrl preservation
-- fix. Every Bundle.entry in the source data has a fullUrl, so the count of
-- rows missing df_full_url must be zero, and the distinct fullUrl count
-- must equal the total row count (each fullUrl is unique).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 24
ASSERT VALUE rows_missing_full_url = 0
ASSERT VALUE distinct_full_urls = 24
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN df_full_url IS NULL THEN 1 ELSE 0 END) AS rows_missing_full_url,
    COUNT(DISTINCT df_full_url) AS distinct_full_urls
FROM {{zone_name}}.fhir_bronze.bundle_resources;


-- ============================================================================
-- Q3: bundle_id is populated on every row (include_bundle_metadata)
-- ============================================================================
-- Regression check for the include_bundle_metadata fix. Every row must carry
-- the source Bundle.id. We also assert bundle_type and bundle_timestamp are
-- populated — these are siblings on the same code path.

ASSERT ROW_COUNT = 1
ASSERT VALUE rows_missing_bundle_id = 0
ASSERT VALUE rows_missing_bundle_type = 0
ASSERT VALUE rows_missing_bundle_timestamp = 0
ASSERT VALUE rows_missing_bundle_total = 0
SELECT
    SUM(CASE WHEN bundle_id IS NULL THEN 1 ELSE 0 END) AS rows_missing_bundle_id,
    SUM(CASE WHEN bundle_type IS NULL THEN 1 ELSE 0 END) AS rows_missing_bundle_type,
    SUM(CASE WHEN bundle_timestamp IS NULL THEN 1 ELSE 0 END) AS rows_missing_bundle_timestamp,
    SUM(CASE WHEN bundle_total IS NULL THEN 1 ELSE 0 END) AS rows_missing_bundle_total
FROM {{zone_name}}.fhir_bronze.bundle_resources;


-- ============================================================================
-- Q4: Distinct bundle count matches file count (one Bundle per file)
-- ============================================================================
-- Each .json file contains exactly one Bundle, and the four bundle ids are
-- bundle-enc-1001..1004. The bundle_total reported by the source must match
-- our actually-emitted resource counts after the resource_types filter
-- (we do not exclude any of the four chosen types).

ASSERT ROW_COUNT = 4
ASSERT VALUE rows_in_bundle = 6 WHERE bundle_id = 'bundle-enc-1001'
ASSERT VALUE rows_in_bundle = 5 WHERE bundle_id = 'bundle-enc-1002'
ASSERT VALUE rows_in_bundle = 8 WHERE bundle_id = 'bundle-enc-1003'
ASSERT VALUE rows_in_bundle = 5 WHERE bundle_id = 'bundle-enc-1004'
ASSERT VALUE bundle_total_reported = '6' WHERE bundle_id = 'bundle-enc-1001'
ASSERT VALUE bundle_total_reported = '8' WHERE bundle_id = 'bundle-enc-1003'
SELECT
    bundle_id,
    bundle_type,
    MAX(bundle_total) AS bundle_total_reported,
    COUNT(*) AS rows_in_bundle
FROM {{zone_name}}.fhir_bronze.bundle_resources
GROUP BY bundle_id, bundle_type
ORDER BY bundle_id;


-- ============================================================================
-- Q5: LOINC code lookup — nested CodeableConcept extraction
-- ============================================================================
-- Three glucose readings (LOINC 2339-0) across three different bundles.
-- Proves that materialized_paths can reach into Observation.code.coding[*].code
-- and surface it as a flat column (`code_coding_code`).

ASSERT ROW_COUNT = 3
ASSERT VALUE code_coding_display = 'Glucose' WHERE id = 'obs-1001-glu'
ASSERT VALUE valuequantity_value = '165' WHERE id = 'obs-1001-glu'
ASSERT VALUE valuequantity_value = '110' WHERE id = 'obs-1003-glu'
ASSERT VALUE valuequantity_value = '96' WHERE id = 'obs-1004-glu'
SELECT
    id,
    bundle_id,
    code_coding_code,
    code_coding_display,
    valuequantity_value,
    valuequantity_unit,
    effectivedatetime
FROM {{zone_name}}.fhir_bronze.bundle_resources
WHERE resourcetype = 'Observation'
  AND code_coding_code = '2339-0'
ORDER BY id;


-- ============================================================================
-- Q6: Cross-resource join — Observation.subject -> Patient.df_full_url
-- ============================================================================
-- This is the use case df_full_url exists for. Every Observation references
-- its Patient via subject.reference, and that reference value is exactly
-- the Patient's Bundle.entry.fullUrl. With the fullUrl preservation fix,
-- this join resolves every observation back to its patient row inside the
-- same Bundle — no master patient index required.

ASSERT ROW_COUNT = 11
ASSERT VALUE patient_id = 'pat-1001' WHERE observation_id = 'obs-1001-glu'
ASSERT VALUE patient_id = 'pat-1003' WHERE observation_id = 'obs-1003-creat'
ASSERT VALUE patient_id = 'pat-1002' WHERE observation_id = 'obs-1002-temp'
ASSERT VALUE patient_gender = 'female' WHERE observation_id = 'obs-1003-bp'
SELECT
    obs.id AS observation_id,
    obs.code_coding_display AS observation_label,
    pat.id AS patient_id,
    pat.gender AS patient_gender,
    obs.bundle_id
FROM {{zone_name}}.fhir_bronze.bundle_resources obs
JOIN {{zone_name}}.fhir_bronze.bundle_resources pat
  ON obs.subject_reference = pat.df_full_url
 AND pat.resourcetype = 'Patient'
WHERE obs.resourcetype = 'Observation'
ORDER BY obs.id;


-- ============================================================================
-- Q7: Silver promotion table sanity
-- ============================================================================
-- The silver table was populated by setup.sql using the same in-Bundle
-- reference resolution. It must contain one row per Observation (11 rows)
-- with all required clinical fields populated.

ASSERT ROW_COUNT = 11
ASSERT VALUE patient_id = 'pat-1001' WHERE loinc_code = '8867-4' AND bundle_id = 'bundle-enc-1001'
ASSERT VALUE observation_value = 39.1 WHERE patient_id = 'pat-1002' AND loinc_code = '8310-5'
ASSERT VALUE observation_unit = 'mg/dL' WHERE patient_id = 'pat-1004' AND loinc_code = '2339-0'
SELECT
    bundle_id,
    patient_id,
    patient_gender,
    loinc_code,
    loinc_display,
    observation_value,
    observation_unit,
    effective_at
FROM clinical_silver.fhir_silver.encounter_observations
ORDER BY patient_id, loinc_code;


-- ============================================================================
-- VERIFY: Cross-cutting summary
-- ============================================================================
-- One PASS row per invariant proven above. Anchored on independently-derived
-- ground truth (see verify.py).

ASSERT ROW_COUNT = 8
ASSERT VALUE result = 'PASS' WHERE check_name = 'unbundle_total_rows_24'
ASSERT VALUE result = 'PASS' WHERE check_name = 'df_full_url_on_every_row'
ASSERT VALUE result = 'PASS' WHERE check_name = 'bundle_id_on_every_row'
ASSERT VALUE result = 'PASS' WHERE check_name = 'four_distinct_bundles'
ASSERT VALUE result = 'PASS' WHERE check_name = 'resource_types_filter_kept_all_four'
ASSERT VALUE result = 'PASS' WHERE check_name = 'glucose_observations_3'
ASSERT VALUE result = 'PASS' WHERE check_name = 'all_observations_link_to_patient'
ASSERT VALUE result = 'PASS' WHERE check_name = 'silver_row_count_11'
SELECT check_name, result FROM (

    SELECT 'unbundle_total_rows_24' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_bronze.bundle_resources) = 24
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'df_full_url_on_every_row' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_bronze.bundle_resources WHERE df_full_url IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'bundle_id_on_every_row' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_bronze.bundle_resources WHERE bundle_id IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'four_distinct_bundles' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT bundle_id) FROM {{zone_name}}.fhir_bronze.bundle_resources) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'resource_types_filter_kept_all_four' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT resourcetype) FROM {{zone_name}}.fhir_bronze.bundle_resources) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'glucose_observations_3' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_bronze.bundle_resources
                       WHERE resourcetype = 'Observation' AND code_coding_code = '2339-0') = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'all_observations_link_to_patient' AS check_name,
           CASE WHEN (
               SELECT COUNT(*)
               FROM {{zone_name}}.fhir_bronze.bundle_resources obs
               JOIN {{zone_name}}.fhir_bronze.bundle_resources pat
                 ON obs.subject_reference = pat.df_full_url
                AND pat.resourcetype = 'Patient'
               WHERE obs.resourcetype = 'Observation'
           ) = 11 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'silver_row_count_11' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM clinical_silver.fhir_silver.encounter_observations) = 11
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
