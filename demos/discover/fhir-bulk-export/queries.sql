-- ============================================================================
-- Demo: FHIR Bulk-Export Auto-Onboarding - Queries
-- ============================================================================
-- The export folder holds a single FHIR Bulk Data ($export) artifact:
--   Patient.ndjson - 20 FHIR R4 Patient resources, one resource per line.
-- DISCOVER reads the bytes, classifies the newline-delimited JSON shape, and
-- registers ONE external table over it. Every assertion value below was
-- precomputed from the data file (gen_data.py), not the engine.
--
-- Column naming: DISCOVER sanitizes each top-level scalar key to a snake_case
-- column. So resourceType -> resource_type, birthDate -> birth_date, and the
-- already-flat keys id / gender / active keep their names. The nested name[]
-- array lands as a JSON string column (name), which is expected and fine.
-- ============================================================================

-- ============================================================================
-- Query 1: DISCOVER in PRINT mode - review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the exported
-- file, classifies the text shape (one JSON object per line = NDJSON, which
-- registers USING JSON), and synthesizes the flatten config from the
-- top-level keys. FILE_METADATA adds the df_file_name / df_row_number
-- provenance columns to the generated table.

DISCOVER {{zone_name}}.discover_demos.patient_export
    PATH 'fhir-bulk-export'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode - auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). This
-- single statement replaces a hand-written CREATE EXTERNAL TABLE plus a
-- json_flatten_config block. The cohort is queryable immediately afterward.

DISCOVER {{zone_name}}.discover_demos.patient_export
    PATH 'fhir-bulk-export'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: The patient cohort - all 20 Patient resources
-- ============================================================================
-- One row per FHIR Patient resource from the NDJSON bulk export. The
-- discovered schema is the union of the top-level scalar keys, plus the
-- nested name[] array as a JSON string.

ASSERT ROW_COUNT = 20
ASSERT VALUE gender = male WHERE id = 'pat-001'
ASSERT VALUE gender = female WHERE id = 'pat-020'
ASSERT VALUE resource_type = Patient WHERE id = 'pat-001'
SELECT
    id,
    resource_type,
    gender,
    birth_date,
    df_file_name
FROM {{zone_name}}.discover_demos.patient_export
ORDER BY id;

-- ============================================================================
-- Query 4: Resource-type homogeneity - every row is a Patient
-- ============================================================================
-- A FHIR Patient bulk-export file contains only Patient resources. Grouping on
-- the discovered resource_type column proves the body of every record decoded
-- correctly: exactly one group, exactly 20 rows.

ASSERT ROW_COUNT = 1
ASSERT VALUE resource_count = 20 WHERE resource_type = 'Patient'
SELECT
    resource_type,
    COUNT(*) AS resource_count
FROM {{zone_name}}.discover_demos.patient_export
GROUP BY resource_type;

-- ============================================================================
-- Query 5: Gender distribution - FHIR Patient.gender ValueSet
-- ============================================================================
-- FHIR Patient.gender uses the required ValueSet male | female | other |
-- unknown. This cohort uses three of those values with exact, precomputed
-- counts.

ASSERT ROW_COUNT = 3
ASSERT VALUE patient_count = 7 WHERE gender = 'male'
ASSERT VALUE patient_count = 7 WHERE gender = 'female'
ASSERT VALUE patient_count = 6 WHERE gender = 'other'
SELECT
    gender,
    COUNT(*) AS patient_count
FROM {{zone_name}}.discover_demos.patient_export
GROUP BY gender
ORDER BY patient_count DESC, gender;

-- ============================================================================
-- Query 6: Active flag - how many patient records are active?
-- ============================================================================
-- FHIR Patient.active is a boolean. Grouping on it proves the discovered
-- active column decoded as a usable value across all 20 rows: exactly two
-- groups, 10 active and 10 inactive.

ASSERT ROW_COUNT = 2
ASSERT VALUE patient_count = 10 WHERE active_label = 'active'
ASSERT VALUE patient_count = 10 WHERE active_label = 'inactive'
SELECT
    CASE WHEN active THEN 'active' ELSE 'inactive' END AS active_label,
    COUNT(*) AS patient_count
FROM {{zone_name}}.discover_demos.patient_export
GROUP BY 1
ORDER BY active_label;

-- ============================================================================
-- Query 7: Provenance - every row traces to the single export file
-- ============================================================================
-- df_file_name comes from the FILE_METADATA = true discovery option. The
-- entire cohort was exported as one NDJSON artifact, so there is exactly one
-- distinct source file and df_row_number runs 1..20 across the lines.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_patients = 20
ASSERT VALUE first_row = 1
ASSERT VALUE last_row = 20
SELECT
    df_file_name,
    MIN(df_row_number) AS first_row,
    MAX(df_row_number) AS last_row,
    COUNT(*)           AS total_patients
FROM {{zone_name}}.discover_demos.patient_export
GROUP BY df_file_name;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total cohort volume, the
-- single-resource-type guarantee, the gender split, and single-file
-- provenance from one DISCOVER-registered table.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_patients = 20
ASSERT VALUE patient_resources = 20
ASSERT VALUE male_patients = 7
ASSERT VALUE distinct_source_files = 1
SELECT
    COUNT(*)                                              AS total_patients,
    COUNT(*) FILTER (WHERE resource_type = 'Patient')     AS patient_resources,
    COUNT(*) FILTER (WHERE gender = 'male')               AS male_patients,
    COUNT(DISTINCT df_file_name)                          AS distinct_source_files
FROM {{zone_name}}.discover_demos.patient_export;
