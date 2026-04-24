-- ============================================================================
-- FHIR Patient Demographics — Setup Script
-- ============================================================================
-- Creates two external tables from FHIR R5 Patient resources:
--   1. patients_bulk     — 50 patients from NDJSON bulk export (flat)
--   2. patients_detailed — 7 individual Patient JSON files (rich, nested)
--
-- Demonstrates:
--   - NDJSON (newline-delimited JSON) bulk format ingestion
--   - FHIR resource flattening with include_paths and column_mappings
--   - Nested array handling (name[], telecom[], address[])
--   - Schema evolution across files with varying completeness
--   - Multi-file reading from a single directory
--   - file_metadata for source file tracking
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.fhir_demos
    COMMENT 'FHIR-backed external tables — HL7 FHIR R5 resources as JSON';

-- ============================================================================
-- TABLE 1: patients_bulk — 50 patients from NDJSON bulk export
-- ============================================================================
-- FHIR Bulk Data exports use NDJSON format: one complete Patient resource per
-- line. These records have a consistent flat structure with id, name (family +
-- given), gender, and birthDate. Column mappings rename nested FHIR paths to
-- analyst-friendly names.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_demos.patients_bulk
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '*.ndjson',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.resourceType",
            "$.id",
            "$.name",
            "$.gender",
            "$.birthDate"
        ],
        "column_mappings": {
            "$.id": "patient_id",
            "$.birthDate": "birth_date"
        },
        "max_depth": 3,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: patients_detailed — 7 individual FHIR Patient JSON files
-- ============================================================================
-- Each file is a standalone FHIR Patient resource with varying levels of
-- detail: identifiers, multiple name variants, telecom contacts, addresses,
-- managing organizations, marital status, and contact persons. This table
-- demonstrates schema evolution — some files include fields that others omit,
-- resulting in NULL values for missing fields (e.g., deceasedBoolean only
-- appears in some patients).
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_demos.patients_detailed
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'patient-example*.json',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.resourceType",
            "$.id",
            "$.active",
            "$.gender",
            "$.birthDate",
            "$.deceasedBoolean",
            "$.name",
            "$.telecom",
            "$.address",
            "$.maritalStatus",
            "$.managingOrganization"
        ],
        "json_paths": ["$.maritalStatus", "$.managingOrganization"],
        "column_mappings": {
            "$.id": "patient_id",
            "$.birthDate": "birth_date",
            "$.deceasedBoolean": "is_deceased",
            "$.managingOrganization": "managing_org"
        },
        "max_depth": 3,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
