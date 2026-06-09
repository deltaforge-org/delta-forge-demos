-- ============================================================================
-- FHIR Clinical Observations — Setup Script
-- ============================================================================
-- Creates two external tables from FHIR R5 Observation resources:
--   1. observations_bulk — 100 heart rate observations from NDJSON bulk export
--   2. observations_clinical — 14 individual observations (vital signs + labs)
--
-- Demonstrates:
--   - FHIR Observation resource — the most common clinical data type
--   - Deep nested flattening: CodeableConcept.coding[].code, Quantity.value
--   - FHIR reference resolution: subject.reference → patient link
--   - Schema evolution across observation types (vital signs vs lab results)
--   - NDJSON vs individual JSON file handling
--   - column_mappings for FHIR-specific nested paths
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.fhir_demos
    COMMENT 'FHIR-backed external tables — HL7 FHIR R5 resources as JSON';

-- ============================================================================
-- TABLE 1: observations_bulk — 100 heart rate readings from NDJSON
-- ============================================================================
-- A FHIR Bulk Data export of 100 Observation resources, all recording heart
-- rate (LOINC 8867-4). Each observation links to a Patient via the subject
-- reference. The valueQuantity holds the actual measurement. This table
-- demonstrates high-volume clinical data ingestion from the standard FHIR
-- bulk export format.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_demos.observations_bulk
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '*.ndjson',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.resourceType",
            "$.id",
            "$.status",
            "$.code",
            "$.subject",
            "$.effectiveDateTime",
            "$.valueQuantity"
        ],
        "json_paths": ["$.code", "$.subject", "$.valueQuantity"],
        "column_mappings": {
            "$.id": "observation_id",
            "$.effectiveDateTime": "effective_date",
            "$.valueQuantity": "value_quantity"
        },
        "max_depth": 4,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: observations_clinical — 14 individual clinical observations
-- ============================================================================
-- A diverse set of FHIR Observation resources covering:
--   - Vital Signs: body weight, height, BMI, blood pressure, temperature,
--     respiratory rate, heart rate, SpO2
--   - Lab Results: glucose, CO2, hemoglobin, erythrocyte, base excess
--   - Clinical Assessments: Glasgow Coma Scale
--
-- Each observation type populates different fields (e.g., blood pressure uses
-- component[] instead of valueQuantity). This demonstrates how DeltaForge
-- handles schema evolution across different observation categories.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_demos.observations_clinical
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'observation-example*.json',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.resourceType",
            "$.id",
            "$.status",
            "$.code",
            "$.category",
            "$.subject",
            "$.effectiveDateTime",
            "$.effectivePeriod",
            "$.valueQuantity",
            "$.valueString",
            "$.interpretation",
            "$.referenceRange",
            "$.component"
        ],
        "json_paths": ["$.code", "$.subject", "$.effectivePeriod", "$.valueQuantity"],
        "column_mappings": {
            "$.id": "observation_id",
            "$.effectiveDateTime": "effective_date",
            "$.effectivePeriod": "effective_period",
            "$.valueQuantity": "value_quantity",
            "$.valueString": "value_string",
            "$.referenceRange": "reference_range"
        },
        "max_depth": 4,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
