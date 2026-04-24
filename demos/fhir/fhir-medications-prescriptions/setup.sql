-- ============================================================================
-- FHIR Medications & Prescriptions — Setup Script
-- ============================================================================
-- Creates two external tables from FHIR R5 resources:
--   1. prescriptions — 12 MedicationRequest resources (prescriptions/orders)
--   2. coverage       — 4 Coverage resources (insurance plans)
--
-- Demonstrates:
--   - FHIR contained resources (Medication embedded inside MedicationRequest)
--   - Deep nesting: dosageInstruction[].timing, dispenseRequest.quantity
--   - json_paths for preserving complex subtrees (dosageInstruction, contained)
--   - FHIR CodeableConcept pattern (code.coding[].system/code/display)
--   - Coverage class arrays (group, plan, rxid, rxbin, rxgroup)
--   - Schema evolution across different prescription and coverage types
--   - column_mappings for FHIR-specific paths
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.fhir_demos
    COMMENT 'FHIR-backed external tables — HL7 FHIR R5 resources as JSON';

-- ============================================================================
-- TABLE 1: prescriptions — 12 MedicationRequest resources
-- ============================================================================
-- FHIR MedicationRequest represents a prescription or medication order.
-- These resources demonstrate complex FHIR patterns:
--   - Contained resources: the Medication is embedded inside the request
--   - Dosage instructions with timing, route, dose ranges
--   - Dispense requests with validity period, quantity, supply duration
--   - Substitution rules with allowed/reason
--   - Subject references linking to Patient resources
--
-- The dosageInstruction and contained arrays are kept as JSON blobs via
-- json_paths since they have deeply variable structure across prescriptions.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_demos.prescriptions
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'medicationrequest*.json',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.resourceType",
            "$.id",
            "$.status",
            "$.intent",
            "$.category",
            "$.medication",
            "$.subject",
            "$.encounter",
            "$.authoredOn",
            "$.requester",
            "$.reason",
            "$.dosageInstruction",
            "$.dispenseRequest",
            "$.substitution",
            "$.contained",
            "$.note",
            "$.insurance"
        ],
        "json_paths": ["$.dosageInstruction", "$.contained", "$.dispenseRequest", "$.medication", "$.subject", "$.encounter", "$.requester", "$.substitution"],
        "column_mappings": {
            "$.id": "prescription_id",
            "$.authoredOn": "authored_date",
            "$.dosageInstruction": "dosage_instructions",
            "$.dispenseRequest": "dispense_request"
        },
        "max_depth": 4,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: coverage — 4 Coverage resources (insurance plans)
-- ============================================================================
-- FHIR Coverage represents a patient's insurance or payment arrangement.
-- These resources show:
--   - Coverage classes: group, subgroup, plan, rxid, rxbin, rxgroup, rxpcn
--   - Beneficiary and subscriber references to Patient resources
--   - Coverage periods with start/end dates
--   - Different coverage types: insurance, EHIC, self-pay
--
-- The class array is preserved as JSON for downstream processing.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_demos.coverage
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'coverage-example*.json',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.resourceType",
            "$.id",
            "$.status",
            "$.kind",
            "$.type",
            "$.subscriber",
            "$.beneficiary",
            "$.dependent",
            "$.period",
            "$.class",
            "$.identifier",
            "$.insurer",
            "$.policyHolder"
        ],
        "json_paths": ["$.class", "$.type", "$.subscriber", "$.beneficiary", "$.period", "$.insurer", "$.policyHolder", "$.identifier"],
        "column_mappings": {
            "$.id": "coverage_id"
        },
        "max_depth": 3,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
