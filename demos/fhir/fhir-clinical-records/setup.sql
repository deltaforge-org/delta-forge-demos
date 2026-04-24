-- ============================================================================
-- FHIR Clinical Records — Setup Script
-- ============================================================================
-- Creates three external tables from FHIR R5 clinical resources:
--   1. conditions  — 8 Condition resources (diagnoses, findings)
--   2. procedures  — 8 Procedure resources (surgeries, interventions)
--   3. allergies   — 6 AllergyIntolerance resources (allergies, NKA assertions)
--
-- Demonstrates:
--   - Multi-resource-type ingestion from a single directory
--   - file_filter to separate resource types by filename pattern
--   - Deep nested arrays: reaction[] with manifestation, severity, onset
--   - CodeableConcept hierarchies: SNOMED CT, ICD-10, LOINC coding systems
--   - FHIR clinical status codes (active, resolved, confirmed, refuted)
--   - Cross-resource references (Condition → Patient, Procedure → Practitioner)
--   - json_paths for preserving complex clinical detail (reaction, performer)
--   - Schema evolution across clinically related but structurally different
--     resource instances
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.fhir_demos
    COMMENT 'FHIR-backed external tables — HL7 FHIR R5 resources as JSON';

-- ============================================================================
-- TABLE 1: conditions — 8 Condition (diagnosis) resources
-- ============================================================================
-- FHIR Condition represents a clinical diagnosis, problem, or finding.
-- Resources include:
--   - Burn of ear (severe, active)
--   - Heart valve disorder (resolved with abatement date)
--   - NSCLC lung cancer (active with staging)
--   - Fever, Sepsis, Renal insufficiency (various severities)
--   - Malignant neoplasm (with body site specification)
--   - Stroke (with onset date)
--
-- Key FHIR patterns: clinicalStatus, verificationStatus, severity,
-- bodySite[], category[], onset/abatement dates, evidence, stage.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_demos.conditions
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'condition-example*.json',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.resourceType",
            "$.id",
            "$.clinicalStatus",
            "$.verificationStatus",
            "$.category",
            "$.severity",
            "$.code",
            "$.bodySite",
            "$.subject",
            "$.onsetDateTime",
            "$.onsetAge",
            "$.abatementDateTime",
            "$.abatementAge",
            "$.recordedDate",
            "$.evidence",
            "$.stage",
            "$.note"
        ],
        "json_paths": ["$.clinicalStatus", "$.verificationStatus", "$.severity", "$.code", "$.subject", "$.onsetAge", "$.abatementAge"],
        "column_mappings": {
            "$.id": "condition_id",
            "$.clinicalStatus": "clinical_status",
            "$.verificationStatus": "verification_status",
            "$.bodySite": "body_site",
            "$.onsetDateTime": "onset_date",
            "$.abatementDateTime": "abatement_date",
            "$.recordedDate": "recorded_date"
        },
        "max_depth": 4,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: procedures — 8 Procedure resources
-- ============================================================================
-- FHIR Procedure represents a clinical intervention or surgery. Resources
-- include:
--   - Appendectomy (with performer, reason, and follow-up)
--   - Biopsy (with body site and specimen)
--   - Colonoscopy (with complication)
--   - Heart valve replacement, Lobectomy, Abscess I&D, Tracheotomy
--   - Device implant (with focal device reference)
--
-- Key FHIR patterns: status, code (SNOMED CT procedure codes), performer[],
-- reason[], bodySite, outcome, followUp[], note[].
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_demos.procedures
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'procedure-example*.json',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.resourceType",
            "$.id",
            "$.status",
            "$.code",
            "$.subject",
            "$.occurrenceDateTime",
            "$.occurrencePeriod",
            "$.performer",
            "$.reason",
            "$.bodySite",
            "$.outcome",
            "$.followUp",
            "$.note",
            "$.recorder",
            "$.reportedReference",
            "$.complication",
            "$.focalDevice"
        ],
        "json_paths": ["$.performer", "$.reason", "$.followUp", "$.code", "$.subject", "$.occurrencePeriod", "$.outcome", "$.recorder", "$.reportedReference"],
        "column_mappings": {
            "$.id": "procedure_id",
            "$.occurrenceDateTime": "occurrence_date",
            "$.occurrencePeriod": "occurrence_period",
            "$.bodySite": "body_site",
            "$.reportedReference": "reported_by",
            "$.focalDevice": "focal_device"
        },
        "max_depth": 4,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 3: allergies — 6 AllergyIntolerance resources
-- ============================================================================
-- FHIR AllergyIntolerance records allergies and adverse reactions. Resources
-- include:
--   - Cashew nut allergy (high criticality, anaphylactic reaction history)
--   - Fish allergy (food category)
--   - Medication allergy (drug category)
--   - NKA, NKDA, NKLA (No Known Allergies/Drug Allergies/Latex Allergies)
--
-- Key FHIR patterns: clinicalStatus, type (allergy vs intolerance),
-- category (food, medication, environment, biologic), criticality,
-- reaction[] with manifestation, severity, onset, exposureRoute.
--
-- The reaction[] array is preserved as JSON via json_paths since it contains
-- deeply nested manifestation/substance/exposure data.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_demos.allergies
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'allergyintolerance*.json',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.resourceType",
            "$.id",
            "$.clinicalStatus",
            "$.verificationStatus",
            "$.type",
            "$.category",
            "$.criticality",
            "$.code",
            "$.patient",
            "$.onsetDateTime",
            "$.recordedDate",
            "$.lastOccurrence",
            "$.reaction",
            "$.note",
            "$.participant"
        ],
        "json_paths": ["$.reaction", "$.participant", "$.clinicalStatus", "$.verificationStatus", "$.type", "$.code", "$.patient"],
        "column_mappings": {
            "$.id": "allergy_id",
            "$.clinicalStatus": "clinical_status",
            "$.verificationStatus": "verification_status",
            "$.onsetDateTime": "onset_date",
            "$.recordedDate": "recorded_date",
            "$.lastOccurrence": "last_occurrence"
        },
        "max_depth": 4,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
