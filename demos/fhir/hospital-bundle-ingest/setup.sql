-- ============================================================================
-- Hospital Interop — Daily Clinical Bundle Ingest (FHIR provider)
-- ============================================================================
-- Scenario:
--   St Michael's Hospital's interoperability gateway emits one FHIR R4
--   transaction Bundle per patient encounter (admit/discharge/transfer plus
--   observations and medications). The clinical-quality team lands these in
--   bronze, then promotes a focused encounter-observation slice into silver
--   for downstream dashboards (length of stay, abnormal vitals, etc.).
--
-- What this demo proves about the dedicated `USING fhir` provider:
--   - `unbundle = 'true'` explodes each Bundle into one row per resource.
--   - Every row carries `df_full_url` — the canonical Bundle.entry.fullUrl,
--     which is required to resolve in-Bundle references like
--     `Observation.subject = "<base>/Patient/<id>"`.
--   - `include_bundle_metadata = 'true'` adds `bundle_id`, `bundle_type`,
--     `bundle_timestamp`, `bundle_total` to every row — the audit trail of
--     "which transmission contained this resource".
--   - `resource_types` filters the landing to just the four clinical
--     resource types we care about for this bronze.
--   - `materialized_paths` projects the nested FHIR paths we need
--     (subject reference, LOINC code, value, etc.) using the configured
--     `path_separator`. All names are lowercased to match DataFusion's
--     unquoted-identifier rules.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Zone & Schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — file-backed bronze landing for FHIR Bundles';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.fhir_bronze
    COMMENT 'Hospital interop bronze — one row per FHIR resource per Bundle';

CREATE ZONE IF NOT EXISTS clinical_silver TYPE DELTA
    COMMENT 'Delta tables — clinical silver layer';

CREATE SCHEMA IF NOT EXISTS clinical_silver.fhir_silver
    COMMENT 'Resource-typed silver tables built from the bronze unbundled view';

-- ----------------------------------------------------------------------------
-- BRONZE: bundle_resources — every Bundle.entry as one row
-- ----------------------------------------------------------------------------
-- Every row produced has the fixed FHIR-provider columns:
--   resourcetype, id, df_resource_json, df_resource_id,
--   df_full_url           (entry.fullUrl — REQUIRED for cross-resource refs)
--   bundle_id             (Bundle.id)
--   bundle_type           (Bundle.type — e.g. 'transaction')
--   bundle_timestamp      (Bundle.timestamp)
--   bundle_total          (Bundle.total — entry count claimed by the source)
--   df_file_name          (the source .json filename)
--   df_row_number
--
-- And the materialized projections (lowercased path with `_` separator):
--   status, gender, birthdate, active
--   subject_reference                (e.g., "<base>/Patient/pat-1001")
--   code_coding_code, code_coding_display, code_coding_system
--   effectivedatetime, valuequantity_value, valuequantity_unit
--   authoredon, intent
--   medicationcodeableconcept_coding_code,
--   medicationcodeableconcept_coding_display
--   class_code, class_display, period_start, period_end
-- ----------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_bronze.bundle_resources
USING fhir
LOCATION '{{data_path}}'
OPTIONS (
    unbundle = 'true',
    include_bundle_metadata = 'true',
    resource_types = 'Patient,Encounter,Observation,MedicationRequest',
    path_separator = '_',
    fhir_version = 'r4',
    materialized_paths = 'status,gender,birthDate,active,subject_reference,code_coding_code,code_coding_display,code_coding_system,effectiveDateTime,valueQuantity_value,valueQuantity_unit,authoredOn,intent,medicationCodeableConcept_coding_code,medicationCodeableConcept_coding_display,class_code,class_display,period_start,period_end',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.fhir_bronze.bundle_resources;

-- ----------------------------------------------------------------------------
-- SILVER: encounter_observations — observations joined to their patient row
-- ----------------------------------------------------------------------------
-- This is the canonical bronze->silver promotion the clinical team uses for
-- dashboards. It exercises the new `df_full_url` column to resolve in-Bundle
-- references (Observation.subject.reference -> Patient.df_full_url) without
-- needing any external master patient index.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS clinical_silver.fhir_silver.encounter_observations (
    bundle_id STRING,
    bundle_timestamp STRING,
    observation_full_url STRING,
    patient_full_url STRING,
    patient_id STRING,
    patient_gender STRING,
    loinc_code STRING,
    loinc_display STRING,
    observation_value DOUBLE,
    observation_unit STRING,
    effective_at STRING,
    source_file STRING
) LOCATION 'hospital-bundle-ingest/_silver_encounter_observations';

INSERT INTO clinical_silver.fhir_silver.encounter_observations
SELECT
    obs.bundle_id,
    obs.bundle_timestamp,
    obs.df_full_url AS observation_full_url,
    pat.df_full_url AS patient_full_url,
    pat.id AS patient_id,
    pat.gender AS patient_gender,
    obs.code_coding_code AS loinc_code,
    obs.code_coding_display AS loinc_display,
    CAST(obs.valuequantity_value AS DOUBLE) AS observation_value,
    obs.valuequantity_unit AS observation_unit,
    obs.effectivedatetime AS effective_at,
    obs.df_file_name AS source_file
FROM {{zone_name}}.fhir_bronze.bundle_resources obs
JOIN {{zone_name}}.fhir_bronze.bundle_resources pat
  ON obs.subject_reference = pat.df_full_url
 AND pat.resourcetype = 'Patient'
WHERE obs.resourcetype = 'Observation';

DETECT SCHEMA FOR TABLE clinical_silver.fhir_silver.encounter_observations;
