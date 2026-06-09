-- ============================================================================
-- Iceberg V3 Equality Delete Files — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg V3 table that
-- includes equality delete files. DeltaForge reads the Iceberg metadata
-- chain directly: metadata.json -> manifest list -> manifests -> Parquet
-- data files + equality delete files.
--
-- Equality deletes are column-value-based: a delete file contains the
-- patient_id values to erase. Any row in the data file whose patient_id
-- matches a value in the delete file is excluded from query results. This
-- differs from position deletes, which reference specific row positions.
--
-- Dataset: 500 patient visit records across 5 hospitals (Cleveland Clinic,
-- Johns Hopkins, Mass General, Mayo Clinic, Mount Sinai). 4 patients
-- exercised GDPR right to erasure — equality deletes by patient_id remove
-- all 55 records for those patients, leaving 445 visible visits.
--
-- Columns: visit_id, patient_id, hospital, department, attending_physician,
-- diagnosis_code, treatment_cost, is_emergency, visit_date.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg V3 table with equality deletes
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- DeltaForge parses metadata.json to discover schema, data files, and equality
-- delete files automatically. The format-version field in metadata.json is 3.
-- The delete file uses equality semantics on the patient_id column.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.patient_visits
USING ICEBERG
LOCATION 'iceberg-v3-equality-deletes/patient_visits';

