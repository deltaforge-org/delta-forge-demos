-- ============================================================================
-- Iceberg V3 Clinical Trial Lab Results — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg v3 table.
-- DeltaForge reads the Iceberg metadata chain directly:
-- metadata.json → manifest list → manifests → Parquet data files.
--
-- Iceberg v3 is the latest format specification, generated here by
-- Apache Spark 4.0 with Iceberg 1.10.1. V3 adds support for new types
-- (variant, geometry), default column values, and enhanced metadata.
--
-- Dataset: 480 clinical trial lab results across 3 trial sites
-- (Boston-MGH, Houston-MD, Seattle-UW) and 4 lab tests (Hemoglobin,
-- Creatinine, ALT, Platelet-Count) with 12 columns: sample_id,
-- trial_name, site, patient_id, test_name, result_value, unit,
-- reference_low, reference_high, is_abnormal, collection_date, analyst.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v3 table
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- DeltaForge parses metadata.json to discover schema and data files automatically.
-- The format-version field in metadata.json is 3 (latest Iceberg spec).
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.lab_results
USING ICEBERG
LOCATION 'iceberg-v3-clinical-trials/iceberg_warehouse/trials/lab_results';

