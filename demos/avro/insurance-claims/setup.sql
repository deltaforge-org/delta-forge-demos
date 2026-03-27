-- ============================================================================
-- Avro Insurance Claims — Setup Script
-- ============================================================================
-- Creates three external tables from 3 Avro files (90 rows):
--   1. all_claims       — All 3 files, schema evolution (v1 + v2)
--   2. auto_claims_only — Auto claims only via file_filter (60 rows)
--   3. sampled_claims   — Sampled subset via max_rows (15 per file)
--
-- Demonstrates:
--   - Schema evolution: v1 (10 fields) → v2 adds adjuster_name, settlement_date
--   - NULL filling: v1 rows get NULLs for v2-only columns
--   - Mixed compression codecs: null and deflate
--   - file_filter: isolate auto claim files
--   - max_rows: limit rows per file for profiling
--   - file_metadata: df_file_name + df_row_number system columns
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.avro_insurance
    COMMENT 'Insurance claims Avro-backed external tables';

-- ============================================================================
-- TABLE 1: all_claims — All 3 files with schema evolution (90 rows)
-- ============================================================================
-- Reads all Avro files. Schema v2 columns (adjuster_name, settlement_date)
-- are NULL-filled for rows from v1 files.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.avro_insurance.all_claims
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.avro_insurance.all_claims;
GRANT ADMIN ON TABLE {{zone_name}}.avro_insurance.all_claims TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: auto_claims_only — Auto claims via file_filter (60 rows)
-- ============================================================================
-- Uses file_filter to read only files matching '*auto*', capturing both
-- the v1 and v2 auto claim files (30 + 30 = 60 rows).
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.avro_insurance.auto_claims_only
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '*auto*',
    file_metadata = '{"columns":["df_file_name"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.avro_insurance.auto_claims_only;
GRANT ADMIN ON TABLE {{zone_name}}.avro_insurance.auto_claims_only TO USER {{current_user}};


-- ============================================================================
-- TABLE 3: sampled_claims — Data profiling via max_rows (15 per file)
-- ============================================================================
-- Limits to 15 rows per file for quick data profiling. With 3 files,
-- produces 45 rows — enough to inspect data quality without reading all 90.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.avro_insurance.sampled_claims
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    max_rows = '15',
    file_metadata = '{"columns":["df_file_name"]}'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.avro_insurance.sampled_claims;
GRANT ADMIN ON TABLE {{zone_name}}.avro_insurance.sampled_claims TO USER {{current_user}};
