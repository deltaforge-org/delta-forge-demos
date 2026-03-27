-- ============================================================================
-- Veterinary Clinic Patient Records — Setup Script
-- ============================================================================
-- Multi-branch veterinary clinic with recursive directory scanning,
-- file_filter, and file_metadata for CSV format.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.csv_vet
    COMMENT 'Veterinary clinic CSV-backed external tables';

-- ============================================================================
-- TABLE 1: all_visits — Recursive scan of all 3 branches
-- ============================================================================
-- Reads visits.csv from branch-north, branch-south, and branch-east
-- using recursive scanning. Includes file metadata columns.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_vet.all_visits
USING CSV
LOCATION '{{data_path}}'
OPTIONS (
    header = 'true',
    recursive = 'true',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
GRANT ADMIN ON TABLE {{zone_name}}.csv_vet.all_visits TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.csv_vet.all_visits;

-- ============================================================================
-- TABLE 2: north_only — File filter for branch-north
-- ============================================================================
-- Uses file_filter glob to read only the north branch data.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_vet.north_only
USING CSV
LOCATION '{{data_path}}'
OPTIONS (
    header = 'true',
    recursive = 'true',
    file_filter = '*north*'
);
GRANT ADMIN ON TABLE {{zone_name}}.csv_vet.north_only TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.csv_vet.north_only;

-- ============================================================================
-- TABLE 3: sampled_visits — Max rows = 10 per file
-- ============================================================================
-- Recursive scan limited to 10 rows per file (30 total from 3 files).
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_vet.sampled_visits
USING CSV
LOCATION '{{data_path}}'
OPTIONS (
    header = 'true',
    recursive = 'true',
    max_rows = '10'
);
GRANT ADMIN ON TABLE {{zone_name}}.csv_vet.sampled_visits TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.csv_vet.sampled_visits;
