-- ============================================================================
-- Veterinary Clinic Patient Records — Setup Script
-- ============================================================================
-- Multi-branch veterinary clinic with multi-file CSV scanning,
-- file_filter, and max_rows for CSV format.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.csv_vet
    COMMENT 'Veterinary clinic CSV-backed external tables';

-- ============================================================================
-- TABLE 1: all_visits — All 3 branch files
-- ============================================================================
-- Reads north_visits.csv, south_visits.csv, east_visits.csv from the data path.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_vet.all_visits
USING CSV
LOCATION '{{data_path}}'
OPTIONS (
    header = 'true'
);
GRANT ADMIN ON TABLE {{zone_name}}.csv_vet.all_visits TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.csv_vet.all_visits;

-- ============================================================================
-- TABLE 2: north_only — Single file for north branch
-- ============================================================================
-- Points directly at the north branch CSV file.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_vet.north_only
USING CSV
LOCATION '{{data_path}}/north_visits.csv'
OPTIONS (
    header = 'true'
);
GRANT ADMIN ON TABLE {{zone_name}}.csv_vet.north_only TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.csv_vet.north_only;

-- ============================================================================
-- TABLE 3: sampled_visits — Max rows = 10 per file
-- ============================================================================
-- All 3 files limited to 10 rows each (30 total).
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_vet.sampled_visits
USING CSV
LOCATION '{{data_path}}'
OPTIONS (
    header = 'true',
    max_rows = '10'
);
GRANT ADMIN ON TABLE {{zone_name}}.csv_vet.sampled_visits TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.csv_vet.sampled_visits;
