-- ============================================================================
-- Veterinary Clinic Patient Records — Setup Script
-- ============================================================================
-- Multi-branch veterinary clinic with recursive directory scanning,
-- file_filter with wildcard patterns, and max_rows for CSV format.
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
-- using recursive scanning.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_vet.all_visits
USING CSV
LOCATION '{{data_path}}'
OPTIONS (
    header = 'true',
    recursive = 'true'
);

-- ============================================================================
-- TABLE 2: north_only — File filter with wildcard for branch-north
-- ============================================================================
-- Uses recursive scan + file_filter wildcard to read only the north branch data.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_vet.north_only
USING CSV
LOCATION '{{data_path}}'
OPTIONS (
    header = 'true',
    recursive = 'true',
    file_filter = '*north*'
);

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
