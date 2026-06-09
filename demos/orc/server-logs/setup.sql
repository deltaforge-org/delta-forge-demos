-- ============================================================================
-- ORC Server Logs — Setup Script
-- ============================================================================
-- Creates two external tables from 5 server access log ORC files:
--   1. all_requests   — All 5 files with schema evolution (2,500 rows)
--   2. api01_only     — Single server via LOCATION glob (500 rows)
--
-- Demonstrates:
--   - Multi-file reading: 5 ORC files in one table
--   - Schema evolution: v1 (11 fields) → v2 (13 fields, adds
--     request_body_bytes, cache_hit); NULL filling for web servers
--   - Self-describing schema: ORC file footers provide types automatically
--   - LOCATION glob: wildcard pattern to select files by name
--   - file_metadata: df_file_name + df_row_number system columns
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.orc_demos
    COMMENT 'ORC-backed external tables';

-- ============================================================================
-- TABLE 1: all_requests — All 5 files with schema evolution
-- ============================================================================
-- Reads all ORC files from the directory. Files use two schema versions:
--   v1 (web-01, web-02, web-03): 11 fields (basic access log)
--   v2 (api-01, api-02): 13 fields (adds request_body_bytes, cache_hit)
-- The union schema merges both versions; v1 rows get NULL for new columns.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc_demos.all_requests
USING ORC
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: api01_only — Single server via LOCATION glob (500 rows)
-- ============================================================================
-- Uses a wildcard in LOCATION to read only api-01_access.orc, which uses
-- schema v2 (includes request_body_bytes and cache_hit).
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc_demos.api01_only
USING ORC
LOCATION 'server-logs/api-01*.orc'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
