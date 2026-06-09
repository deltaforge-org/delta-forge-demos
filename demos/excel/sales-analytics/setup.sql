-- ============================================================================
-- Excel Sales Analytics — Setup Script
-- ============================================================================
-- Creates two external tables from 4 Superstore sales XLSX files (2014–2017):
--   1. all_orders   — All 4 files unified (9,994 rows)
--   2. orders_2017  — Single file only via file_filter (3,312 rows)
--
-- Demonstrates:
--   - Multi-file reading: 4 XLSX files from one directory
--   - sheet_name: select "Orders" sheet by name
--   - file_filter: isolate a single file from a multi-file location
--   - file_metadata: df_file_name + df_row_number system columns
--   - infer_schema_rows: control type inference sample size
--   - Type inference: dates, numbers, strings auto-detected
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.excel_demos
    COMMENT 'Excel-backed external tables';

-- ============================================================================
-- TABLE 1: all_orders — All 4 files, full data (9,994 rows)
-- ============================================================================
-- Reads all 4 XLSX files from the directory. Selects the "Orders" sheet by
-- name, enables file metadata for traceability, and samples 1000 rows for
-- schema inference.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_demos.all_orders
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Orders',
    has_header = 'true',
    infer_schema_rows = '1000',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: orders_2017 — Single file only (3,312 rows)
-- ============================================================================
-- Uses file_filter to read only the 2017 file from the same directory.
-- Demonstrates single-file extraction from a multi-file location.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_demos.orders_2017
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Orders',
    has_header = 'true',
    file_filter = 'sales-data-2017*',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
