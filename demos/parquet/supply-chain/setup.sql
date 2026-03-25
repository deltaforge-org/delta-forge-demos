-- ============================================================================
-- Parquet Supply Chain — Setup Script
-- ============================================================================
-- Creates four external tables from 14 quarterly Parquet files (2012–2016):
--   1. all_orders     — All 14 files via recursive scanning (73,089 rows)
--   2. orders_2015    — Year filter via file_filter (23,636 rows)
--   3. orders_sample  — Sampled subset via max_rows (100 per file)
--   4. orders_q1_2014 — Single quarter via file_filter (5,210 rows)
--
-- Demonstrates:
--   - recursive: scan year-based subdirectories (2012/, 2013/, etc.)
--   - file_filter: glob pattern to select files by year or quarter
--   - max_rows: limit rows per file for data profiling
--   - row_group_filter: enable predicate pushdown via row group statistics
--   - file_metadata: df_file_name + df_row_number system columns
--   - Self-describing schema: Parquet metadata provides types automatically
--   - Multi-file reading: 14 files across 5 directories
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.parquet
    COMMENT 'Parquet-backed external tables';

-- ============================================================================
-- TABLE 1: all_orders — All 14 files via recursive directory scanning
-- ============================================================================
-- Reads all Parquet files from the orders/ directory tree, recursing into
-- year-based subdirectories (2012/, 2013/, 2014/, 2015/, 2016/). Row group
-- filtering is enabled for predicate pushdown via Parquet statistics.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.parquet.all_orders
USING PARQUET
LOCATION '{{data_path}}/orders'
OPTIONS (
    recursive = 'true',
    row_group_filter = 'true',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
GRANT ADMIN ON TABLE {{zone_name}}.parquet.all_orders TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.parquet.all_orders;


-- ============================================================================
-- TABLE 2: orders_2015 — Single year via file_filter (23,636 rows)
-- ============================================================================
-- Uses file_filter to read only files starting with 'Orders_2015' from the
-- recursive directory scan. This extracts all 4 quarters of 2015 data.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.parquet.orders_2015
USING PARQUET
LOCATION '{{data_path}}/orders'
OPTIONS (
    recursive = 'true',
    file_filter = 'Orders_2015*',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
GRANT ADMIN ON TABLE {{zone_name}}.parquet.orders_2015 TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.parquet.orders_2015;


-- ============================================================================
-- TABLE 3: orders_sample — Data profiling via max_rows (100 per file)
-- ============================================================================
-- Limits to 100 rows per file for quick data profiling. With 14 files,
-- produces approximately 1,400 rows — enough to inspect data quality
-- without reading the full 73,089-row dataset.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.parquet.orders_sample
USING PARQUET
LOCATION '{{data_path}}/orders'
OPTIONS (
    recursive = 'true',
    max_rows = '100',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
GRANT ADMIN ON TABLE {{zone_name}}.parquet.orders_sample TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.parquet.orders_sample;


-- ============================================================================
-- TABLE 4: orders_q1_2014 — Single quarter drill-down (5,210 rows)
-- ============================================================================
-- Uses file_filter to read only the Q1 2014 file (March–May 2014).
-- Demonstrates precise single-file extraction from a multi-file location.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.parquet.orders_q1_2014
USING PARQUET
LOCATION '{{data_path}}/orders'
OPTIONS (
    recursive = 'true',
    file_filter = 'Orders_2014-03*',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
GRANT ADMIN ON TABLE {{zone_name}}.parquet.orders_q1_2014 TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.parquet.orders_q1_2014;
