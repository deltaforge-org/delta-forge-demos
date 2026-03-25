-- ============================================================================
-- Excel Options Testbench — Setup Script
-- ============================================================================
-- Creates five external tables, each exercising a specific Excel connector
-- option with data designed so incorrect parsing produces obviously wrong
-- results:
--   1. opt_sheet_name  — sheet_name='Details'  (from 01_multi_sheet.xlsx)
--   2. opt_no_header   — has_header='false'     (from 02_no_header.xlsx)
--   3. opt_skip_rows   — skip_rows='3'          (from 03_skip_rows.xlsx)
--   4. opt_max_rows    — max_rows='5'           (from 04_max_rows.xlsx)
--   5. opt_file_filter — file_filter='05_target*' (from 05_target.xlsx only)
--
-- DETECT SCHEMA notes:
--   - opt_no_header:  NO DETECT SCHEMA (would discover headers, overriding
--                     column_0..column_N naming)
--   - opt_max_rows:   NO DETECT SCHEMA (would read all 20 rows, overriding
--                     the max_rows limit)
--   - All others:     DETECT SCHEMA is safe and applied
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.excel_opts
    COMMENT 'Excel option validation tables — one table per option';


-- ============================================================================
-- TABLE 1: opt_sheet_name — Select a specific sheet from a multi-sheet workbook
-- ============================================================================
-- 01_multi_sheet.xlsx has 3 sheets: Summary (3 rows), Details (5 rows),
-- Metadata (2 rows). We target "Details" which has columns: id, name, score.
-- If sheet_name is ignored, Summary (the first sheet) would be read instead,
-- giving 3 rows with wrong columns.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_opts.opt_sheet_name
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '01_multi_sheet*',
    sheet_name = 'Details'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.excel_opts.opt_sheet_name;
GRANT ADMIN ON TABLE {{zone_name}}.excel_opts.opt_sheet_name TO USER {{current_user}};


-- ============================================================================
-- TABLE 2: opt_no_header — Read file with no header row
-- ============================================================================
-- 02_no_header.xlsx has a single "Data" sheet with 5 rows and NO header row.
-- With has_header='false', columns are auto-named column_0..column_3.
-- If has_header were true (default), the first data row would be consumed as
-- the header, giving only 4 rows with mangled column names.
--
-- ** NO DETECT SCHEMA ** — DETECT SCHEMA would discover headers and override
-- the auto-generated column_0..column_3 names.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_opts.opt_no_header
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '02_no_header*',
    has_header = 'false'
);
GRANT ADMIN ON TABLE {{zone_name}}.excel_opts.opt_no_header TO USER {{current_user}};


-- ============================================================================
-- TABLE 3: opt_skip_rows — Skip metadata rows before the real header
-- ============================================================================
-- 03_skip_rows.xlsx has a "Report" sheet with 3 metadata rows at the top,
-- then a header row (id, project, hours, status), then 5 data rows.
-- skip_rows=3 skips the metadata so the parser finds the header correctly.
-- Without skip_rows, the first metadata row becomes the header and data is
-- garbage.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_opts.opt_skip_rows
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '03_skip_rows*',
    sheet_name = 'Report',
    skip_rows = '3',
    has_header = 'true'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.excel_opts.opt_skip_rows;
GRANT ADMIN ON TABLE {{zone_name}}.excel_opts.opt_skip_rows TO USER {{current_user}};


-- ============================================================================
-- TABLE 4: opt_max_rows — Limit the number of rows read
-- ============================================================================
-- 04_max_rows.xlsx has an "Inventory" sheet with 20 data rows (header +
-- sku, product, stock, price). With max_rows=5, only the first 5 data rows
-- should be returned. Without the limit, all 20 rows appear.
--
-- ** NO DETECT SCHEMA ** — DETECT SCHEMA would read all rows and override
-- the max_rows setting.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_opts.opt_max_rows
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '04_max_rows*',
    sheet_name = 'Inventory',
    max_rows = '5'
);
GRANT ADMIN ON TABLE {{zone_name}}.excel_opts.opt_max_rows TO USER {{current_user}};


-- ============================================================================
-- TABLE 5: opt_file_filter — Include only matching files from a directory
-- ============================================================================
-- The data directory contains 05_target.xlsx (value=CORRECT) and
-- 05_decoy.xlsx (value=WRONG). file_filter='05_target*' should include only
-- the target file. If the filter is ignored, both files are read, injecting
-- WRONG values into the result set.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_opts.opt_file_filter
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '05_target*',
    sheet_name = 'Data'
);
DETECT SCHEMA FOR TABLE {{zone_name}}.excel_opts.opt_file_filter;
GRANT ADMIN ON TABLE {{zone_name}}.excel_opts.opt_file_filter TO USER {{current_user}};
