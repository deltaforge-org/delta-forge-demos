-- ============================================================================
-- Excel Options Testbench — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel_opts.opt_sheet_name WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel_opts.opt_no_header WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel_opts.opt_skip_rows WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel_opts.opt_max_rows WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel_opts.opt_file_filter WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.excel_opts;
DROP ZONE IF EXISTS {{zone_name}};
