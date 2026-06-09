-- ============================================================================
-- CSV Advanced Options Testbench — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_demos.opt_delimiter WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_demos.opt_null_value WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_demos.opt_comment WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_demos.opt_skip_rows WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_demos.opt_max_rows WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_demos.opt_trim WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_demos.opt_quoted WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.csv_demos.opt_combined WITH FILES;

-- Shared resources (safe — won't fail if other demos use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'csv-options-testbench' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.csv_demos;
DROP ZONE IF EXISTS {{zone_name}};
