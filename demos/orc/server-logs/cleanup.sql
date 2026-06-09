-- ============================================================================
-- ORC Server Logs — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_demos.all_requests WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_demos.api01_only WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'server-logs' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.orc_demos;
DROP ZONE IF EXISTS {{zone_name}};
