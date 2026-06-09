-- ============================================================================
-- JSON Country Factbook — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json_demos.countries WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json_demos.country_economy WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.json_demos;
DROP ZONE IF EXISTS {{zone_name}};
