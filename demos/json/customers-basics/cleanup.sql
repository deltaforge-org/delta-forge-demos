-- ============================================================================
-- JSON Customers Basics — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external table
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.json_demos.customers WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.json_demos;
DROP ZONE IF EXISTS {{zone_name}};
