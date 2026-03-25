-- ============================================================================
-- Excel Multi-Sheet Reporting — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.all_sales WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.all_returns WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.excel.all_staff WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.excel;
DROP ZONE IF EXISTS {{zone_name}};
