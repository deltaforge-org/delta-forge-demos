-- ============================================================================
-- ORC Banking Transactions — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_bank.all_transactions WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_bank.downtown_only WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.orc_bank;
DROP ZONE IF EXISTS {{zone_name}};
