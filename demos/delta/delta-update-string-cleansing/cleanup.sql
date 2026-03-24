-- ============================================================================
-- Delta UPDATE String Cleansing — CRM Data Normalization — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.cleansing_demos.customer_imports WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.cleansing_demos;
