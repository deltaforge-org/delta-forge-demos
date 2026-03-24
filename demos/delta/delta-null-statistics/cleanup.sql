-- ============================================================================
-- NULL Statistics — NULL-Aware Query Optimization — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.null_demos.patient_records WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.null_demos;
DROP ZONE IF EXISTS {{zone_name}};
