-- ============================================================================
-- Delta Append-Only Enforcement — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.compliance_ledger WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.mutable_ledger WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
