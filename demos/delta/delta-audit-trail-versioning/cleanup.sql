-- ============================================================================
-- Delta Audit Trail — Native Version-Based Compliance — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.audit_demos.compliance_events WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.audit_demos;
DROP ZONE IF EXISTS {{zone_name}};
