-- ============================================================================
-- Delta MERGE SCD2 — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.policy_changes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.policy_dim WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
