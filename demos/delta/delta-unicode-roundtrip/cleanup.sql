-- ============================================================================
-- Delta Unicode Roundtrip — Mutation Fidelity Proof — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.global_bazaar WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
