-- ============================================================================
-- Delta Protocol Feature Inspection — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.inherited_plain WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.inherited_cdc WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.inherited_constrained WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-protocol-feature-inspection' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
