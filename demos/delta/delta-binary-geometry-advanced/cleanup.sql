-- ============================================================================
-- Delta Binary & Geometry Advanced — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.geo_audit_log WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.documents WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.locations WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-binary-geometry-advanced' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
