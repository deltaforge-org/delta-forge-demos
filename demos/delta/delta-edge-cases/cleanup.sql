-- ============================================================================
-- Delta Edge Cases — Empty, Wide & Minimal Tables — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.config_singleton WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.wide_metrics WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.empty_staging WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-edge-cases' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
