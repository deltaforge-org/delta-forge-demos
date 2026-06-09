-- ============================================================================
-- Delta MERGE Composite Keys — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.telemetry_batch WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.fleet_daily_summary WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-merge-composite-keys' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
