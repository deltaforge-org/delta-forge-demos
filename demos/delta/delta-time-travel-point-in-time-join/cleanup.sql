-- ============================================================================
-- Delta Time Travel — Point-in-Time Joins — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.fx_trades WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.fx_rates WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-time-travel-point-in-time-join' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
