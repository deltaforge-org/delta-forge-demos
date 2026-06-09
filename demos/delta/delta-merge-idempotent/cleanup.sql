-- ============================================================================
-- Delta MERGE Idempotent — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.sensor_batch WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.idempotent_sensor_readings WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-merge-idempotent' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
