-- ============================================================================
-- Delta MERGE Idempotent — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.sensor_batch WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.idempotent_sensor_readings WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
