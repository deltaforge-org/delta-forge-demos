-- ============================================================================
-- Iceberg V2 Multi-Partition Weather Readings — Cleanup
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.weather_readings WITH FILES;

-- STEP 2: Shared resources (used by other iceberg demos if present)
-- Remove the per-demo wrapper folder (now empty)
DROP FOLDER 'iceberg-v2-multi-partition' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
