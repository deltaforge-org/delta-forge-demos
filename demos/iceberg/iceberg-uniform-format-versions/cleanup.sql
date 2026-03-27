-- ============================================================================
-- Iceberg UniForm Format Versions — Cleanup
-- ============================================================================

-- STEP 1: Drop Iceberg read-back verification tables
DROP TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v1_iceberg;
DROP TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v2_iceberg;
DROP TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v3_iceberg;

-- STEP 2: Drop all three Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v1 WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v2 WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.sensors_v3 WITH FILES;

-- STEP 3: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
