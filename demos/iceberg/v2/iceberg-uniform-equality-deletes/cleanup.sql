-- ============================================================================
-- Iceberg UniForm Equality Deletes — Cleanup
-- ============================================================================

-- STEP 1: Drop tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.products_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.products WITH FILES;

-- STEP 2: Drop schema and zone
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
