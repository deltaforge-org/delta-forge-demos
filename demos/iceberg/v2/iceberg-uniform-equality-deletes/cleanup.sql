-- ============================================================================
-- Iceberg UniForm Equality Deletes — Cleanup
-- ============================================================================

-- STEP 1: Drop tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.products_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.products WITH FILES;

-- STEP 2: Drop schema and zone
-- Remove the per-demo wrapper folder (now empty)
DROP FOLDER 'iceberg-uniform-equality-deletes' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
