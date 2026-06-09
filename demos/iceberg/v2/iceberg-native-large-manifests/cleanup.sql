-- ============================================================================
-- Iceberg Native Large Manifests (Web Analytics) — Cleanup
-- ============================================================================

-- STEP 1: Drop external table and its files
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.web_analytics WITH FILES;

-- STEP 2: Shared resources
-- Remove the per-demo wrapper folder (now empty)
DROP FOLDER 'iceberg-native-large-manifests' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
