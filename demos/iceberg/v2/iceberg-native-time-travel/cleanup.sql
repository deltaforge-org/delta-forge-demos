-- ============================================================================
-- Iceberg Native Time Travel (Stock Prices) — Cleanup
-- ============================================================================

-- STEP 1: Drop external table and its files
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.stock_prices WITH FILES;

-- STEP 2: Shared resources
-- Remove the per-demo wrapper folder (now empty)
DROP FOLDER 'iceberg-native-time-travel' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
