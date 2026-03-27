-- ============================================================================
-- Iceberg Native Time Travel (Stock Prices) — Cleanup
-- ============================================================================

-- STEP 1: Drop external table and its files
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg.stock_prices WITH FILES;

-- STEP 2: Shared resources
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg;
DROP ZONE IF EXISTS {{zone_name}};
