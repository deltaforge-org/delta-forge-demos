-- ============================================================================
-- Iceberg V3 Position Delete Files — Cleanup
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.equity_trades WITH FILES;

-- STEP 2: Shared resources (used by other iceberg demos if present)
-- Remove the per-demo wrapper folder (now empty)
DROP FOLDER 'iceberg-v3-position-deletes' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
