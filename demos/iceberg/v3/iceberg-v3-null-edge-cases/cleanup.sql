-- ============================================================================
-- Iceberg V3 — Clinical Lab NULL Edge Cases — Cleanup
-- ============================================================================

-- STEP 1: Drop tables (native Iceberg, files live under LOCATION)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.null_lab_results WITH FILES;

-- STEP 2: Shared resources (used by other iceberg demos if present)
-- Remove the per-demo wrapper folder (now empty)
DROP FOLDER 'iceberg-v3-null-edge-cases' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
