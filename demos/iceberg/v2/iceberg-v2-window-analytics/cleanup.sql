-- ============================================================================
-- Iceberg V2 — Airline Loyalty Window Analytics — Cleanup
-- ============================================================================

-- STEP 1: Drop tables (native Iceberg, files live under LOCATION)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.loyalty_members WITH FILES;

-- STEP 2: Shared resources (used by other iceberg demos if present)
-- Remove the per-demo wrapper folder (now empty)
DROP FOLDER 'iceberg-v2-window-analytics' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
