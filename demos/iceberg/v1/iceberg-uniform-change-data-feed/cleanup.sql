-- ============================================================================
-- E-Commerce Order Lifecycle — Change Data Feed with UniForm — Cleanup
-- ============================================================================

-- STEP 1: Drop Iceberg read-back verification table
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.orders_iceberg WITH FILES;

-- STEP 2: Drop tables (includes Delta log + Iceberg metadata/ directory)
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.orders WITH FILES;

-- STEP 3: Remove the per-demo wrapper folder (now empty)
DROP FOLDER 'iceberg-uniform-change-data-feed' IF EXISTS IN ZONE {{zone_name}};

-- STEP 4: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
