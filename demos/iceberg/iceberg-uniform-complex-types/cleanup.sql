-- ============================================================================
-- Iceberg UniForm Complex Types — Cleanup
-- ============================================================================

-- STEP 1: Drop Iceberg read-back verification table
DROP TABLE IF EXISTS {{zone_name}}.iceberg_demos.product_catalog_nested_iceberg;

-- STEP 2: Drop tables (includes Delta log + Iceberg metadata/ directory)
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.product_catalog_nested WITH FILES;

-- STEP 3: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
