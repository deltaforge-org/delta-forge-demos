-- ============================================================================
-- Iceberg UniForm Puffin Deletion Vectors — Cleanup
-- ============================================================================

-- STEP 1: Drop tables
-- The external iceberg table shares the Delta table's location, so we drop
-- the registration only (no WITH FILES). The Delta DROP WITH FILES below
-- removes the backing directory once for both registrations.
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.products_iceberg;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.products WITH FILES;

-- STEP 2: Drop schema and zone
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
