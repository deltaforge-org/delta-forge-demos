-- ============================================================================
-- Delta Partition-Scoped DELETE — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop Iceberg read-back verification table
DROP TABLE IF EXISTS {{zone_name}}.delta_demos.warehouse_orders_iceberg;

-- STEP 2: Drop Delta table (includes Delta log + Iceberg metadata/ directory)
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.warehouse_orders WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
