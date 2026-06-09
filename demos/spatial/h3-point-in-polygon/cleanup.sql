-- ============================================================================
-- H3 Point-in-Polygon — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before the tables they depend on.
-- ============================================================================

-- STEP 1: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.spatial_demos.zone_cells;
DROP VIEW IF EXISTS {{zone_name}}.spatial_demos.driver_cells;

-- STEP 2: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial_demos.driver_positions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial_demos.zones WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'h3-point-in-polygon' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.spatial_demos;
DROP ZONE IF EXISTS {{zone_name}};
