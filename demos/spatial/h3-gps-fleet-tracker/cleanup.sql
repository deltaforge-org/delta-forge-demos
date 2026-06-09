-- ============================================================================
-- H3 GPS Fleet Tracker — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before tables they depend on.
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- STEP 1: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.spatial_demos.region_cells;
DROP VIEW IF EXISTS {{zone_name}}.spatial_demos.points_h3;

-- STEP 2: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial_demos.gps_points WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial_demos.regions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.spatial_demos.landmarks WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'h3-gps-fleet-tracker' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.spatial_demos;
DROP ZONE IF EXISTS {{zone_name}};
