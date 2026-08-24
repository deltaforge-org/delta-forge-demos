-- ============================================================================
-- Subsurface: North Sea Demo Field - Cleanup Script
-- ============================================================================
-- Removes every object setup.sql created.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.subsurface.seismic_traces WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.subsurface.seismic_headers WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.subsurface.well_logs WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.subsurface.top_reservoir WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.subsurface.reservoir_model WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.subsurface.survey_navigation WITH FILES;

-- STEP 2: Shared resources (safe, will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.subsurface;
DROP ZONE IF EXISTS {{zone_name}};
