-- ============================================================================
-- FHIR Patient Demographics — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.fhir_demos.patients_bulk WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.fhir_demos.patients_detailed WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.fhir_demos;
DROP ZONE IF EXISTS {{zone_name}};
