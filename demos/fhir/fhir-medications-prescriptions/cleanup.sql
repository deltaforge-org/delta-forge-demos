-- ============================================================================
-- FHIR Medications & Prescriptions — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.fhir_demos.prescriptions WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.fhir_demos.coverage WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.fhir_demos;
DROP ZONE IF EXISTS {{zone_name}};
