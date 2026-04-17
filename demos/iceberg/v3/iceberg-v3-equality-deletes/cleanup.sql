-- ============================================================================
-- Iceberg V3 Equality Delete Files — Cleanup
-- ============================================================================

-- STEP 1: Drop the external Iceberg table and its backing files.
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.patient_visits WITH FILES;

-- STEP 2: Shared resources (used by other iceberg demos if present)
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
