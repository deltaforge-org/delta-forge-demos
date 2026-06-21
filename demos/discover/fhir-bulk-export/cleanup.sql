-- ============================================================================
-- Cleanup: FHIR Bulk-Export Auto-Onboarding
-- ============================================================================
-- patient_export was registered by DISCOVER (not a hand-written CREATE
-- EXTERNAL TABLE), but it is an ordinary external table and drops the same
-- way. IF EXISTS keeps this harmless when a prior run failed before the
-- DISCOVER EXECUTE block.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.discover_demos.patient_export WITH FILES;

-- Remove the per-demo export folder (now empty after the table drop).
DROP FOLDER 'discover-fhir-bulk-export' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.discover_demos;
