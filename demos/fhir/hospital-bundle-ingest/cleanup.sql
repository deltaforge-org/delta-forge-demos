-- ============================================================================
-- Hospital Interop — Daily Clinical Bundle Ingest — Cleanup
-- ============================================================================
-- Order: silver delta table -> bronze external table -> schemas.
-- Zones are left in place (other demos may share them).
-- ============================================================================

DROP DELTA TABLE IF EXISTS clinical_silver.fhir_silver.encounter_observations WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.fhir_bronze.bundle_resources WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'hospital-bundle-ingest' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS clinical_silver.fhir_silver;
DROP SCHEMA IF EXISTS {{zone_name}}.fhir_bronze;
