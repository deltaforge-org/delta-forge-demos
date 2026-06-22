-- ============================================================================
-- Cleanup: Healthcare Interoperability Onboarding
-- ============================================================================
-- Drop the silver Delta tables first, then the DISCOVER-registered bronze
-- external tables, then the per-demo landing folder and the schema. IF EXISTS
-- keeps this harmless when a prior run failed partway through.
-- ============================================================================

-- Drop the query-time pseudonymisation rules before their table.
DROP PSEUDONYMISATION RULE ON {{zone_name}}.healthcare.fhir_patients;

DROP DELTA TABLE IF EXISTS {{zone_name}}.healthcare.hl7_admissions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.healthcare.fhir_patients WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.healthcare.fhir_patients_shared WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.healthcare.edi_claims WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.healthcare.adt_bronze WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.healthcare.patients_bronze WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.healthcare.claims_bronze WITH FILES;

-- Remove the per-demo landing + silver folder tree.
DROP FOLDER 'discover-healthcare-interop' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.healthcare;
