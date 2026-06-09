-- ============================================================================
-- FHIR XML Clinical Resources — Cleanup Script
-- ============================================================================
-- Removes the tables and schema created by this demo.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.fhir_xml.patients_xml WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.fhir_xml.observations_xml WITH FILES;
DROP SCHEMA IF EXISTS {{zone_name}}.fhir_xml;
-- Note: {{zone_name}} zone is shared across demos; not dropped here.
