-- ============================================================================
-- Pseudonymisation Healthcare — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql: pseudonymisation rules, tables,
-- schema, and zone.
--
-- Pseudonymisation rules are dropped per-table (omitting column pattern drops
-- all rules for that table). Tables are dropped in reverse creation order.
-- ============================================================================

-- STEP 1: Drop Pseudonymisation Rules
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.hl7_patients;
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.fhir_patients;
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.edi_claims;

-- STEP 2: Drop Tables
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation_demos.edi_claims;
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation_demos.fhir_patients;
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation_demos.hl7_patients;

-- STEP 3: Drop Schema

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'pseudonymisation-healthcare' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.pseudonymisation_demos;

-- STEP 4: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
