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
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.hl7_patients;
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.fhir_patients;
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation.edi_claims;

-- STEP 2: Drop Tables (WITH FILES removes Delta data from disk)
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation.edi_claims WITH FILES;
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation.fhir_patients WITH FILES;
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation.hl7_patients WITH FILES;

-- STEP 3: Drop Schema
DROP SCHEMA IF EXISTS {{zone_name}}.pseudonymisation;

-- STEP 4: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
