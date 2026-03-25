-- ============================================================================
-- EDI HIPAA Claims Financial — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop External Tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi.claims_header WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi.claims_remittance WITH FILES;

-- STEP 2: Drop Schema
DROP SCHEMA IF EXISTS {{zone_name}}.edi;

-- STEP 3: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
