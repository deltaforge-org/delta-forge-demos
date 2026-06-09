-- ============================================================================
-- EDI EDIFACT International Trade — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop External Table
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi_demos.edifact_messages WITH FILES;

-- STEP 2: Drop Schema

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'edi-edifact-international-trade' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.edi_demos;

-- STEP 3: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
