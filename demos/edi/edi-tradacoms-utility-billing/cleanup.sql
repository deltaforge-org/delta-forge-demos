-- ============================================================================
-- EDI TRADACOMS — UK Energy Billing & VAT Reconciliation — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
--
-- The schema and zone are shared across demos. DROP SCHEMA / DROP ZONE will
-- succeed silently if they are empty, or produce a warning (not an error) if
-- other tables / schemas still exist — so it is always safe to leave them in.
-- ============================================================================

-- STEP 1: Drop External Tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi_demos.tradacoms_bills WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.edi_demos.tradacoms_bill_details WITH FILES;

-- STEP 2: Drop Schema

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'edi-tradacoms-utility-billing' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.edi_demos;

-- STEP 3: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
