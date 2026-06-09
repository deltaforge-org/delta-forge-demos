-- ============================================================================
-- Pseudonymisation Apply — Clinical Trial De-identification — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql: pseudonymisation rules, table,
-- schema, and zone.
--
-- Pseudonymisation rules are dropped per-table (omitting column pattern drops
-- all rules for that table). Table is dropped in reverse creation order.
-- ============================================================================

-- STEP 1: Drop Pseudonymisation Rules
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.trial_participants;

-- STEP 2: Drop Table
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation_demos.trial_participants;

-- STEP 3: Drop Schema

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'pseudonymisation-apply' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.pseudonymisation_demos;

-- STEP 4: Drop Zone
DROP ZONE IF EXISTS {{zone_name}};
