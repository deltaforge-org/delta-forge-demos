-- ============================================================================
-- Pseudonymisation Exempt Roles & Users -- Cleanup Script
-- ============================================================================
-- Removes everything created by setup.sql. Pseudonymisation rules are
-- removed first so the table can be dropped without the catalog
-- complaining about dangling rule references.
-- ============================================================================

-- STEP 1: Drop pseudonymisation rules
DROP PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_exempt.customers;

-- STEP 2: Drop table
DROP TABLE IF EXISTS {{zone_name}}.pseudonymisation_exempt.customers;

-- STEP 3: Drop schema

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'pseudonymisation-exempt-roles' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.pseudonymisation_exempt;

-- STEP 4: Drop zone
DROP ZONE IF EXISTS {{zone_name}};
