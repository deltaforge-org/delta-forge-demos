-- ============================================================================
-- Email-Eu-core — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- ============================================================================

-- STEP 1: Drop graph definition (also cascade-deletes table mappings)
DROP GRAPH IF EXISTS {{zone_name}}.email_eu_core.email_eu_core;

-- STEP 2: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.email_eu_core.vertices WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.email_eu_core.edges WITH FILES;

-- STEP 3: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.email_eu_core_raw.email_eu_edges WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.email_eu_core_raw.email_eu_vertices WITH FILES;

-- STEP 4: Drop schemas and zone

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'graph-email-eu-core' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.email_eu_core;
DROP SCHEMA IF EXISTS {{zone_name}}.email_eu_core_raw;
DROP ZONE IF EXISTS {{zone_name}};
