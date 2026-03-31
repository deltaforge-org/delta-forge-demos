-- ============================================================================
-- NetScience Coauthorship Network — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- ============================================================================

-- STEP 1: Drop graph definition (also cascade-deletes table mappings)
DROP GRAPH IF EXISTS {{zone_name}}.netscience_collab.netscience_collab;

-- STEP 2: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.netscience_collab.vertices WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.netscience_collab.edges WITH FILES;

-- STEP 3: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.netscience_raw.netscience_edges WITH FILES;

-- STEP 4: Drop schemas and zone
DROP SCHEMA IF EXISTS {{zone_name}}.netscience_collab;
DROP SCHEMA IF EXISTS {{zone_name}}.netscience_raw;
DROP ZONE IF EXISTS {{zone_name}};
