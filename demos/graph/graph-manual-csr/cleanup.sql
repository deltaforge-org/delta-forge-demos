-- ============================================================================
-- Manual CSR Cache Management — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.karate_manual.karate_manual;

-- STEP 2: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.karate_manual.vertices WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.karate_manual.edges WITH FILES;

-- STEP 3: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.karate_edges WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raw.karate_vertices WITH FILES;

-- STEP 4: Drop schemas
DROP SCHEMA IF EXISTS {{zone_name}}.karate_manual;
DROP SCHEMA IF EXISTS {{zone_name}}.raw;
