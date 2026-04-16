-- ============================================================================
-- GPU Karate Club — Cleanup Script
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.gpu_karate.gpu_karate;

-- STEP 2: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.gpu_karate.edges WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.gpu_karate.vertices WITH FILES;

-- STEP 3: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.gpu_karate_raw.karate_edges WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.gpu_karate_raw.karate_vertices WITH FILES;

-- STEP 4: Drop schemas
DROP SCHEMA IF EXISTS {{zone_name}}.gpu_karate;
DROP SCHEMA IF EXISTS {{zone_name}}.gpu_karate_raw;
