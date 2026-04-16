-- ============================================================================
-- GPU Graph Stress Test — Cleanup Script
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.gpu_stress_network.gpu_stress_network;

-- STEP 2: Drop Delta tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.gpu_stress_network.gpu_st_edges WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.gpu_stress_network.gpu_st_people WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.gpu_stress_network.gpu_st_departments WITH FILES;

-- STEP 3: Drop schema
DROP SCHEMA IF EXISTS {{zone_name}}.gpu_stress_network;
