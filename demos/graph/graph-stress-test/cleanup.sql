-- ============================================================================
-- Graph Stress Test — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Views must be dropped before tables they depend on.
-- ============================================================================

-- STEP 1: Drop graph definition (also cascade-deletes table mappings)
DROP GRAPH IF EXISTS {{zone_name}}.stress_test_network.stress_test_network;

-- STEP 2: Drop views (depend on tables, so drop first)
DROP VIEW IF EXISTS {{zone_name}}.stress_test_network.st_dept_matrix;
DROP VIEW IF EXISTS {{zone_name}}.stress_test_network.st_people_stats;

-- STEP 3: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.stress_test_network.st_edges WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.stress_test_network.st_people WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.stress_test_network.st_departments WITH FILES;

-- STEP 4: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.stress_test_network;
DROP ZONE IF EXISTS {{zone_name}};
