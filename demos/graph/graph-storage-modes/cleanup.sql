-- ============================================================================
-- Graph Storage Modes — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql (3 graph definitions, 6 tables).
-- ============================================================================

-- STEP 1: Drop graph definitions
DROP GRAPH IF EXISTS {{zone_name}}.storage_modes.storage_flat;
DROP GRAPH IF EXISTS {{zone_name}}.storage_modes.storage_hybrid;
DROP GRAPH IF EXISTS {{zone_name}}.storage_modes.storage_json;

-- STEP 2: Drop flattened tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.storage_modes.edges_flat WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.storage_modes.persons_flat WITH FILES;

-- STEP 3: Drop hybrid tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.storage_modes.edges_hybrid WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.storage_modes.persons_hybrid WITH FILES;

-- STEP 4: Drop JSON tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.storage_modes.edges_json WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.storage_modes.persons_json WITH FILES;

-- STEP 5: Shared resources
DROP SCHEMA IF EXISTS {{zone_name}}.storage_modes;
DROP ZONE IF EXISTS {{zone_name}};
