-- ============================================================================
-- Graph Weighted Paths — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before tables.
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.shipping_network.shipping_network;

-- STEP 2: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.shipping_network.routes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.shipping_network.ports WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.shipping_network;
DROP ZONE IF EXISTS {{zone_name}};
