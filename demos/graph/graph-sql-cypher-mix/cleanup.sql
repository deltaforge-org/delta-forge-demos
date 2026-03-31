-- ============================================================================
-- Sales Territory Optimization — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- Graph definition must be dropped before the tables it references.
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.customer_network.customer_network;

-- STEP 2: Drop working tables (populated by Cypher in queries.sql)
DROP DELTA TABLE IF EXISTS {{zone_name}}.customer_network.community_assignments WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.customer_network.influence_scores WITH FILES;

-- STEP 3: Drop data tables
DROP DELTA TABLE IF EXISTS {{zone_name}}.customer_network.sales_reps WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.customer_network.orders WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.customer_network.referrals WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.customer_network.customers WITH FILES;

-- STEP 4: Drop schema and zone
DROP SCHEMA IF EXISTS {{zone_name}}.customer_network;
DROP ZONE IF EXISTS {{zone_name}};
