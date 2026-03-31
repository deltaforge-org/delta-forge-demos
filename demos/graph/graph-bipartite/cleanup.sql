-- ============================================================================
-- Graph Bipartite — Movie Recommendation Engine — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.movie_recs.movie_recs;

-- STEP 2: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.movie_recs.ratings WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.movie_recs.entities WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.movie_recs;
DROP ZONE IF EXISTS {{zone_name}};
