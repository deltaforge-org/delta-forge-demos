-- ============================================================================
-- Graph Mutations — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql (including mutated data).
-- ============================================================================

-- STEP 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.hospital_referrals.hospital_referrals;

-- STEP 2: Drop Delta tables (WITH FILES removes physical data too)
DROP DELTA TABLE IF EXISTS {{zone_name}}.hospital_referrals.referrals WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.hospital_referrals.physicians WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'graph-mutations' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.hospital_referrals;
DROP ZONE IF EXISTS {{zone_name}};
