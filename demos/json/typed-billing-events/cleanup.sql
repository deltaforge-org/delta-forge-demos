-- ============================================================================
-- JSON Typed Billing Events — Cleanup Script
-- ============================================================================
-- Removes silver -> bronze -> schema -> zone in dependency order.
-- ============================================================================

-- STEP 1: Drop silver Delta table (curated layer first — depends on bronze)
DROP DELTA TABLE IF EXISTS {{zone_name}}.billing.events_curated WITH FILES;

-- STEP 2: Drop bronze external table
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.billing.events WITH FILES;

-- STEP 3: Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'typed-billing-events' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.billing;
DROP ZONE IF EXISTS {{zone_name}};
