-- ============================================================================
-- Parquet Supply Chain — Cleanup Script
-- ============================================================================
-- Removes all objects created by setup.sql.
-- ============================================================================

-- STEP 1: Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet_demos.all_orders WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet_demos.orders_2015 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet_demos.orders_sample WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.parquet_demos.orders_q1_2014 WITH FILES;

-- STEP 2: Shared resources (safe — will warn if other demos still use them)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'supply-chain' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.parquet_demos;
DROP ZONE IF EXISTS {{zone_name}};
