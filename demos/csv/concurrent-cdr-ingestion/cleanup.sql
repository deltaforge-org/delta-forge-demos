-- ============================================================================
-- Concurrent CDR Ingestion -- Cleanup Script
-- ============================================================================
-- Drop order: Delta targets -> regional feed tables (own the CSV files) ->
-- the all_cdr glob view (shares those files, so dropped without FILES) ->
-- wrapper folder -> schema -> zone.
-- ============================================================================

-- Delta targets (each owns its own data directory)
DROP DELTA TABLE IF EXISTS {{zone_name}}.cdr.cdr_consolidated WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.cdr.cdr_parallel WITH FILES;

-- Regional feed external tables -- WITH FILES deletes each region_NN.csv once
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_01 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_02 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_03 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_04 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_05 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_06 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_07 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_08 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_09 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.region_10 WITH FILES;

-- Glob view over the same files (already removed above) -- unregister only
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.cdr.all_cdr;

-- Remove the now-empty per-demo wrapper folder
DROP FOLDER 'concurrent-cdr-ingestion' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.cdr;
DROP ZONE IF EXISTS {{zone_name}};
