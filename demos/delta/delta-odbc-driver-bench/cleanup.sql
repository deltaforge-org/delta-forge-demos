-- Cleanup: ODBC Driver Wire Benchmark Suite
-- Drop order: tables -> schema -> zone. WITH FILES on each delta table so
-- the underlying _delta_log and Parquet files are removed too. The zone
-- itself is left in place because the demo created it dedicated; flip the
-- zone DROP on if you want a fully clean slate.

DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.fixed_narrow      WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.fixed_wide        WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.string_narrow     WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.string_wide_kv    WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.string_long       WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.binary_blobs      WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.decimal_temporal  WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.nested_json       WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.null_heavy        WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.skewed_strings    WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.bench;

DROP ZONE IF EXISTS {{zone_name}};
