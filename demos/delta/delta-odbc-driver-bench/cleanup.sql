-- Cleanup: ACME Corporation Production Warehouse (ODBC Driver Wire Benchmark)
-- Drop order: tables -> schema. WITH FILES on each Delta table so the
-- underlying _delta_log and Parquet files are removed too.
--
-- The DROPs against bench.* are kept for one release cycle so that
-- environments still holding the previous demo state (renamed from
-- bench.<n> to acme.<scenario>) can be cleaned up by the same cleanup
-- pass. Remove the bench.* and DROP SCHEMA bench block once you have
-- confirmed no environment still has bench.* tables.

DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.market_ticks          WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.manufacturing_runs    WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.support_tickets       WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.product_catalog       WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.knowledge_articles    WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.document_archive      WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.banking_transactions  WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.shipment_orders       WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.patient_records       WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.acme.forum_posts           WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.acme;

-- Legacy schema cleanup (one-release transition; remove after the next cycle).
-- Tables only: the bench schema itself is left in place because user-driven
-- extras like extras/huge_fixed_narrow_200m.sql may have created tables
-- under it (e.g. bench.fixed_narrow_200m) that live outside the demo's
-- lifecycle and should not be dropped automatically.
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
