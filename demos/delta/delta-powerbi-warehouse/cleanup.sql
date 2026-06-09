-- Cleanup: Pacific Retail Group: Power BI Star Warehouse Benchmark
-- Drop order: tables -> schema. WITH FILES on each Delta table so the
-- underlying _delta_log and Parquet files are removed too.

DROP DELTA TABLE IF EXISTS {{zone_name}}.retail.fact_web_events           WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.retail.fact_inventory_snapshot   WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.retail.fact_sales                WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.retail.dim_customer              WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.retail.dim_product               WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.retail.dim_store                 WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.retail.dim_date                  WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.retail;
