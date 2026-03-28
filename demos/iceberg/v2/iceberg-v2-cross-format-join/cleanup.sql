-- Cleanup: Iceberg Cross-Format Join — Retail Store Analytics

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.retail_demo.sales_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.retail_demo.sales WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.retail_demo.stores WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.retail_demo;
