-- Cleanup: Iceberg Cross-Format Join — Retail Store Analytics

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.sales_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.sales WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.stores WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
