-- Cleanup: Iceberg V2 — Retail Multi-Dimensional Aggregation

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg.retail_sales WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg;
