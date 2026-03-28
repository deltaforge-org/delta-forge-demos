-- Cleanup: Iceberg V3 — Clinical Lab NULL Edge Cases

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg.lab_results WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg;
