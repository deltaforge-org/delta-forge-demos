-- Cleanup: Monthly Production Allocation

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.production.production_reports WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.production.production_history WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.production;
