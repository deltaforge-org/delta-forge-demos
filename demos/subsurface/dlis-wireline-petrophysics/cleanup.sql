-- Cleanup: Volve Composite Log Ingestion

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.petrophysics.lwd_composite WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.petrophysics.wireline_composite WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.petrophysics.well_logs WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.petrophysics;
