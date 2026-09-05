-- Cleanup: Geomodel Export Audit

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.model WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.model_all WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.model_ascii WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.grid_export WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.grid_export_all WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.top_reservoir WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.handover.audited_model WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.handover;
