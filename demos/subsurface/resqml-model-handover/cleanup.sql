-- Cleanup: Reservoir Model Handover Audit

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.model_handover.model_packages WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.model_handover.unpacked_parts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.model_handover.model_inventory WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.model_handover;
