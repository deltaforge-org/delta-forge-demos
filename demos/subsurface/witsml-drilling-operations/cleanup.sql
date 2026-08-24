-- Cleanup: Drilling Survey Anti-Collision

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.drilling.survey_documents WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.drilling.survey_stations WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.drilling;
