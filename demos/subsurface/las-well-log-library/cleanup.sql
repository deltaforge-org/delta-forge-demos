-- Cleanup: Well Log Library Consolidation

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.log_library.log_files WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.log_library.log_library WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.log_library;
