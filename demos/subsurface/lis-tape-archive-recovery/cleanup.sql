-- Cleanup: LIS Tape Archive Recovery

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.log_archive.volve_composite WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.log_archive.reprocessed_composite WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.log_archive.tape_archive WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.log_archive;
