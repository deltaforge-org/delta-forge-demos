-- Cleanup: RMS Horizon Handover, Irap Surfaces in Both Containers

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.geomodel.horizons WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.geomodel.all_nodes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.geomodel.surfaces WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.geomodel;
