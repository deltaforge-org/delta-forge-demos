-- Cleanup: Depth Surface Handover and Gross Rock Volume

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.mapping.depth_grids WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.mapping.all_nodes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.mapping.depth_surfaces WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.mapping;
