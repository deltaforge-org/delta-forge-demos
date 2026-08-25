-- Cleanup: Offshore Lease Footprint

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.surface_land.leases WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.surface_land.blocks WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.surface_land.lease_register WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.surface_land;
