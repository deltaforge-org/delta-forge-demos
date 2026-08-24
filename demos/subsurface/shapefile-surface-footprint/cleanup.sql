-- Cleanup: Well Pad Lease Compliance

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.surface_land.well_pads WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.surface_land.lease_tracts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.surface_land.pad_compliance WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.surface_land;
