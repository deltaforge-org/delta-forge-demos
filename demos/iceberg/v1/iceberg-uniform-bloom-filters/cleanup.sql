-- Cleanup: Customer Loyalty Program — Bloom Filters with UniForm

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.members_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.members WITH FILES;
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
