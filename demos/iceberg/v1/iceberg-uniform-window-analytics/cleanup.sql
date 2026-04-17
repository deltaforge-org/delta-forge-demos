-- Cleanup: Regional Sales Performance — Window Analytics with UniForm

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.sales_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.sales WITH FILES;
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
