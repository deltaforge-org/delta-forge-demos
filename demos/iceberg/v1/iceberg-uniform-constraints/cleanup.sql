-- Cleanup: Bank Transaction Validation — CHECK Constraints with UniForm

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.transactions_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.transactions WITH FILES;
DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
