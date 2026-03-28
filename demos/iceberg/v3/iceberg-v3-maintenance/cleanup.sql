-- Cleanup: Iceberg V3 UniForm — SaaS Billing OPTIMIZE & VACUUM

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.subscriptions_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.subscriptions WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
