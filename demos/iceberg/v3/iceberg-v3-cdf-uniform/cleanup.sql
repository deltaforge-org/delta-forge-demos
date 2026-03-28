-- Cleanup: Iceberg V3 UniForm — CDF Payment Reconciliation

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.payment_transactions_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.payment_transactions WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
