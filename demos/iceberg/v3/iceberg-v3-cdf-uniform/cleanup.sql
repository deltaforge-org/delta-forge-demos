-- Cleanup: Iceberg V3 UniForm — CDF Payment Reconciliation

-- The external iceberg table shares the Delta table's location, so we drop
-- the registration only (no WITH FILES). The Delta DROP WITH FILES below
-- removes the backing directory once for both registrations.
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.payment_transactions_iceberg;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.payment_transactions WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
DROP ZONE IF EXISTS {{zone_name}};
