-- Cleanup: Iceberg V3 UniForm — Supply Chain Inventory MERGE Sync

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.warehouse_inventory_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.warehouse_inventory WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
