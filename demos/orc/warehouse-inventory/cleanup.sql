-- Cleanup: ORC Warehouse Inventory

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_inventory.stock WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.orc_inventory;
DROP ZONE IF EXISTS {{zone_name}};
