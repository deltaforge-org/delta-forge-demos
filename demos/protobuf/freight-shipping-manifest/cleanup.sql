-- Cleanup: Freight Shipping Manifest

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.protobuf_freight.shipment_tracking WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.protobuf_freight.shipment_packages WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.protobuf_freight.shipments WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.protobuf_freight;
