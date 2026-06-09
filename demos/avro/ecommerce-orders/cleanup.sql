-- Cleanup: Avro E-Commerce Orders

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.ecommerce.all_orders WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.ecommerce.q1_orders WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.ecommerce.sample_orders WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.ecommerce;
