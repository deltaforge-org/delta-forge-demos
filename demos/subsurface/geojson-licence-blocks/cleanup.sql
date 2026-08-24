-- Cleanup: Licence Block Acreage and Relinquishment

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.licensing.block_awards WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.licensing.licence_blocks WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.licensing;
