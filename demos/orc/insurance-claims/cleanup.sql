-- Cleanup: ORC Insurance Claims

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_insurance.claims WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_insurance.policies WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.orc_insurance;
DROP ZONE IF EXISTS {{zone_name}};
