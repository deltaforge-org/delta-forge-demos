-- Cleanup: ORC Energy Meters

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_energy.readings WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.orc_energy;
DROP ZONE IF EXISTS {{zone_name}};
