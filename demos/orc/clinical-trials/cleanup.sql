-- Cleanup: ORC Clinical Trials

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_trials.patients WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.orc_trials;
DROP ZONE IF EXISTS {{zone_name}};
