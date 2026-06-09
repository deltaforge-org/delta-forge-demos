-- Cleanup: ORC Clinical Trials

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_trials.patients WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'clinical-trials' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.orc_trials;
DROP ZONE IF EXISTS {{zone_name}};
