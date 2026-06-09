-- Cleanup: ORC Insurance Claims

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_insurance.claims WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.orc_insurance.policies WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'insurance-claims' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.orc_insurance;
DROP ZONE IF EXISTS {{zone_name}};
