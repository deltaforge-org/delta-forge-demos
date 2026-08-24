-- Cleanup: 2D Survey CDP Index

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.seismic_survey.survey_lines WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.seismic_survey.cdp_index WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.seismic_survey;
