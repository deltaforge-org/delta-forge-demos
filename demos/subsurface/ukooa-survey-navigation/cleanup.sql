-- Cleanup: Survey Navigation Database

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.survey_navigation.navigation_lines WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.survey_navigation.shot_point_index WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.survey_navigation;
