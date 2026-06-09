-- Cleanup: GIS Emergency Response Network

DROP DELTA TABLE IF EXISTS {{zone_name}}.emergency.response_zones WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.emergency.incidents WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.emergency.hospitals WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'gis-emergency-response' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.emergency;
