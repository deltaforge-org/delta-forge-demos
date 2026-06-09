-- Cleanup: GIS Maritime Shipping

DROP DELTA TABLE IF EXISTS {{zone_name}}.maritime.positions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.maritime.vessels WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.maritime.ports WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'gis-maritime-shipping' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.maritime;
