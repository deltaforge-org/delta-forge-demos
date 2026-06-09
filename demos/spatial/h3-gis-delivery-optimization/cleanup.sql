-- Cleanup: H3+GIS Delivery Optimization

DROP DELTA TABLE IF EXISTS {{zone_name}}.logistics.stores WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.logistics.warehouses WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'h3-gis-delivery-optimization' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.logistics;
