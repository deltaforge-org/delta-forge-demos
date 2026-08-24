-- Cleanup: Aerial Survey Raster Catalogue

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.raster_survey.raster_tiles WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.raster_survey.raster_catalog WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.raster_survey;
