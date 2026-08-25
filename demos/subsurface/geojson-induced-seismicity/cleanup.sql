-- Cleanup: Induced Seismicity Monitoring

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.seismicity.seismic_feed WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.seismicity.seismic_register WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.seismicity;
