-- Cleanup: Industrial IoT Telemetry — Composite Row-Level Index

DROP INDEX IF EXISTS idx_sensor_time ON TABLE {{zone_name}}.delta_demos.iot_sensor_telemetry;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.iot_sensor_telemetry WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-row-index-iot-telemetry' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
