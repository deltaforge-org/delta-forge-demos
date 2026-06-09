-- ============================================================================
-- Protobuf IoT Sensor Network — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.protobuf_iot.sensor_readings WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.protobuf_iot.sensor_summary WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.protobuf_iot;
DROP ZONE IF EXISTS {{zone_name}};
