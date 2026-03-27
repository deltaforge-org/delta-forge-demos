-- ============================================================================
-- Protobuf IoT Sensor Network — Setup Script
-- ============================================================================
-- Creates two external tables from 3 protobuf binary files (.pb):
--   1. sensor_readings  — exploded: one row per reading (82 rows)
--   2. sensor_summary   — joined: one row per sensor, readings comma-joined (20 rows)
--
-- Demonstrates:
--   - Proto3 binary format: schema-driven reading from .proto definitions
--   - Nested messages: Sensor.SensorReading flattened to top-level columns
--   - Repeated fields: readings as exploded rows vs. comma-joined values
--   - Well-known types: google.protobuf.Timestamp → ISO 8601 datetime
--   - Multi-file reading: 3 facility files merged into one table
--   - File metadata: df_file_name, df_row_number for traceability
--   - Column mappings: proto field paths → friendly column names
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.protobuf_iot
    COMMENT 'Protobuf-backed IoT sensor external tables';

-- ============================================================================
-- TABLE 1: sensor_readings — One row per reading (82 total)
-- ============================================================================
-- Each SensorReading within each Sensor becomes its own row. Sensor-level
-- fields (sensor_id, sensor_type, location, status) are duplicated per row.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.protobuf_iot.sensor_readings
USING PROTOBUF
LOCATION '{{data_path}}'
OPTIONS (
    schema_path = '{{data_path}}/schema/sensor.proto',
    message_name = 'iot.SensorNetwork',
    proto_flatten_config = '{
        "row_path": "sensors",
        "explode_paths": ["sensors.readings"],
        "include_paths": [
            "sensors.sensor_id",
            "sensors.sensor_type",
            "sensors.location",
            "sensors.status",
            "sensors.readings.value",
            "sensors.readings.recorded_at",
            "sensors.readings.unit",
            "sensors.installed_at"
        ],
        "column_mappings": {
            "sensors.sensor_id": "sensor_id",
            "sensors.sensor_type": "sensor_type",
            "sensors.location": "location",
            "sensors.status": "status",
            "sensors.readings.value": "reading_value",
            "sensors.readings.recorded_at": "recorded_at",
            "sensors.readings.unit": "unit",
            "sensors.installed_at": "installed_at"
        },
        "timestamp_format": "iso8601",
        "separator": "_",
        "max_depth": 10
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
GRANT ADMIN ON TABLE {{zone_name}}.protobuf_iot.sensor_readings TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.protobuf_iot.sensor_readings;


-- ============================================================================
-- TABLE 2: sensor_summary — One row per sensor (20 total)
-- ============================================================================
-- Each Sensor message becomes a row. Repeated SensorReading messages are
-- joined into comma-separated strings. Timestamps are converted to ISO 8601.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.protobuf_iot.sensor_summary
USING PROTOBUF
LOCATION '{{data_path}}'
OPTIONS (
    schema_path = '{{data_path}}/schema/sensor.proto',
    message_name = 'iot.SensorNetwork',
    proto_flatten_config = '{
        "row_path": "sensors",
        "include_paths": [
            "sensors.sensor_id",
            "sensors.sensor_type",
            "sensors.location",
            "sensors.status",
            "sensors.readings.value",
            "sensors.readings.recorded_at",
            "sensors.readings.unit",
            "sensors.installed_at"
        ],
        "default_repeat_handling": "join_comma",
        "column_mappings": {
            "sensors.sensor_id": "sensor_id",
            "sensors.sensor_type": "sensor_type",
            "sensors.location": "location",
            "sensors.status": "status",
            "sensors.readings.value": "reading_values",
            "sensors.readings.recorded_at": "reading_times",
            "sensors.readings.unit": "reading_units",
            "sensors.installed_at": "installed_at"
        },
        "timestamp_format": "iso8601",
        "separator": "_",
        "max_depth": 10
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
GRANT ADMIN ON TABLE {{zone_name}}.protobuf_iot.sensor_summary TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.protobuf_iot.sensor_summary;
