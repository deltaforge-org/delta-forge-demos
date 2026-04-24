-- ============================================================================
-- Iceberg Energy Grid Monitoring — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg table
-- (format v2). DeltaForge reads the Iceberg metadata chain directly:
-- metadata.json → manifest list → manifests → Parquet data files.
--
-- Dataset: 600 smart meter readings across 3 regions (North, South, East)
-- with 11 columns: meter_id, region, substation, meter_type,
-- reading_timestamp, voltage, current_amps, power_kw, energy_kwh,
-- power_factor, grid_frequency_hz.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg table
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- DeltaForge parses metadata.json to discover schema and data files automatically.
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.grid_readings
USING ICEBERG
LOCATION '{{data_path}}';

