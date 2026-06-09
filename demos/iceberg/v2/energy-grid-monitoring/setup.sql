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


-- STEP 3: UniForm-enabled Delta table for the cross-format readback test
-- The demo verifies that bulk-loading from a native Iceberg table into a
-- UniForm Delta table produces correct iceberg metadata, by reading the
-- result back through an external Iceberg view.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.grid_readings_delta (
    meter_id           VARCHAR,
    region             VARCHAR,
    substation         VARCHAR,
    meter_type         VARCHAR,
    reading_timestamp  VARCHAR,
    voltage            INT,
    current_amps       DOUBLE,
    power_kw           DOUBLE,
    energy_kwh         DOUBLE,
    power_factor       INT,
    grid_frequency_hz  DOUBLE
) LOCATION 'energy-grid-monitoring/grid_readings_delta'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);


-- STEP 4: Bulk-load the native iceberg dataset into the UniForm Delta table
-- (UniForm writes the iceberg metadata as a side effect of this commit)
INSERT INTO {{zone_name}}.iceberg_demos.grid_readings_delta
SELECT * FROM {{zone_name}}.iceberg_demos.grid_readings;


-- STEP 5: External Iceberg view over the UniForm Delta location
-- queries.sql reads through this view to verify UniForm shadow metadata
-- correctly represents the Delta state.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.grid_readings_iceberg_readback
USING ICEBERG
LOCATION 'energy-grid-monitoring/grid_readings_delta';

