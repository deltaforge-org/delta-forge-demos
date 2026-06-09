-- ============================================================================
-- Iceberg V2 Fleet Telemetry — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg v2 table.
-- DeltaForge reads the Iceberg metadata chain directly:
-- metadata.json → manifest list → manifests → Parquet data files.
--
-- Iceberg v2 adds enhanced column-level statistics in manifests, which
-- enables more efficient query planning and predicate pushdown.
--
-- Dataset: 450 fleet telemetry GPS pings across 3 regional fleets
-- (West-Coast, Midwest, East-Coast) with 13 columns: vehicle_id, fleet,
-- vehicle_type, driver_id, latitude, longitude, speed_mph, fuel_level_pct,
-- engine_temp_f, odometer_miles, idle_minutes, harsh_braking, route_id.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v2 table
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- DeltaForge parses metadata.json to discover schema and data files automatically.
-- The format-version field in metadata.json is 2 (enhanced Iceberg spec).
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.fleet_telemetry
USING ICEBERG
LOCATION 'iceberg-v2-fleet-telemetry/fleet_telemetry';

