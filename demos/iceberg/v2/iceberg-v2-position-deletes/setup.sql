-- ============================================================================
-- Iceberg V2 Position Deletes — Setup
-- ============================================================================
-- Creates an external table backed by an Iceberg format-version 2 table that
-- contains position delete files. DeltaForge must parse the full metadata
-- chain (metadata.json → manifest list → data manifest + delete manifest →
-- data Parquet + delete Parquet) and apply row-level exclusions at query time.
--
-- Scenario: Pharmaceutical cold-chain monitoring — 600 temperature sensor
-- readings across 4 vaccine shipment routes. 30 readings from faulty sensor
-- SENSOR-F01 were retracted via V2 position deletes, leaving 570 valid rows.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v2 table with position deletes
-- The table root contains both data/ and metadata/ directories.
-- metadata.json v3 has the current snapshot with delete manifest references.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.cold_chain_readings
USING ICEBERG
LOCATION 'iceberg-v2-position-deletes/cold_chain_readings';

