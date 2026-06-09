-- ============================================================================
-- Iceberg V2 Multi-Partition Weather Readings — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg v2 table
-- with multi-column partitioning: region (identity) + years(observation_date).
-- DeltaForge reads the Iceberg metadata chain directly:
-- metadata.json -> manifest list -> manifests -> Parquet data files.
--
-- Multi-column partitioning enables partition pruning on both the region
-- string and the year extracted from observation_date, reducing I/O for
-- queries that filter on either or both dimensions.
--
-- Dataset: 450 global weather station readings across 5 regions
-- (North America, Europe, Asia, South America, Africa) and 3 years
-- (2023, 2024, 2025). 15 partitions total (5 regions x 3 years).
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v2 table
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- DeltaForge parses metadata.json to discover schema and data files automatically.
-- The table is partitioned by region (identity) and years(observation_date).
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.weather_readings
USING ICEBERG
LOCATION 'iceberg-v2-multi-partition/weather_readings';

