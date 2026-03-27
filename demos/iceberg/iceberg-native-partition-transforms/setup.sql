-- ============================================================================
-- Iceberg Native Partition Transforms — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg v2 table
-- using Iceberg-native partition transforms:
--   bucket(8, source_ip)  — hash-based bucketing on source IP address
--   days(capture_time)    — daily partitioning on capture timestamp
--
-- These transforms are Iceberg-specific (Delta Lake cannot produce them),
-- making this a pure native-Iceberg read test.
--
-- Delta Forge reads the Iceberg metadata chain directly:
-- metadata.json → manifest list → manifests → Parquet data files.
-- The partition spec in metadata.json describes the bucket and day transforms.
--
-- Dataset: 480 network traffic packet records across 3 regions
-- (north-america, europe, asia-pacific) with 9 columns: packet_id, source_ip,
-- dest_ip, protocol, port, bytes_transferred, threat_level, capture_time, region.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v2 table with partition transforms
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- Delta Forge parses metadata.json to discover schema, partition spec, and data files.
-- The partition-spec uses bucket(8, source_ip) and days(capture_time).
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg.network_traffic
USING ICEBERG
LOCATION '{{data_path}}/network_traffic';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg.network_traffic TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg.network_traffic;
