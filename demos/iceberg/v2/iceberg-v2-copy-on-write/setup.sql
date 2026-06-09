-- ============================================================================
-- Iceberg V2 Copy-on-Write — Setup
-- ============================================================================
-- Creates an external table backed by an Iceberg format-version 2 table that
-- uses copy-on-write mode for updates and deletes. Unlike merge-on-read,
-- copy-on-write rewrites entire data files — there are NO delete files.
-- DeltaForge reads clean data files directly from the current snapshot.
--
-- Scenario: Logistics shipment tracking — 120 shipments initially loaded,
-- 20 updated from "In Transit" to "Delivered", 10 cancelled shipments
-- deleted. Final state: 110 shipments across a single rewritten data file.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v2 copy-on-write table
-- The table root contains data/ and metadata/ directories.
-- v4.metadata.json has 3 snapshots: append (120 rows), overwrite (update 20),
-- overwrite (delete 10). Only one data file is current — no delete files.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.shipments
USING ICEBERG
LOCATION 'iceberg-v2-copy-on-write/shipments';

