-- ============================================================================
-- Iceberg V3 Deletion Vectors (Puffin) — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg v3 table that
-- uses Puffin-encoded deletion vectors for row-level deletes.
--
-- DeltaForge reads the Iceberg metadata chain directly:
-- v3.metadata.json → manifest list → manifests → Parquet data file
-- and applies the Puffin deletion vector to retract invalidated rows.
--
-- Iceberg v3 is the latest format specification, generated here by
-- Apache Spark 4.0 with Iceberg 1.10.1. V3 introduces deletion vectors
-- as a native row-level delete mechanism using Puffin files that encode
-- row-position bitmaps (Roaring Bitmaps).
--
-- Dataset: 540 supply-chain shipment manifests across 3 regions
-- (Americas, EMEA, APAC). A faulty barcode scanner (SCAN-ERR) produced
-- 36 corrupt records that were retracted via a Puffin deletion vector,
-- leaving 504 valid shipments.
--
-- Columns: shipment_id, region, carrier, product_category, scanner_id,
-- weight_kg, declared_value, is_hazardous, destination_country, ship_date.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v3 table with deletion vectors
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- DeltaForge parses v3.metadata.json to discover schema, data files, and the
-- Puffin deletion vector. The format-version field in metadata.json is 3.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.shipment_manifests
USING ICEBERG
LOCATION 'iceberg-v3-deletion-vectors/shipment_manifests';

