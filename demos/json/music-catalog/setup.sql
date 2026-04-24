-- ============================================================================
-- JSON Music Catalog — Setup Script
-- ============================================================================
-- Creates two external tables from 3 genre-based music catalog JSON files:
--   1. album_tracks  — exploded: one row per track (3,503 rows)
--   2. album_summary — one row per album, tracks counted, vendor as JSON (347 rows)
--
-- Demonstrates:
--   - Nested object flattening ($.vendor.id → vendor_id, $.vendor.name → vendor_name, etc.)
--   - explode_paths: $.details array → one row per track
--   - json_paths: keep $.vendor as JSON blob (not flattened)
--   - column_mappings: deep paths → friendly names
--   - default_array_handling: count (track count per album in summary)
--   - Multi-file reading (3 JSON files in one directory)
--   - file_metadata: df_file_name to identify source catalog file
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.json_demos
    COMMENT 'JSON-backed external tables';

-- ============================================================================
-- TABLE 1: album_tracks — Exploded, one row per track (3,503 total)
-- ============================================================================
-- Each track in each album's details[] array becomes its own row. Album-level
-- fields (id, name, sku, vendor, price, status) are duplicated on each row.
-- The vendor nested object is flattened to vendor_id and vendor_name.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json_demos.album_tracks
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "explode_paths": ["$.details"],
        "include_paths": [
            "$.id",
            "$.name",
            "$.sku",
            "$.status",
            "$.price",
            "$.taxable",
            "$.vendor.id",
            "$.vendor.name",
            "$.details.track_id",
            "$.details.name",
            "$.details.composer",
            "$.details.genre_id",
            "$.details.milliseconds",
            "$.details.bytes",
            "$.details.unit_price"
        ],
        "column_mappings": {
            "$.id": "id",
            "$.name": "name",
            "$.vendor.id": "vendor_id",
            "$.vendor.name": "vendor_name",
            "$.details.track_id": "details_track_id",
            "$.details.name": "details_name",
            "$.details.genre_id": "details_genre_id",
            "$.details.milliseconds": "details_milliseconds",
            "$.details.bytes": "details_bytes",
            "$.details.unit_price": "details_unit_price"
        },
        "max_depth": 10,
        "separator": "_",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: album_summary — One row per album (347 total)
-- ============================================================================
-- Non-exploded view: one row per album. The details[] array is counted (not
-- flattened). The vendor subtree is kept as a JSON string blob via json_paths,
-- useful for downstream JSON processing or API responses.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json_demos.album_summary
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.id",
            "$.name",
            "$.sku",
            "$.status",
            "$.price",
            "$.taxable",
            "$.requires_shipping",
            "$.vendor",
            "$.details"
        ],
        "json_paths": ["$.vendor"],
        "column_mappings": {
            "$.id": "id",
            "$.name": "name"
        },
        "default_array_handling": "count",
        "max_depth": 10,
        "separator": "_",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
