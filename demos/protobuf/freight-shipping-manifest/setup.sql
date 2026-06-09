-- ==========================================================================
-- Demo: Freight Shipping Manifest
-- Feature: bool, int64, float, 3-level nesting, multiple enums, multi-repeated
-- ==========================================================================
-- Creates three external tables from 3 protobuf binary files (.pb):
--   1. shipments          — one row per shipment (12 rows), packages/tracking joined
--   2. shipment_packages  — exploded: one row per package (24 rows) with dimensions
--   3. shipment_tracking  — exploded: one row per tracking event (39 rows)
--
-- Demonstrates:
--   - Proto3 binary format: schema-driven reading from .proto definitions
--   - Boolean fields: is_express, is_insured, requires_signature
--   - Int64 fields: total_cost_cents, declared_value_cents (monetary values)
--   - Float fields: weight_kg, length_cm, width_cm, height_cm
--   - 3-level nesting: Shipment → Package → Dimensions
--   - Multiple enums: ShipmentStatus and PackageClass decoded to strings
--   - Multiple repeated fields: packages + tracking in same message
--   - Multi-file reading: 3 carrier files merged into each table
--   - File metadata: df_file_name, df_row_number for traceability
--   - Column mappings: proto field paths → friendly column names
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.protobuf_freight
    COMMENT 'Protobuf-backed freight shipping external tables';

-- ==========================================================================
-- TABLE 1: shipments — One row per shipment (12 total)
-- ==========================================================================
-- Each Shipment message becomes a row. Repeated Package and TrackingEvent
-- messages are joined into comma-separated strings. Enums are decoded to
-- string labels. Bool fields appear as true/false. Int64 fields carry
-- monetary values in cents.
-- ==========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.protobuf_freight.shipments
USING PROTOBUF
LOCATION '{{data_path}}'
OPTIONS (
    schema_path = '{{data_path}}/schema/freight.proto',
    message_name = 'freight.ShippingManifest',
    proto_flatten_config = '{
        "row_path": "shipments",
        "include_paths": [
            "shipments.shipment_id",
            "shipments.origin",
            "shipments.destination",
            "shipments.status",
            "shipments.is_express",
            "shipments.is_insured",
            "shipments.total_cost_cents",
            "shipments.packages.package_id",
            "shipments.packages.description",
            "shipments.tracking.location",
            "shipments.created_at"
        ],
        "default_repeat_handling": "join_comma",
        "column_mappings": {
            "shipments.shipment_id": "shipment_id",
            "shipments.origin": "origin",
            "shipments.destination": "destination",
            "shipments.status": "status",
            "shipments.is_express": "is_express",
            "shipments.is_insured": "is_insured",
            "shipments.total_cost_cents": "total_cost_cents",
            "shipments.packages.package_id": "package_ids",
            "shipments.packages.description": "package_descriptions",
            "shipments.tracking.location": "tracking_locations",
            "shipments.created_at": "created_at"
        },
        "decode_enums": true,
        "timestamp_format": "iso8601",
        "separator": "_",
        "max_depth": 10
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);

-- ==========================================================================
-- TABLE 2: shipment_packages — One row per package (24 total)
-- ==========================================================================
-- Exploded view: each Package within each Shipment becomes its own row.
-- Shipment-level fields are duplicated per package row. Dimensions are
-- extracted from the 3rd nesting level (Shipment → Package → Dimensions).
-- ==========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.protobuf_freight.shipment_packages
USING PROTOBUF
LOCATION '{{data_path}}'
OPTIONS (
    schema_path = '{{data_path}}/schema/freight.proto',
    message_name = 'freight.ShippingManifest',
    proto_flatten_config = '{
        "row_path": "shipments",
        "explode_paths": ["shipments.packages"],
        "include_paths": [
            "shipments.shipment_id",
            "shipments.status",
            "shipments.is_express",
            "shipments.packages.package_id",
            "shipments.packages.description",
            "shipments.packages.weight_kg",
            "shipments.packages.dimensions.length_cm",
            "shipments.packages.dimensions.width_cm",
            "shipments.packages.dimensions.height_cm",
            "shipments.packages.package_class",
            "shipments.packages.requires_signature",
            "shipments.packages.declared_value_cents"
        ],
        "column_mappings": {
            "shipments.shipment_id": "shipment_id",
            "shipments.status": "shipment_status",
            "shipments.is_express": "is_express",
            "shipments.packages.package_id": "package_id",
            "shipments.packages.description": "description",
            "shipments.packages.weight_kg": "weight_kg",
            "shipments.packages.dimensions.length_cm": "length_cm",
            "shipments.packages.dimensions.width_cm": "width_cm",
            "shipments.packages.dimensions.height_cm": "height_cm",
            "shipments.packages.package_class": "package_class",
            "shipments.packages.requires_signature": "requires_signature",
            "shipments.packages.declared_value_cents": "declared_value_cents"
        },
        "decode_enums": true,
        "timestamp_format": "iso8601",
        "separator": "_",
        "max_depth": 10
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);

-- ==========================================================================
-- TABLE 3: shipment_tracking — One row per tracking event (39 total)
-- ==========================================================================
-- Exploded view: each TrackingEvent within each Shipment becomes its own row.
-- Demonstrates the second repeated field (tracking) alongside packages.
-- ==========================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.protobuf_freight.shipment_tracking
USING PROTOBUF
LOCATION '{{data_path}}'
OPTIONS (
    schema_path = '{{data_path}}/schema/freight.proto',
    message_name = 'freight.ShippingManifest',
    proto_flatten_config = '{
        "row_path": "shipments",
        "explode_paths": ["shipments.tracking"],
        "include_paths": [
            "shipments.shipment_id",
            "shipments.origin",
            "shipments.destination",
            "shipments.status",
            "shipments.tracking.event_time",
            "shipments.tracking.location",
            "shipments.tracking.description"
        ],
        "column_mappings": {
            "shipments.shipment_id": "shipment_id",
            "shipments.origin": "origin",
            "shipments.destination": "destination",
            "shipments.status": "shipment_status",
            "shipments.tracking.event_time": "event_time",
            "shipments.tracking.location": "event_location",
            "shipments.tracking.description": "event_description"
        },
        "decode_enums": true,
        "timestamp_format": "iso8601",
        "separator": "_",
        "max_depth": 10
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
