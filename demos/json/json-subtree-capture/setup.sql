-- ============================================================================
-- JSON Subtree Capture — Setup Script
-- ============================================================================
-- Creates two external tables from property listing JSON files (5 listings):
--   1. listings_captured  — location and pricing subtrees captured as JSON blobs
--   2. listings_flattened — same data with location and pricing fully flattened
--
-- Demonstrates:
--   - json_paths: keep complex subtrees as JSON string columns (not flattened)
--   - Combining json_paths with include_paths and column_mappings
--   - Multiple subtree captures per row (location + pricing)
--   - Contrast: captured vs flattened views of the same data
--   - Nested objects with arrays (pricing.tax_history) and sub-objects (location.geo)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.json_demos
    COMMENT 'JSON-backed external tables';

-- ============================================================================
-- TABLE 1: listings_captured — Subtrees captured as JSON strings
-- ============================================================================
-- The location and pricing subtrees are captured as JSON string columns via
-- json_paths. Top-level listing fields (id, title, type, bedrooms, etc.)
-- are flattened normally. This is useful when downstream consumers need the
-- full nested structure (e.g., map rendering, mortgage calculators).
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json_demos.listings_captured
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.id",
            "$.title",
            "$.type",
            "$.bedrooms",
            "$.bathrooms",
            "$.sqft",
            "$.year_built",
            "$.status",
            "$.location",
            "$.pricing",
            "$.tags"
        ],
        "json_paths": [
            "$.location",
            "$.pricing"
        ],
        "column_mappings": {
            "$.id": "id",
            "$.type": "type",
            "$.year_built": "year_built",
            "$.location": "location",
            "$.pricing": "pricing"
        },
        "max_depth": 10,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: listings_flattened — Same data, fully flattened (no json_paths)
-- ============================================================================
-- For comparison: the same location and pricing subtrees are fully flattened
-- into individual columns. This creates more columns but allows direct SQL
-- filtering on nested fields (e.g., WHERE city = 'Boston').
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json_demos.listings_flattened
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.id",
            "$.title",
            "$.type",
            "$.bedrooms",
            "$.bathrooms",
            "$.sqft",
            "$.year_built",
            "$.status",
            "$.location.address.street",
            "$.location.address.unit",
            "$.location.address.city",
            "$.location.address.state",
            "$.location.address.zip",
            "$.location.geo.lat",
            "$.location.geo.lng",
            "$.location.neighborhood",
            "$.location.walk_score",
            "$.pricing.list_price",
            "$.pricing.price_per_sqft",
            "$.pricing.hoa_monthly",
            "$.pricing.tax_annual",
            "$.pricing.mortgage_estimate.monthly_payment",
            "$.tags"
        ],
        "column_mappings": {
            "$.id": "id",
            "$.type": "type",
            "$.location.address.street": "location_address_street",
            "$.location.address.unit": "location_address_unit",
            "$.location.address.city": "location_address_city",
            "$.location.address.state": "location_address_state",
            "$.location.address.zip": "location_address_zip",
            "$.location.geo.lat": "location_geo_lat",
            "$.location.geo.lng": "location_geo_lng",
            "$.location.neighborhood": "location_neighborhood",
            "$.location.walk_score": "location_walk_score",
            "$.pricing.list_price": "pricing_list_price",
            "$.pricing.price_per_sqft": "pricing_price_per_sqft",
            "$.pricing.hoa_monthly": "pricing_hoa_monthly",
            "$.pricing.tax_annual": "pricing_tax_annual",
            "$.pricing.mortgage_estimate.monthly_payment": "pricing_mortgage_estimate_monthly_payment"
        },
        "max_depth": 10,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
