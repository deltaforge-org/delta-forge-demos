-- ============================================================================
-- Iceberg V2 Complex Types — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg v2 table with
-- complex/nested column types: STRUCT and ARRAY<STRUCT>.
--
-- Scenario: E-Commerce Order Processing — 100 orders with nested product
-- items (array of structs) and shipping address (struct). Tests Iceberg's
-- support for complex/nested column types through the native reader.
--
-- Dataset: 100 orders with 8 columns including:
--   - shipping_address: STRUCT<street, city, state, zip_code>
--   - items: ARRAY<STRUCT<product_name, quantity, unit_price>>
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v2 table
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- Delta Forge parses metadata.json to discover schema and data files automatically.
-- The table contains STRUCT and ARRAY<STRUCT> columns for nested data.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg.orders
USING ICEBERG
LOCATION '{{data_path}}/orders';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg.orders TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg.orders;
