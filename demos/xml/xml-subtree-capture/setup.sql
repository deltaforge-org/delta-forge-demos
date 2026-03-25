-- ============================================================================
-- XML Subtree Capture — Setup Script
-- ============================================================================
-- Creates two external tables from product catalog XML files (5 products):
--   1. products_json — subtrees captured as JSON strings
--   2. products_xml  — subtrees captured as raw XML strings
--
-- Demonstrates:
--   - xml_paths: keep complex subtrees as serialized strings (not flattened)
--   - nested_output_format "json": subtrees serialized as JSON key-value pairs
--   - nested_output_format "xml": subtrees serialized as raw XML fragments
--   - Combining xml_paths with include_paths and column_mappings
--   - Multiple subtree captures in a single table (specifications + supplier)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.xml
    COMMENT 'XML-backed external tables';

-- ============================================================================
-- TABLE 1: products_json — Subtrees captured as JSON strings
-- ============================================================================
-- The specifications and supplier subtrees are captured as JSON strings via
-- xml_paths. Top-level product fields (id, name, category, price) are
-- flattened normally. This is useful when subtree contents need to be
-- parsed downstream (e.g., JSON functions in SQL).
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.xml.products_json
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    xml_flatten_config = '{
        "row_xpath": "//product",
        "include_paths": [
            "/catalog/product/@id",
            "/catalog/product/@status",
            "/catalog/product/name",
            "/catalog/product/category",
            "/catalog/product/price",
            "/catalog/product/price/@currency",
            "/catalog/product/specifications",
            "/catalog/product/supplier",
            "/catalog/product/tags"
        ],
        "xml_paths": [
            "/catalog/product/specifications",
            "/catalog/product/supplier"
        ],
        "column_mappings": {
            "/catalog/product/@id": "catalog_product_attr_id",
            "/catalog/product/@status": "catalog_product_attr_status",
            "/catalog/product/name": "catalog_product_name",
            "/catalog/product/category": "catalog_product_category",
            "/catalog/product/price": "catalog_product_price",
            "/catalog/product/price/@currency": "catalog_product_price_attr_currency",
            "/catalog/product/specifications": "catalog_product_specifications",
            "/catalog/product/supplier": "catalog_product_supplier",
            "/catalog/product/tags": "catalog_product_tags"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "nested_output_format": "json",
        "strip_namespace_prefixes": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
GRANT ADMIN ON TABLE {{zone_name}}.xml.products_json TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.xml.products_json;


-- ============================================================================
-- TABLE 2: products_xml — Subtrees captured as raw XML strings
-- ============================================================================
-- Same data, but the specifications and supplier subtrees are captured as
-- raw XML fragment strings. This preserves the original XML structure
-- including element names, nesting, and attributes.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.xml.products_xml
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    xml_flatten_config = '{
        "row_xpath": "//product",
        "include_paths": [
            "/catalog/product/@id",
            "/catalog/product/@status",
            "/catalog/product/name",
            "/catalog/product/category",
            "/catalog/product/price",
            "/catalog/product/price/@currency",
            "/catalog/product/specifications",
            "/catalog/product/supplier",
            "/catalog/product/tags"
        ],
        "xml_paths": [
            "/catalog/product/specifications",
            "/catalog/product/supplier"
        ],
        "column_mappings": {
            "/catalog/product/@id": "catalog_product_attr_id",
            "/catalog/product/@status": "catalog_product_attr_status",
            "/catalog/product/name": "catalog_product_name",
            "/catalog/product/category": "catalog_product_category",
            "/catalog/product/price": "catalog_product_price",
            "/catalog/product/price/@currency": "catalog_product_price_attr_currency",
            "/catalog/product/specifications": "catalog_product_specifications",
            "/catalog/product/supplier": "catalog_product_supplier",
            "/catalog/product/tags": "catalog_product_tags"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "nested_output_format": "xml",
        "strip_namespace_prefixes": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
GRANT ADMIN ON TABLE {{zone_name}}.xml.products_xml TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.xml.products_xml;
