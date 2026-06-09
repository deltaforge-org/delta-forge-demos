-- ============================================================================
-- XML E-Commerce Order Line Explosion — Setup Script
-- ============================================================================
-- Creates two external tables from 2 daily order batch XML files:
--   1. order_lines   — exploded: one row per line item (11 rows)
--   2. order_summary — one row per order with item count (5 rows)
--
-- Demonstrates:
--   - Deep nesting (3+ levels): order/items/item/variant/color
--   - explode_paths: one row per <item> within each <order>
--   - CDATA sections: HTML-embedded product descriptions
--   - exclude_paths: internal_audit block hidden from analytics
--   - column_mappings: deep XPaths → friendly column names
--   - default_repeat_handling: count (items per order in summary)
--   - Self-closing elements: <gift_wrap/> and <express/> flags
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.xml_demos
    COMMENT 'XML-backed external tables';

-- ============================================================================
-- TABLE 1: order_lines — Exploded, one row per line item (11 total)
-- ============================================================================
-- Each <item> within each <order> becomes its own row. Order-level fields
-- (order_id, status, customer_name, order_date) are duplicated on each row.
-- The internal_audit block is excluded. Deep variant paths are mapped to
-- friendly names (item_size, item_color).
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.xml_demos.order_lines
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    xml_flatten_config = '{
        "row_xpath": "//order",
        "explode_paths": ["/orders/order/items/item"],
        "include_paths": [
            "/orders/order/@id",
            "/orders/order/@status",
            "/orders/order/customer/name",
            "/orders/order/customer/tier",
            "/orders/order/order_date",
            "/orders/order/items/item/@sku",
            "/orders/order/items/item/product",
            "/orders/order/items/item/description",
            "/orders/order/items/item/quantity",
            "/orders/order/items/item/unit_price",
            "/orders/order/items/item/variant/size",
            "/orders/order/items/item/variant/color",
            "/orders/order/gift_wrap",
            "/orders/order/express",
            "/orders/order/shipping_total"
        ],
        "exclude_paths": ["/orders/order/internal_audit"],
        "column_mappings": {
            "/orders/order/@id": "orders_order_attr_id",
            "/orders/order/@status": "orders_order_attr_status",
            "/orders/order/customer/name": "orders_order_customer_name",
            "/orders/order/customer/tier": "orders_order_customer_tier",
            "/orders/order/order_date": "orders_order_order_date",
            "/orders/order/items/item/@sku": "orders_order_items_item_attr_sku",
            "/orders/order/items/item/product": "orders_order_items_item_product",
            "/orders/order/items/item/description": "orders_order_items_item_description",
            "/orders/order/items/item/quantity": "orders_order_items_item_quantity",
            "/orders/order/items/item/unit_price": "orders_order_items_item_unit_price",
            "/orders/order/items/item/variant/size": "orders_order_items_item_variant_size",
            "/orders/order/items/item/variant/color": "orders_order_items_item_variant_color",
            "/orders/order/gift_wrap": "orders_order_gift_wrap",
            "/orders/order/express": "orders_order_express",
            "/orders/order/shipping_total": "orders_order_shipping_total"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "strip_namespace_prefixes": true
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: order_summary — One row per order (5 total)
-- ============================================================================
-- Non-exploded view: one row per <order>. Repeating <item> elements are
-- counted (not flattened). Customer fields are flattened normally.
-- Internal audit is excluded.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.xml_demos.order_summary
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    xml_flatten_config = '{
        "row_xpath": "//order",
        "include_paths": [
            "/orders/order/@id",
            "/orders/order/@status",
            "/orders/order/customer/name",
            "/orders/order/customer/email",
            "/orders/order/customer/tier",
            "/orders/order/order_date",
            "/orders/order/items/item",
            "/orders/order/gift_wrap",
            "/orders/order/express",
            "/orders/order/shipping_total"
        ],
        "exclude_paths": ["/orders/order/internal_audit"],
        "default_repeat_handling": "count",
        "column_mappings": {
            "/orders/order/@id": "orders_order_attr_id",
            "/orders/order/@status": "orders_order_attr_status",
            "/orders/order/customer/name": "orders_order_customer_name",
            "/orders/order/customer/email": "orders_order_customer_email",
            "/orders/order/customer/tier": "orders_order_customer_tier",
            "/orders/order/order_date": "orders_order_order_date",
            "/orders/order/items/item": "orders_order_items_item",
            "/orders/order/gift_wrap": "orders_order_gift_wrap",
            "/orders/order/express": "orders_order_express",
            "/orders/order/shipping_total": "orders_order_shipping_total"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "strip_namespace_prefixes": true
    }',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
