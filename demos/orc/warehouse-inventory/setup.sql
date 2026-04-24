-- ==========================================================================
-- Demo: ORC Warehouse Inventory — Stock Level Analytics
-- Feature: Window functions on ORC data with mixed numeric types
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.orc_inventory
    COMMENT 'ORC-backed warehouse inventory tables';

-- --------------------------------------------------------------------------
-- Table: stock — Both warehouses (100 rows)
-- --------------------------------------------------------------------------
-- Reads 2 ORC files (WH-NORTH, WH-SOUTH) with mixed types:
--   string (sku_id, warehouse, category, product_name, last_restock_date)
--   int32 (quantity_on_hand, reorder_point)
--   float64 (unit_cost)
--   bool (is_active)
-- last_restock_date has ~14% NULLs for testing NULL-aware window functions.
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc_inventory.stock
USING ORC
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.orc_inventory.stock;
