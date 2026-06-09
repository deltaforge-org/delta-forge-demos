-- ============================================================================
-- Iceberg V1 Warehouse Inventory — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg v1 table.
-- DeltaForge reads the Iceberg metadata chain directly:
-- metadata.json → manifest list → manifests → Parquet data files.
--
-- Dataset: 489 warehouse inventory SKUs across 3 warehouses (Portland-OR,
-- Dallas-TX, Charlotte-NC) and 5 categories (Electronics, Furniture,
-- Apparel, Food-Bev, Industrial) with 10 columns: sku, product_name,
-- category, warehouse, quantity_on_hand, reorder_point, unit_cost,
-- last_restock_date, supplier, aisle_location.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v1 table
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- DeltaForge parses metadata.json to discover schema and data files automatically.
-- The format-version field in metadata.json is 1 (original Iceberg spec).
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.warehouse_inventory
USING ICEBERG
LOCATION 'iceberg-v1-warehouse-inventory/warehouse_inventory';

