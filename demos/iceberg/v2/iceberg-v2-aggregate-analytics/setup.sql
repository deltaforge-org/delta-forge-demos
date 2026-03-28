-- ============================================================================
-- Iceberg V2 — Retail Multi-Dimensional Aggregation — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg V2 table.
--
-- Dataset: 120 retail transactions across 4 stores in 3 regions (East,
-- West, Central) and 5 product categories (Electronics, Clothing,
-- Home & Garden, Sports, Grocery) with 11 columns including quantity,
-- unit_price, discount_pct, and is_return flag.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg V2 table
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg.retail_sales
USING ICEBERG
LOCATION '{{data_path}}/retail_sales';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg.retail_sales TO USER {{current_user}};
