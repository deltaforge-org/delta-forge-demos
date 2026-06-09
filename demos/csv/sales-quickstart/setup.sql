-- ============================================================================
-- Sales Schema Evolution Demo — Setup Script
-- ============================================================================
-- Demonstrates how DeltaForge handles CSV files whose columns change over
-- time: new columns get added, old columns get retired.  A single external
-- table spans 5 quarterly files (2024-Q1 → 2025-Q1); missing columns from
-- older files surface as NULL when queried together.
--
-- Evolution timeline:
--   Q1 2024  base schema   id, product_name, quantity, unit_price, sale_date, region
--   Q2 2024  + sales_rep
--   Q3 2024  + discount_pct
--   Q4 2024  - region (retired), + territory
--   Q1 2025  - discount_pct (retired), + channel
--
-- Variables (auto-injected by DeltaForge):
--   data_path     — Local or cloud path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--   zone_name     — Target zone name (defaults to 'external')
--
-- What this script does:
--   1. Creates the target zone (defaults to 'external')
--   2. Creates the '{{zone_name}}.csv' schema (named after the file format)
--   3. Creates one external table over the sales-evolution directory
--
-- Naming convention: zone_name.format.table
--   zone   = {{zone_name}}  (defaults to 'external')
--   schema = 'csv'          (the file format)
--   table  = object name
--
-- After running, try these queries:
--   SELECT * FROM {{zone_name}}.csv_demos.sales ORDER BY id;
--   SELECT id, sale_date, region, territory FROM {{zone_name}}.csv_demos.sales ORDER BY id;
--   SELECT sales_rep, SUM(quantity * unit_price) AS revenue
--   FROM {{zone_name}}.csv_demos.sales WHERE sales_rep IS NOT NULL
--   GROUP BY sales_rep ORDER BY revenue DESC;
-- ============================================================================
-- ============================================================================
-- STEP 1: Zone
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';
-- ============================================================================
-- STEP 2: Schema
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.csv_demos
    COMMENT 'CSV-backed external tables';
-- ============================================================================
-- STEP 3: External Table
-- ============================================================================

-- SALES — 15 records across 5 quarterly CSV files (2024-Q1 to 2025-Q1).
-- Each file has a slightly different set of columns:
--   sales_2024_q1.csv  6 cols  (no sales_rep, no territory, no channel)
--   sales_2024_q2.csv  7 cols  (+ sales_rep)
--   sales_2024_q3.csv  8 cols  (+ discount_pct)
--   sales_2024_q4.csv  8 cols  (region removed, + territory)
--   sales_2025_q1.csv  8 cols  (discount_pct removed, + channel)
-- DeltaForge unifies all schemas; missing columns appear as NULL.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.sales
USING CSV
LOCATION 'sales-quickstart/sales*.csv'
OPTIONS (
    header = 'true',
    file_metadata = '{"columns":["df_file_name","df_file_modified","df_dataset","df_row_number"]}'
);
