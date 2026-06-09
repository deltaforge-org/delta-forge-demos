-- ============================================================================
-- Northwind Trading Company — Setup Script
-- ============================================================================
-- Provisions the Northwind sample database as 11 external tables for
-- cross-table queries: joins, aggregations, and business analytics.
--
-- Variables (auto-injected by DeltaForge):
--   {{data_path}}     — Local or cloud path where demo data files were downloaded
--   {{current_user}}  — Username of the current logged-in user
--   {{zone_name}}     — Target zone name (defaults to 'external')
--
-- What this script does:
--   1. Creates the target zone (defaults to 'external')
--   2. Creates the '{{zone_name}}.csv' schema (named after the file format)
--   3. Creates 11 external tables from semicolon-delimited CSV files
--   4. Detects schema for all tables
--   5. Grants read access on each table to the current user
--
-- See queries.sql for cross-table demo queries.
--
-- Naming convention: zone_name.format.table
--   zone   = {{zone_name}}  (defaults to 'external')
--   schema = 'csv'          (the file format)
--   table  = object name    (e.g. customers, orders)
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
-- STEP 3: External Tables
-- ============================================================================
-- Each table reads from a semicolon-delimited CSV file. All names are fully
-- qualified: {{zone_name}}.csv_demos.<table_name>
-- ============================================================================

-- CUSTOMERS — 91 customer companies with contact and address details
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_customers
USING CSV
LOCATION 'northwind-database/customers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- EMPLOYEES — 9 sales employees with hire dates and reporting hierarchy
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_employees
USING CSV
LOCATION 'northwind-database/employees.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- ORDERS — 830 customer orders with dates, shipping info, and freight costs
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_orders
USING CSV
LOCATION 'northwind-database/orders.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- ORDER_DETAILS — 2,155 line items linking orders to products with pricing
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_order_details
USING CSV
LOCATION 'northwind-database/order_details.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- PRODUCTS — 77 products with pricing, stock levels, and reorder thresholds
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_products
USING CSV
LOCATION 'northwind-database/products.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- CATEGORIES — 8 product categories (Beverages, Condiments, Seafood, etc.)
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_categories
USING CSV
LOCATION 'northwind-database/categories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- SUPPLIERS — 29 product suppliers with contact and location details
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_suppliers
USING CSV
LOCATION 'northwind-database/suppliers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- SHIPPERS — 3 shipping companies used for order delivery
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_shippers
USING CSV
LOCATION 'northwind-database/shippers.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- REGIONS — 4 geographic sales regions (Eastern, Western, Northern, Southern)
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_regions
USING CSV
LOCATION 'northwind-database/regions.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- TERRITORIES — 53 sales territories linked to regions
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_territories
USING CSV
LOCATION 'northwind-database/territories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);

-- EMPLOYEE_TERRITORIES — Maps employees to the territories they cover
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.nw_employee_territories
USING CSV
LOCATION 'northwind-database/employee_territories.csv'
OPTIONS (
    header = 'true',
    delimiter = ';'
);
