-- ==========================================================================
-- Demo: Avro E-Commerce Orders — Logical Types & Nullable Unions
-- Feature: Avro logical types (date, timestamp-millis), nullable unions,
--          mixed compression codecs, multi-file reading
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'E-commerce order analytics with Avro logical types';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.ecommerce
    COMMENT 'Online retail order data — Q1/Q2 2025';

-- --------------------------------------------------------------------------
-- Tables
-- --------------------------------------------------------------------------

-- All orders: reads both Q1 (null codec) and Q2 (deflate codec) files
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ecommerce.all_orders
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

-- Q1 only: file_filter isolates the first quarter
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ecommerce.q1_orders
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = '*q1*',
    file_metadata = '{"columns":["df_file_name"]}'
);

-- Sample: max_rows limits each file to 10 rows
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.ecommerce.sample_orders
USING AVRO
LOCATION '{{data_path}}'
OPTIONS (
    max_rows = '10'
);

-- --------------------------------------------------------------------------
-- Permissions (schema discovery runs automatically on CREATE EXTERNAL TABLE)
-- --------------------------------------------------------------------------

GRANT ADMIN ON TABLE {{zone_name}}.ecommerce.all_orders TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.ecommerce.q1_orders TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.ecommerce.sample_orders TO USER {{current_user}};
