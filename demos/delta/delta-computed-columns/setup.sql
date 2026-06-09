-- ============================================================================
-- Delta Computed Fields — Setup Script
-- ============================================================================
-- Creates the zone, schema, and table for the computed columns demo.
-- The teaching content (CTE-based INSERTs and UPDATEs) is in queries.sql.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: sales_invoices
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sales_invoices (
    id           INT,
    item         VARCHAR,
    qty          INT,
    unit_price   DOUBLE,
    discount_pct DOUBLE,
    subtotal     DOUBLE,
    discount_amt DOUBLE,
    total        DOUBLE,
    sales_rep    VARCHAR,
    commission   DOUBLE,
    invoice_date VARCHAR
) LOCATION 'delta-computed-columns/sales_invoices';


