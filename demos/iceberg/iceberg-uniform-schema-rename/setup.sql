-- ============================================================================
-- Iceberg UniForm Column Rename (Field-ID Stability) — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table with legacy column names and seeds
-- 24 financial transactions. Column renames happen in queries.sql to
-- demonstrate how Iceberg field IDs remain stable across name changes.
--
-- Dataset: 24 transactions across 4 account types (checking, savings,
-- credit, investment) and 3 currencies (USD, EUR, GBP).
-- Columns: txn_id, acct_num, txn_type, amt, ccy, txn_date, branch_code.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm and column mapping (id mode required for rename)
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.financial_transactions (
    txn_id       INT,
    acct_num     VARCHAR,
    txn_type     VARCHAR,
    amt          DOUBLE,
    ccy          VARCHAR,
    txn_date     VARCHAR,
    branch_code  VARCHAR
) LOCATION '{{data_path}}/financial_transactions'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.financial_transactions TO USER {{current_user}};

-- STEP 3: Seed 24 financial transactions (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.financial_transactions VALUES
    (1,  'CHK-10001', 'checking',    1500.00, 'USD', '2024-01-05', 'BR-NYC'),
    (2,  'CHK-10001', 'checking',     250.75, 'USD', '2024-01-12', 'BR-NYC'),
    (3,  'SAV-20001', 'savings',     5000.00, 'USD', '2024-01-15', 'BR-NYC'),
    (4,  'SAV-20001', 'savings',     1200.00, 'EUR', '2024-01-20', 'BR-LON'),
    (5,  'CRD-30001', 'credit',        89.99, 'USD', '2024-01-22', 'BR-NYC'),
    (6,  'CRD-30001', 'credit',       320.50, 'GBP', '2024-01-25', 'BR-LON'),
    (7,  'INV-40001', 'investment', 10000.00, 'USD', '2024-02-01', 'BR-CHI'),
    (8,  'INV-40001', 'investment',  7500.00, 'EUR', '2024-02-05', 'BR-LON'),
    (9,  'CHK-10002', 'checking',     800.00, 'GBP', '2024-02-10', 'BR-LON'),
    (10, 'CHK-10002', 'checking',     450.25, 'USD', '2024-02-15', 'BR-NYC'),
    (11, 'SAV-20002', 'savings',     3000.00, 'EUR', '2024-02-18', 'BR-LON'),
    (12, 'SAV-20002', 'savings',     2200.00, 'GBP', '2024-02-22', 'BR-LON'),
    (13, 'CRD-30002', 'credit',       175.00, 'USD', '2024-02-25', 'BR-CHI'),
    (14, 'CRD-30002', 'credit',       540.00, 'EUR', '2024-03-01', 'BR-LON'),
    (15, 'INV-40002', 'investment',  8500.00, 'GBP', '2024-03-05', 'BR-LON'),
    (16, 'INV-40002', 'investment',  6000.00, 'USD', '2024-03-08', 'BR-CHI'),
    (17, 'CHK-10003', 'checking',    1100.00, 'EUR', '2024-03-12', 'BR-LON'),
    (18, 'CHK-10003', 'checking',     675.50, 'GBP', '2024-03-15', 'BR-LON'),
    (19, 'SAV-20003', 'savings',     4500.00, 'USD', '2024-03-18', 'BR-NYC'),
    (20, 'SAV-20003', 'savings',     1800.00, 'GBP', '2024-03-22', 'BR-LON'),
    (21, 'CRD-30003', 'credit',       225.00, 'USD', '2024-03-25', 'BR-CHI'),
    (22, 'CRD-30003', 'credit',        99.99, 'GBP', '2024-03-28', 'BR-LON'),
    (23, 'INV-40003', 'investment', 12000.00, 'EUR', '2024-04-01', 'BR-LON'),
    (24, 'INV-40003', 'investment',  5500.00, 'USD', '2024-04-05', 'BR-CHI');
