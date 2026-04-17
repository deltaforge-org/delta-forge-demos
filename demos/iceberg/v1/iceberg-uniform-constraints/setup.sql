-- ==========================================================================
-- Demo: Bank Transaction Validation — CHECK Constraints with UniForm
-- Feature: delta.constraints.* on UniForm Iceberg tables
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos COMMENT 'CHECK constraints with UniForm';

-- --------------------------------------------------------------------------
-- Transactions Table — Constraints + UniForm
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.transactions (
    txn_id        INT,
    account_id    VARCHAR,
    txn_type      VARCHAR,
    amount        DECIMAL(12,2),
    balance_after DECIMAL(12,2),
    currency      VARCHAR,
    txn_date      DATE
) LOCATION '{{data_path}}/transactions'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id',
    'delta.constraints.positive_amount' = 'amount > 0',
    'delta.constraints.valid_currency' = 'currency IN (''USD'', ''EUR'', ''GBP'')'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.transactions TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- Seed Data — 25 transactions across 5 accounts, 3 currencies
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.iceberg_demos.transactions VALUES
    (1,  'ACC-1001', 'deposit',    5000.00,  5000.00,  'USD', '2025-02-01'),
    (2,  'ACC-1001', 'withdrawal', 1200.00,  3800.00,  'USD', '2025-02-02'),
    (3,  'ACC-1002', 'deposit',    8500.00,  8500.00,  'EUR', '2025-02-01'),
    (4,  'ACC-1002', 'transfer',   2000.00,  6500.00,  'EUR', '2025-02-03'),
    (5,  'ACC-1003', 'deposit',    3200.00,  3200.00,  'GBP', '2025-02-01'),
    (6,  'ACC-1003', 'withdrawal', 800.00,   2400.00,  'GBP', '2025-02-04'),
    (7,  'ACC-1001', 'deposit',    2500.00,  6300.00,  'USD', '2025-02-05'),
    (8,  'ACC-1004', 'deposit',    12000.00, 12000.00, 'USD', '2025-02-01'),
    (9,  'ACC-1004', 'transfer',   3500.00,  8500.00,  'USD', '2025-02-06'),
    (10, 'ACC-1005', 'deposit',    1500.00,  1500.00,  'EUR', '2025-02-01'),
    (11, 'ACC-1005', 'withdrawal', 500.00,   1000.00,  'EUR', '2025-02-07'),
    (12, 'ACC-1001', 'transfer',   1000.00,  5300.00,  'USD', '2025-02-08'),
    (13, 'ACC-1002', 'deposit',    3000.00,  9500.00,  'EUR', '2025-02-09'),
    (14, 'ACC-1003', 'deposit',    4500.00,  6900.00,  'GBP', '2025-02-10'),
    (15, 'ACC-1004', 'withdrawal', 2000.00,  6500.00,  'USD', '2025-02-11'),
    (16, 'ACC-1005', 'transfer',   300.00,   700.00,   'EUR', '2025-02-12'),
    (17, 'ACC-1001', 'withdrawal', 800.00,   4500.00,  'USD', '2025-02-13'),
    (18, 'ACC-1002', 'withdrawal', 1500.00,  8000.00,  'EUR', '2025-02-14'),
    (19, 'ACC-1003', 'transfer',   1200.00,  5700.00,  'GBP', '2025-02-15'),
    (20, 'ACC-1004', 'deposit',    5000.00,  11500.00, 'USD', '2025-02-16'),
    (21, 'ACC-1005', 'deposit',    2200.00,  2900.00,  'EUR', '2025-02-17'),
    (22, 'ACC-1001', 'deposit',    3500.00,  8000.00,  'USD', '2025-02-18'),
    (23, 'ACC-1002', 'transfer',   4000.00,  4000.00,  'EUR', '2025-02-19'),
    (24, 'ACC-1003', 'withdrawal', 700.00,   5000.00,  'GBP', '2025-02-20'),
    (25, 'ACC-1004', 'transfer',   1500.00,  10000.00, 'USD', '2025-02-21');
