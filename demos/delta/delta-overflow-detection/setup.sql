-- ============================================================================
-- Delta Overflow Detection — Treasury Balance Monitoring — Setup Script
-- ============================================================================
-- Creates a transaction ledger with INT columns and inserts 30 baseline
-- transactions across 5 accounts of varying scale. The overflow detection
-- queries, type widening, and BIGINT-range inserts happen in queries.sql.
--
-- Real-world scenario: A treasury management platform tracks balances for
-- accounts ranging from small business ($4.5M) to sovereign wealth funds
-- ($2B). As balances grow, they approach the INT limit (2,147,483,647).
-- The system must detect approaching overflow and widen types proactively.
--
-- Tables created:
--   1. transaction_ledger — 30 transactions across 5 account tiers
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: transaction_ledger — multi-tier treasury balances
-- ============================================================================
-- amount and running_balance start as INT. ACCT-5001 (sovereign fund)
-- has a balance of $2.05B — 95.5% of INT max. Growth will overflow.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.transaction_ledger (
    id              INT,
    account_id      VARCHAR,
    tx_type         VARCHAR,
    amount          INT,
    running_balance INT,
    description     VARCHAR,
    tx_date         VARCHAR
) LOCATION 'delta-overflow-detection/transaction_ledger';


-- STEP 2: Insert 30 baseline transactions (all values fit in INT)
INSERT INTO {{zone_name}}.delta_demos.transaction_ledger VALUES
    (1,  'ACCT-1001', 'deposit',    500000,     500000,     'Initial deposit',        '2025-01-01'),
    (2,  'ACCT-1001', 'deposit',    750000,     1250000,    'Wire transfer in',       '2025-01-05'),
    (3,  'ACCT-1001', 'withdrawal', -120000,    1130000,    'Operating expenses',     '2025-01-10'),
    (4,  'ACCT-1001', 'deposit',    2000000,    3130000,    'Revenue collection',     '2025-01-15'),
    (5,  'ACCT-1001', 'withdrawal', -450000,    2680000,    'Payroll',                '2025-01-20'),
    (6,  'ACCT-2001', 'deposit',    1000000,    1000000,    'Seed funding',           '2025-01-01'),
    (7,  'ACCT-2001', 'deposit',    3000000,    4000000,    'Series A tranche',       '2025-01-15'),
    (8,  'ACCT-2001', 'withdrawal', -800000,    3200000,    'Infrastructure spend',   '2025-02-01'),
    (9,  'ACCT-2001', 'deposit',    5000000,    8200000,    'Series A remainder',     '2025-02-15'),
    (10, 'ACCT-2001', 'withdrawal', -1500000,   6700000,    'Hiring costs',           '2025-03-01'),
    (11, 'ACCT-3001', 'deposit',    10000000,   10000000,   'Fund allocation',        '2025-01-01'),
    (12, 'ACCT-3001', 'deposit',    25000000,   35000000,   'Quarterly inflow',       '2025-01-15'),
    (13, 'ACCT-3001', 'withdrawal', -8000000,   27000000,   'Portfolio rebalance',    '2025-02-01'),
    (14, 'ACCT-3001', 'deposit',    50000000,   77000000,   'Large client deposit',   '2025-02-15'),
    (15, 'ACCT-3001', 'withdrawal', -15000000,  62000000,   'Fund distribution',      '2025-03-01'),
    (16, 'ACCT-4001', 'deposit',    100000000,  100000000,  'Treasury allocation',    '2025-01-01'),
    (17, 'ACCT-4001', 'deposit',    200000000,  300000000,  'Bond proceeds',          '2025-01-15'),
    (18, 'ACCT-4001', 'withdrawal', -50000000,  250000000,  'Capital expenditure',    '2025-02-01'),
    (19, 'ACCT-4001', 'deposit',    400000000,  650000000,  'Asset liquidation',      '2025-02-15'),
    (20, 'ACCT-4001', 'deposit',    500000000,  1150000000, 'Merger proceeds',        '2025-03-01'),
    (21, 'ACCT-5001', 'deposit',    800000000,  800000000,  'Sovereign fund transfer','2025-01-01'),
    (22, 'ACCT-5001', 'deposit',    600000000,  1400000000, 'Oil revenue',            '2025-01-15'),
    (23, 'ACCT-5001', 'deposit',    400000000,  1800000000, 'Tax collection',         '2025-02-01'),
    (24, 'ACCT-5001', 'deposit',    100000000,  1900000000, 'Bond maturity',          '2025-02-15'),
    (25, 'ACCT-5001', 'withdrawal', -50000000,  1850000000, 'Infrastructure project', '2025-03-01'),
    (26, 'ACCT-1001', 'deposit',    1800000,    4480000,    'Q2 revenue',             '2025-04-01'),
    (27, 'ACCT-2001', 'deposit',    4000000,    10700000,   'Series B funding',       '2025-04-01'),
    (28, 'ACCT-3001', 'deposit',    30000000,   92000000,   'Annual inflow',          '2025-04-01'),
    (29, 'ACCT-4001', 'deposit',    300000000,  1450000000, 'Government grant',       '2025-04-01'),
    (30, 'ACCT-5001', 'deposit',    200000000,  2050000000, 'Trade surplus',          '2025-04-15');
