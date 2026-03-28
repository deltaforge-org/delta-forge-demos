-- ============================================================================
-- Iceberg V3 UniForm — Investment Portfolio Audit Trail — Setup
-- ============================================================================
-- Creates a Delta table with Iceberg UniForm V3 for tracking investment
-- portfolio holdings. Seeds 25 positions across 5 sectors and 3 accounts.
-- Time travel mutations happen in queries.sql to create distinct snapshots.
--
-- Dataset: 25 holdings with columns: holding_id, account, ticker, sector,
-- shares, cost_basis, market_price, acquired_date.
-- Accounts: IRA-401K (9), ROTH-IRA (8), TAXABLE (8)
-- Sectors: Technology (6), Healthcare (5), Energy (5), Finance (5), Consumer (4)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm V3
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.portfolio_holdings (
    holding_id     INT,
    account        VARCHAR,
    ticker         VARCHAR,
    sector         VARCHAR,
    shares         INT,
    cost_basis     DOUBLE,
    market_price   DOUBLE,
    acquired_date  VARCHAR
) LOCATION '{{data_path}}/portfolio_holdings'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '3',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.portfolio_holdings TO USER {{current_user}};

-- STEP 3: Seed 25 portfolio holdings (Version 1)
INSERT INTO {{zone_name}}.iceberg_demos.portfolio_holdings VALUES
    (1,  'IRA-401K',  'AAPL',  'Technology',  150, 145.00, 178.50, '2023-01-15'),
    (2,  'IRA-401K',  'MSFT',  'Technology',  100, 280.00, 415.20, '2023-02-10'),
    (3,  'IRA-401K',  'JNJ',   'Healthcare',   80, 160.00, 155.30, '2023-03-01'),
    (4,  'IRA-401K',  'XOM',   'Energy',       200, 95.00,  105.60, '2023-03-15'),
    (5,  'IRA-401K',  'JPM',   'Finance',      120, 135.00, 198.40, '2023-04-01'),
    (6,  'IRA-401K',  'PG',    'Consumer',      90, 148.00, 162.80, '2023-04-15'),
    (7,  'IRA-401K',  'NVDA',  'Technology',    60, 220.00, 875.30, '2023-05-01'),
    (8,  'IRA-401K',  'UNH',   'Healthcare',    40, 490.00, 528.10, '2023-05-15'),
    (9,  'IRA-401K',  'CVX',   'Energy',       110, 155.00, 148.90, '2023-06-01'),
    (10, 'ROTH-IRA',  'GOOGL', 'Technology',    75, 105.00, 174.60, '2023-01-20'),
    (11, 'ROTH-IRA',  'AMZN',  'Technology',    50, 95.00,  185.40, '2023-02-15'),
    (12, 'ROTH-IRA',  'PFE',   'Healthcare',   300, 48.00,  27.50,  '2023-03-10'),
    (13, 'ROTH-IRA',  'SLB',   'Energy',       180, 52.00,  48.70,  '2023-04-05'),
    (14, 'ROTH-IRA',  'GS',    'Finance',       35, 340.00, 475.20, '2023-04-20'),
    (15, 'ROTH-IRA',  'KO',    'Consumer',     200, 58.00,  62.40,  '2023-05-10'),
    (16, 'ROTH-IRA',  'BAC',   'Finance',      250, 30.00,  37.80,  '2023-06-05'),
    (17, 'ROTH-IRA',  'ABBV',  'Healthcare',    65, 155.00, 172.30, '2023-06-15'),
    (18, 'TAXABLE',   'TSLA',  'Technology',    40, 175.00, 248.90, '2023-02-01'),
    (19, 'TAXABLE',   'MRK',   'Healthcare',   100, 110.00, 128.40, '2023-02-20'),
    (20, 'TAXABLE',   'HAL',   'Energy',       150, 35.00,  36.20,  '2023-03-20'),
    (21, 'TAXABLE',   'WFC',   'Finance',      180, 42.00,  58.30,  '2023-04-10'),
    (22, 'TAXABLE',   'MCD',   'Consumer',      55, 265.00, 295.80, '2023-05-05'),
    (23, 'TAXABLE',   'BKR',   'Energy',       220, 28.00,  34.50,  '2023-05-25'),
    (24, 'TAXABLE',   'C',     'Finance',      160, 48.00,  62.10,  '2023-06-10'),
    (25, 'TAXABLE',   'NKE',   'Consumer',      70, 115.00, 98.70,  '2023-06-20');
