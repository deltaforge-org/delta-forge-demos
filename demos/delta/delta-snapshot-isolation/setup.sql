-- ============================================================================
-- Delta Snapshot Isolation — Setup Script
-- ============================================================================
-- Creates the fund_holdings table and loads 60 positions across 3 batch
-- inserts to simulate fragmented data files from overnight fund loads.
--
-- Tables created:
--   1. fund_holdings — 60 positions across 4 investment funds
--
-- Operations performed:
--   1. Zone & schema creation
--   2. CREATE DELTA TABLE
--   3. INSERT batch 1 — 20 positions (Growth Fund GF01 + Value Fund VF02)
--   4. INSERT batch 2 — 20 positions (Income Fund IF03)
--   5. INSERT batch 3 — 20 positions (Sector Rotation Fund SR04)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: fund_holdings — investment positions with multi-fund access patterns
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.fund_holdings (
    id          INT,
    fund_id     VARCHAR,
    ticker      VARCHAR,
    shares      INT,
    price       DECIMAL(10,2),
    sector      VARCHAR,
    trade_date  VARCHAR
) LOCATION 'delta-snapshot-isolation/fund_holdings';


-- ============================================================================
-- STEP 2: Batch 1 — Growth Fund (GF01) + Value Fund (VF02) — 20 positions
-- ============================================================================
-- Each batch INSERT creates separate data files. Three batches = three files,
-- which is the fragmentation that OPTIMIZE will later compact.
INSERT INTO {{zone_name}}.delta_demos.fund_holdings VALUES
    (1,  'GF01', 'AAPL',  500,  185.50, 'Technology',  '2025-01-02'),
    (2,  'GF01', 'MSFT',  300,  375.20, 'Technology',  '2025-01-02'),
    (3,  'GF01', 'GOOGL', 200,  140.80, 'Technology',  '2025-01-02'),
    (4,  'GF01', 'AMZN',  150,  155.30, 'Technology',  '2025-01-02'),
    (5,  'GF01', 'JPM',   400,  172.40, 'Financials',  '2025-01-02'),
    (6,  'GF01', 'BAC',   800,   34.60, 'Financials',  '2025-01-02'),
    (7,  'GF01', 'JNJ',   250,  156.90, 'Healthcare',  '2025-01-02'),
    (8,  'GF01', 'PFE',   600,   28.75, 'Healthcare',  '2025-01-02'),
    (9,  'GF01', 'XOM',   350,  104.20, 'Energy',      '2025-01-02'),
    (10, 'GF01', 'CVX',   200,  152.80, 'Energy',      '2025-01-02'),
    (11, 'VF02', 'AAPL',  1000, 185.50, 'Technology',  '2025-01-02'),
    (12, 'VF02', 'MSFT',  600,  375.20, 'Technology',  '2025-01-02'),
    (13, 'VF02', 'BRK.B', 400,  362.10, 'Financials',  '2025-01-02'),
    (14, 'VF02', 'V',     300,  260.40, 'Financials',  '2025-01-02'),
    (15, 'VF02', 'UNH',   150,  528.30, 'Healthcare',  '2025-01-02'),
    (16, 'VF02', 'ABBV',  500,  162.70, 'Healthcare',  '2025-01-02'),
    (17, 'VF02', 'XOM',   700,  104.20, 'Energy',      '2025-01-02'),
    (18, 'VF02', 'COP',   400,  118.50, 'Energy',      '2025-01-02'),
    (19, 'VF02', 'PG',    350,  152.60, 'Consumer',    '2025-01-02'),
    (20, 'VF02', 'KO',    800,   59.40, 'Consumer',    '2025-01-02');


-- ============================================================================
-- STEP 3: Batch 2 — Income Fund (IF03) — 20 positions
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.fund_holdings
SELECT * FROM (VALUES
    (21, 'IF03', 'T',      1500, 17.20,  'Telecom',     '2025-01-03'),
    (22, 'IF03', 'VZ',     1000, 38.90,  'Telecom',     '2025-01-03'),
    (23, 'IF03', 'IBM',    400,  168.50, 'Technology',  '2025-01-03'),
    (24, 'IF03', 'INTC',   800,   48.30, 'Technology',  '2025-01-03'),
    (25, 'IF03', 'WFC',    600,   48.70, 'Financials',  '2025-01-03'),
    (26, 'IF03', 'USB',    900,   42.10, 'Financials',  '2025-01-03'),
    (27, 'IF03', 'MRK',    500,  108.40, 'Healthcare',  '2025-01-03'),
    (28, 'IF03', 'BMY',    700,   52.60, 'Healthcare',  '2025-01-03'),
    (29, 'IF03', 'D',      400,   48.90, 'Utilities',   '2025-01-03'),
    (30, 'IF03', 'SO',     600,   72.30, 'Utilities',   '2025-01-03'),
    (31, 'IF03', 'PG',     500,  152.60, 'Consumer',    '2025-01-03'),
    (32, 'IF03', 'KO',     1200,  59.40, 'Consumer',    '2025-01-03'),
    (33, 'IF03', 'PEP',    400,  174.80, 'Consumer',    '2025-01-03'),
    (34, 'IF03', 'WMT',    300,  162.50, 'Consumer',    '2025-01-03'),
    (35, 'IF03', 'JNJ',    450,  156.90, 'Healthcare',  '2025-01-03'),
    (36, 'IF03', 'ABBV',   350,  162.70, 'Healthcare',  '2025-01-03'),
    (37, 'IF03', 'XOM',    500,  104.20, 'Energy',      '2025-01-03'),
    (38, 'IF03', 'CVX',    300,  152.80, 'Energy',      '2025-01-03'),
    (39, 'IF03', 'NEE',    400,   62.40, 'Utilities',   '2025-01-03'),
    (40, 'IF03', 'DUK',    500,   98.70, 'Utilities',   '2025-01-03')
) AS t(id, fund_id, ticker, shares, price, sector, trade_date);


-- ============================================================================
-- STEP 4: Batch 3 — Sector Rotation Fund (SR04) — 20 positions
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.fund_holdings
SELECT * FROM (VALUES
    (41, 'SR04', 'NVDA',  200,  495.20, 'Technology',  '2025-01-03'),
    (42, 'SR04', 'AMD',   500,  147.80, 'Technology',  '2025-01-03'),
    (43, 'SR04', 'META',  250,  353.60, 'Technology',  '2025-01-03'),
    (44, 'SR04', 'NFLX',  150,  487.30, 'Technology',  '2025-01-03'),
    (45, 'SR04', 'GS',    200,  385.40, 'Financials',  '2025-01-03'),
    (46, 'SR04', 'MS',    400,   87.60, 'Financials',  '2025-01-03'),
    (47, 'SR04', 'LLY',   100,  612.50, 'Healthcare',  '2025-01-03'),
    (48, 'SR04', 'TMO',   150,  532.80, 'Healthcare',  '2025-01-03'),
    (49, 'SR04', 'SLB',   600,   52.40, 'Energy',      '2025-01-03'),
    (50, 'SR04', 'EOG',   350,  122.70, 'Energy',      '2025-01-03'),
    (51, 'SR04', 'COST',  100,  575.90, 'Consumer',    '2025-01-03'),
    (52, 'SR04', 'HD',    200,  348.20, 'Consumer',    '2025-01-03'),
    (53, 'SR04', 'TSLA',  300,  248.50, 'Technology',  '2025-01-03'),
    (54, 'SR04', 'CRM',   250,  262.40, 'Technology',  '2025-01-03'),
    (55, 'SR04', 'AVGO',  120,  1085.60,'Technology',  '2025-01-03'),
    (56, 'SR04', 'ORCL',  400,  118.90, 'Technology',  '2025-01-03'),
    (57, 'SR04', 'QCOM',  350,  148.30, 'Technology',  '2025-01-03'),
    (58, 'SR04', 'AMAT',  300,  158.40, 'Technology',  '2025-01-03'),
    (59, 'SR04', 'MU',    500,   84.70, 'Technology',  '2025-01-03'),
    (60, 'SR04', 'LRCX',  150,  688.20, 'Technology',  '2025-01-03')
) AS t(id, fund_id, ticker, shares, price, sector, trade_date);
