-- ============================================================================
-- Delta Selective Recovery — VERSION AS OF Extraction — Setup Script
-- ============================================================================
-- Creates the portfolio_positions table with 24 positions across 3 investment
-- portfolios (growth, income, balanced). The version-building operations
-- (V2–V4) are in queries.sql for interactive exploration.
--
-- Tables created:
--   1. portfolio_positions — 24 investment positions (V1 baseline)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- V0: CREATE + V1: INSERT 24 portfolio positions
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.portfolio_positions (
    id            INT,
    portfolio     VARCHAR,
    ticker        VARCHAR,
    shares        INT,
    price         DOUBLE,
    market_value  DOUBLE,
    sector        VARCHAR,
    last_updated  VARCHAR
) LOCATION 'delta-restore-selective-recovery/portfolio_positions';


INSERT INTO {{zone_name}}.delta_demos.portfolio_positions VALUES
    -- Growth Portfolio (8 positions)
    (1,  'growth', 'AAPL',  100, 185.50, 18550.00,  'technology', '2025-03-01'),
    (2,  'growth', 'MSFT',  75,  410.20, 30765.00,  'technology', '2025-03-01'),
    (3,  'growth', 'NVDA',  50,  875.00, 43750.00,  'technology', '2025-03-01'),
    (4,  'growth', 'AMZN',  60,  178.30, 10698.00,  'consumer',   '2025-03-01'),
    (5,  'growth', 'GOOGL', 40,  141.80, 5672.00,   'technology', '2025-03-01'),
    (6,  'growth', 'META',  55,  505.60, 27808.00,  'technology', '2025-03-01'),
    (7,  'growth', 'TSLA',  80,  245.10, 19608.00,  'consumer',   '2025-03-01'),
    (8,  'growth', 'CRM',   45,  272.40, 12258.00,  'technology', '2025-03-01'),
    -- Income Portfolio (8 positions)
    (9,  'income', 'JNJ',   120, 155.80, 18696.00,  'healthcare', '2025-03-01'),
    (10, 'income', 'PG',    100, 162.40, 16240.00,  'consumer',   '2025-03-01'),
    (11, 'income', 'KO',    150, 59.20,  8880.00,   'consumer',   '2025-03-01'),
    (12, 'income', 'PEP',   90,  172.50, 15525.00,  'consumer',   '2025-03-01'),
    (13, 'income', 'VZ',    200, 40.80,  8160.00,   'telecom',    '2025-03-01'),
    (14, 'income', 'T',     250, 17.50,  4375.00,   'telecom',    '2025-03-01'),
    (15, 'income', 'XOM',   110, 105.60, 11616.00,  'energy',     '2025-03-01'),
    (16, 'income', 'CVX',   85,  152.30, 12945.50,  'energy',     '2025-03-01'),
    -- Balanced Portfolio (8 positions)
    (17, 'balanced', 'SPY',  200, 505.40, 101080.00, 'index',       '2025-03-01'),
    (18, 'balanced', 'QQQ',  100, 435.20, 43520.00,  'index',       '2025-03-01'),
    (19, 'balanced', 'BND',  300, 72.80,  21840.00,  'bonds',       '2025-03-01'),
    (20, 'balanced', 'GLD',  80,  215.60, 17248.00,  'commodities', '2025-03-01'),
    (21, 'balanced', 'VNQ',  150, 82.90,  12435.00,  'real_estate', '2025-03-01'),
    (22, 'balanced', 'SCHD', 175, 78.40,  13720.00,  'index',       '2025-03-01'),
    (23, 'balanced', 'TLT',  120, 92.10,  11052.00,  'bonds',       '2025-03-01'),
    (24, 'balanced', 'IWM',  90,  198.70, 17883.00,  'index',       '2025-03-01');
