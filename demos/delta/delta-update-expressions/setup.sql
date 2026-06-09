-- ============================================================================
-- Delta UPDATE Expressions — Portfolio Rebalancing — Setup Script
-- ============================================================================
-- Demonstrates computed UPDATE expressions: arithmetic in SET clauses,
-- CASE WHEN reclassification, and multi-column recalculation.
-- Three investment portfolios go through a quarterly rebalancing cycle.
--
-- Tables created:
--   1. portfolio_holdings — 20 rows (no inserts/deletes, only updates)
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE
--   3. INSERT — 20 holdings across 3 portfolios and 5 asset classes
--   4. UPDATE — Equity market price appreciation (+5%)
--   5. UPDATE — Risk rating reclassification based on market value thresholds
--   6. UPDATE — Bond market price decline (-2%)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: portfolio_holdings — Investment portfolio positions
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.portfolio_holdings (
    holding_id     INT,
    portfolio_id   VARCHAR,
    ticker         VARCHAR,
    asset_class    VARCHAR,
    shares         DECIMAL(12,4),
    cost_basis     DECIMAL(12,2),
    market_price   DECIMAL(12,2),
    market_value   DECIMAL(14,2),
    weight_pct     DECIMAL(6,2),
    risk_rating    VARCHAR,
    last_rebalance VARCHAR
) LOCATION 'delta-update-expressions/portfolio_holdings';


-- ============================================================================
-- VERSION 1: Initial positions — 20 holdings across 3 portfolios
-- ============================================================================
-- PF-ALPHA: Conservative balanced (6 holdings)
-- PF-BETA:  Growth-oriented (6 holdings)
-- PF-GAMMA: Diversified value (8 holdings)
-- Asset classes: equity, bond, commodity, reit, cash
INSERT INTO {{zone_name}}.delta_demos.portfolio_holdings VALUES
    (1,  'PF-ALPHA', 'AAPL',  'equity',    150.0000, 142.50,   185.25,   27787.50,  18.52, 'medium', '2024-01-02 09:00:00'),
    (2,  'PF-ALPHA', 'MSFT',  'equity',    100.0000, 310.00,   378.50,   37850.00,  25.23, 'medium', '2024-01-02 09:00:00'),
    (3,  'PF-ALPHA', 'BND',   'bond',      200.0000, 72.00,    71.50,    14300.00,  9.53,  'low',    '2024-01-02 09:00:00'),
    (4,  'PF-ALPHA', 'GLD',   'commodity', 50.0000,  175.00,   192.30,   9615.00,   6.41,  'medium', '2024-01-02 09:00:00'),
    (5,  'PF-ALPHA', 'VNQ',   'reit',      80.0000,  85.00,    82.75,    6620.00,   4.41,  'medium', '2024-01-02 09:00:00'),
    (6,  'PF-ALPHA', 'CASH',  'cash',      1.0000,   50000.00, 50000.00, 50000.00,  33.33, 'low',    '2024-01-02 09:00:00'),
    (7,  'PF-BETA',  'TSLA',  'equity',    75.0000,  205.00,   245.80,   18435.00,  15.36, 'high',   '2024-01-02 09:00:00'),
    (8,  'PF-BETA',  'NVDA',  'equity',    60.0000,  450.00,   620.75,   37245.00,  31.04, 'high',   '2024-01-02 09:00:00'),
    (9,  'PF-BETA',  'AGG',   'bond',      300.0000, 100.00,   98.25,    29475.00,  24.56, 'low',    '2024-01-02 09:00:00'),
    (10, 'PF-BETA',  'SLV',   'commodity', 100.0000, 22.50,    24.80,    2480.00,   2.07,  'high',   '2024-01-02 09:00:00'),
    (11, 'PF-BETA',  'IYR',   'reit',      120.0000, 90.00,    87.50,    10500.00,  8.75,  'medium', '2024-01-02 09:00:00'),
    (12, 'PF-BETA',  'CASH',  'cash',      1.0000,   20000.00, 20000.00, 20000.00,  16.67, 'low',    '2024-01-02 09:00:00'),
    (13, 'PF-GAMMA', 'AMZN',  'equity',    40.0000,  135.00,   178.25,   7130.00,   8.91,  'medium', '2024-01-02 09:00:00'),
    (14, 'PF-GAMMA', 'GOOGL', 'equity',    55.0000,  125.00,   141.20,   7766.00,   9.71,  'medium', '2024-01-02 09:00:00'),
    (15, 'PF-GAMMA', 'TLT',   'bond',      250.0000, 98.00,    92.80,    23200.00,  29.00, 'low',    '2024-01-02 09:00:00'),
    (16, 'PF-GAMMA', 'DBA',   'commodity', 80.0000,  18.50,    19.75,    1580.00,   1.98,  'medium', '2024-01-02 09:00:00'),
    (17, 'PF-GAMMA', 'SCHH',  'reit',      90.0000,  22.00,    20.85,    1876.50,   2.35,  'low',    '2024-01-02 09:00:00'),
    (18, 'PF-GAMMA', 'CASH',  'cash',      1.0000,   35000.00, 35000.00, 35000.00,  43.75, 'low',    '2024-01-02 09:00:00'),
    (19, 'PF-GAMMA', 'JPM',   'equity',    30.0000,  155.00,   183.40,   5502.00,   6.88,  'medium', '2024-01-02 09:00:00'),
    (20, 'PF-GAMMA', 'META',  'equity',    25.0000,  340.00,   390.50,   9762.50,   12.20, 'high',   '2024-01-02 09:00:00');


-- ============================================================================
-- VERSION 2: Equity appreciation — market prices up 5%
-- ============================================================================
-- Q1 rally: all equity holdings see a 5% market price increase.
-- Both market_price and market_value are recalculated using arithmetic
-- expressions in the SET clause.
UPDATE {{zone_name}}.delta_demos.portfolio_holdings
SET market_price = ROUND(market_price * 1.05, 2),
    market_value = ROUND(shares * ROUND(market_price * 1.05, 2), 2),
    last_rebalance = '2024-03-15 09:00:00'
WHERE asset_class = 'equity';


-- ============================================================================
-- VERSION 3: Risk reclassification — CASE WHEN based on market value
-- ============================================================================
-- Compliance requires risk ratings to reflect current exposure.
-- High: market_value > 35000 | Medium: > 15000 | Low: <= 15000
UPDATE {{zone_name}}.delta_demos.portfolio_holdings
SET risk_rating = CASE
        WHEN market_value > 35000 THEN 'high'
        WHEN market_value > 15000 THEN 'medium'
        ELSE 'low'
    END,
    last_rebalance = '2024-03-15 10:00:00';


-- ============================================================================
-- VERSION 4: Bond price decline — rates up, bonds down 2%
-- ============================================================================
-- Rising interest rates push bond prices down. Same arithmetic pattern
-- as the equity update, but in the opposite direction.
UPDATE {{zone_name}}.delta_demos.portfolio_holdings
SET market_price = ROUND(market_price * 0.98, 2),
    market_value = ROUND(shares * ROUND(market_price * 0.98, 2), 2),
    last_rebalance = '2024-03-15 11:00:00'
WHERE asset_class = 'bond';
