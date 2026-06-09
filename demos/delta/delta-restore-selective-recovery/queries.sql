-- ============================================================================
-- Delta Selective Recovery — VERSION AS OF Extraction — Educational Queries
-- ============================================================================
-- WHAT: When only part of a table is corrupted, full RESTORE would undo good
--       changes elsewhere. VERSION AS OF lets you extract clean rows from a
--       historical version and surgically replace only the bad data.
-- WHY:  In production, corruption is rarely total. A bad data feed might
--       affect one partition while other partitions have valid work (rebalances,
--       new trades, corrections) that must be preserved.
-- HOW:  DELETE the corrupted rows, then INSERT them back from a known-good
--       version using SELECT * FROM table VERSION AS OF N WHERE condition.
-- ============================================================================
--
-- Version history we will build:
--   V0: CREATE table                                          (done in setup)
--   V1: INSERT 24 positions across 3 portfolios               (done in setup)
--   V2: UPDATE — rebalance income portfolio (increase shares)
--   V3: UPDATE — bad market data corrupts growth prices (10x)
--   V4: Selective recovery — DELETE growth + INSERT FROM VERSION AS OF 1
-- ============================================================================


-- ============================================================================
-- Baseline: Portfolio Summary after V1 Insert
-- ============================================================================
-- Setup created 24 positions across 3 portfolios. Let's see the starting point.

ASSERT ROW_COUNT = 3
SELECT portfolio,
       COUNT(*)                  AS positions,
       ROUND(SUM(market_value), 2) AS total_market_value
FROM {{zone_name}}.delta_demos.portfolio_positions
GROUP BY portfolio
ORDER BY portfolio;

-- Verify total row count
ASSERT ROW_COUNT = 24
SELECT * FROM {{zone_name}}.delta_demos.portfolio_positions
ORDER BY id;


-- ============================================================================
-- V2: Rebalance Income Portfolio — Increase Shares
-- ============================================================================
-- The portfolio manager increases holdings in 4 income stocks after quarterly
-- review. This is intentional, good work that must survive any later recovery.

ASSERT ROW_COUNT = 4
UPDATE {{zone_name}}.delta_demos.portfolio_positions
SET shares = CASE ticker
        WHEN 'JNJ' THEN 150
        WHEN 'PG'  THEN 120
        WHEN 'KO'  THEN 200
        WHEN 'PEP' THEN 100
        ELSE shares END,
    market_value = CASE ticker
        WHEN 'JNJ' THEN 150 * 155.80
        WHEN 'PG'  THEN 120 * 162.40
        WHEN 'KO'  THEN 200 * 59.20
        WHEN 'PEP' THEN 100 * 172.50
        ELSE market_value END,
    last_updated = '2025-03-15'
WHERE portfolio = 'income' AND ticker IN ('JNJ', 'PG', 'KO', 'PEP');

-- Observe V2: Income portfolio after rebalancing
ASSERT ROW_COUNT = 8
ASSERT VALUE shares = 150 WHERE ticker = 'JNJ'
ASSERT VALUE market_value = 23370.0 WHERE ticker = 'JNJ'
ASSERT VALUE shares = 120 WHERE ticker = 'PG'
ASSERT VALUE market_value = 19488.0 WHERE ticker = 'PG'
ASSERT VALUE shares = 200 WHERE ticker = 'KO'
ASSERT VALUE market_value = 11840.0 WHERE ticker = 'KO'
ASSERT VALUE shares = 100 WHERE ticker = 'PEP'
ASSERT VALUE market_value = 17250.0 WHERE ticker = 'PEP'
SELECT ticker, shares, price, market_value, last_updated
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'income'
ORDER BY id;

-- Verify income total after rebalance
ASSERT ROW_COUNT = 1
ASSERT VALUE total_market_value = 109044.5
SELECT ROUND(SUM(market_value), 2) AS total_market_value
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'income';


-- ============================================================================
-- V3: Bad Market Data Feed — Growth Prices Corrupted (10x)
-- ============================================================================
-- A faulty market data feed pushes prices that are 10x too high for all growth
-- stocks. This makes the growth portfolio absurdly overvalued.

ASSERT ROW_COUNT = 8
UPDATE {{zone_name}}.delta_demos.portfolio_positions
SET price = price * 10,
    market_value = shares * price * 10,
    last_updated = '2025-03-16'
WHERE portfolio = 'growth';

-- Observe V3: Growth portfolio with corrupted 10x prices
ASSERT ROW_COUNT = 8
ASSERT VALUE price = 1855.0 WHERE ticker = 'AAPL'
ASSERT VALUE market_value = 185500.0 WHERE ticker = 'AAPL'
ASSERT VALUE price = 8750.0 WHERE ticker = 'NVDA'
ASSERT VALUE market_value = 437500.0 WHERE ticker = 'NVDA'
SELECT ticker, shares, price, market_value, last_updated
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'growth'
ORDER BY id;

-- Growth total is now absurdly high
ASSERT ROW_COUNT = 1
ASSERT VALUE growth_total = 1691090.0
SELECT ROUND(SUM(market_value), 2) AS growth_total
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'growth';

-- Current state: growth is corrupted, but income and balanced are fine
ASSERT ROW_COUNT = 3
SELECT portfolio,
       COUNT(*)                  AS positions,
       ROUND(SUM(market_value), 2) AS total_market_value
FROM {{zone_name}}.delta_demos.portfolio_positions
GROUP BY portfolio
ORDER BY portfolio;


-- ============================================================================
-- The Problem: Why Full RESTORE Won't Work
-- ============================================================================
-- A full RESTORE TO VERSION 1 would fix growth prices, but it would also
-- undo the income rebalancing from V2. Let's prove this by inspecting V1:

-- VERSION AS OF 1 shows income BEFORE rebalancing (original shares)
ASSERT ROW_COUNT = 1
ASSERT VALUE v1_income_total = 96437.5
SELECT ROUND(SUM(market_value), 2) AS v1_income_total
FROM {{zone_name}}.delta_demos.portfolio_positions VERSION AS OF 1
WHERE portfolio = 'income';

-- Current income total (with rebalancing we want to KEEP)
ASSERT ROW_COUNT = 1
ASSERT VALUE current_income_total = 109044.5
SELECT ROUND(SUM(market_value), 2) AS current_income_total
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'income';

-- The difference: $12,607 in rebalancing gains would be lost
ASSERT ROW_COUNT = 1
ASSERT VALUE rebalancing_at_risk = 12607.0
SELECT ROUND(
    (SELECT SUM(market_value) FROM {{zone_name}}.delta_demos.portfolio_positions WHERE portfolio = 'income')
  - (SELECT SUM(market_value) FROM {{zone_name}}.delta_demos.portfolio_positions VERSION AS OF 1 WHERE portfolio = 'income')
, 2) AS rebalancing_at_risk;


-- ============================================================================
-- V4: Selective Recovery — DELETE + INSERT FROM VERSION AS OF
-- ============================================================================
-- Instead of full RESTORE, we surgically replace ONLY the growth portfolio:
--   Step 1: DELETE the corrupted growth rows
--   Step 2: INSERT the clean growth rows from VERSION AS OF 1

-- Step 1: Remove corrupted growth positions
ASSERT ROW_COUNT = 8
DELETE FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'growth';

-- Step 2: Restore growth positions from the original clean version
ASSERT ROW_COUNT = 8
INSERT INTO {{zone_name}}.delta_demos.portfolio_positions
SELECT * FROM {{zone_name}}.delta_demos.portfolio_positions VERSION AS OF 1
WHERE portfolio = 'growth';


-- ============================================================================
-- Verification: Growth Restored, Income Rebalancing Preserved
-- ============================================================================

-- Growth portfolio is back to original correct prices
ASSERT ROW_COUNT = 8
ASSERT VALUE price = 185.5 WHERE ticker = 'AAPL'
ASSERT VALUE market_value = 18550.0 WHERE ticker = 'AAPL'
ASSERT VALUE price = 875.0 WHERE ticker = 'NVDA'
ASSERT VALUE market_value = 43750.0 WHERE ticker = 'NVDA'
SELECT ticker, shares, price, market_value, last_updated
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'growth'
ORDER BY id;

-- Growth total is back to the correct value
ASSERT ROW_COUNT = 1
ASSERT VALUE growth_total = 169109.0
SELECT ROUND(SUM(market_value), 2) AS growth_total
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'growth';

-- Income portfolio STILL has the rebalanced shares (V2 changes preserved)
ASSERT ROW_COUNT = 8
ASSERT VALUE shares = 150 WHERE ticker = 'JNJ'
ASSERT VALUE shares = 120 WHERE ticker = 'PG'
ASSERT VALUE shares = 200 WHERE ticker = 'KO'
ASSERT VALUE shares = 100 WHERE ticker = 'PEP'
SELECT ticker, shares, price, market_value, last_updated
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'income'
ORDER BY id;

-- Income total confirms rebalancing is intact
ASSERT ROW_COUNT = 1
ASSERT VALUE income_total = 109044.5
SELECT ROUND(SUM(market_value), 2) AS income_total
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'income';

-- Balanced portfolio is completely untouched
ASSERT ROW_COUNT = 1
ASSERT VALUE balanced_total = 238778.0
SELECT ROUND(SUM(market_value), 2) AS balanced_total
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'balanced';

-- Final portfolio summary — all three portfolios in correct state
ASSERT ROW_COUNT = 3
SELECT portfolio,
       COUNT(*)                  AS positions,
       ROUND(SUM(market_value), 2) AS total_market_value
FROM {{zone_name}}.delta_demos.portfolio_positions
GROUP BY portfolio
ORDER BY portfolio;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: all 24 positions present
ASSERT ROW_COUNT = 24
SELECT * FROM {{zone_name}}.delta_demos.portfolio_positions;

-- Verify growth_restored: growth total matches original V1 value
ASSERT VALUE growth_total = 169109.0
SELECT ROUND(SUM(market_value), 2) AS growth_total
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'growth';

-- Verify income_preserved: income total matches V2 rebalanced value
ASSERT VALUE income_total = 109044.5
SELECT ROUND(SUM(market_value), 2) AS income_total
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'income';

-- Verify balanced_untouched: balanced total unchanged throughout
ASSERT VALUE balanced_total = 238778.0
SELECT ROUND(SUM(market_value), 2) AS balanced_total
FROM {{zone_name}}.delta_demos.portfolio_positions
WHERE portfolio = 'balanced';

-- Verify grand_total: sum of all portfolios
ASSERT VALUE grand_total = 516931.5
SELECT ROUND(SUM(market_value), 2) AS grand_total
FROM {{zone_name}}.delta_demos.portfolio_positions;

-- Verify aapl_price_correct: AAPL back to original price (not 10x)
ASSERT VALUE price = 185.5
SELECT price FROM {{zone_name}}.delta_demos.portfolio_positions WHERE ticker = 'AAPL';

-- Verify jnj_shares_kept: JNJ keeps rebalanced shares (150, not original 120)
ASSERT VALUE shares = 150
SELECT shares FROM {{zone_name}}.delta_demos.portfolio_positions WHERE ticker = 'JNJ';

-- Verify portfolio_count: all 3 portfolios present
ASSERT VALUE cnt = 3
SELECT COUNT(DISTINCT portfolio) AS cnt FROM {{zone_name}}.delta_demos.portfolio_positions;

-- Verify positions_per_portfolio: each portfolio has exactly 8 positions
ASSERT ROW_COUNT = 3
ASSERT VALUE positions = 8 WHERE portfolio = 'growth'
ASSERT VALUE positions = 8 WHERE portfolio = 'income'
ASSERT VALUE positions = 8 WHERE portfolio = 'balanced'
SELECT portfolio, COUNT(*) AS positions
FROM {{zone_name}}.delta_demos.portfolio_positions
GROUP BY portfolio
ORDER BY portfolio;
