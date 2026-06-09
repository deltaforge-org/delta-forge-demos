-- ============================================================================
-- Delta UPDATE Expressions — Portfolio Rebalancing — Educational Queries
-- ============================================================================
-- WHAT: Shows how UPDATE SET clauses can use computed expressions — arithmetic,
--       CASE WHEN, and ROUND — to transform data in place.
-- WHY:  Real-world data transformations (pricing, risk, rebalancing) require
--       more than simple value assignment. Expressive UPDATEs eliminate the
--       need for read-modify-write patterns in application code.
-- HOW:  Three UPDATE passes demonstrate: (1) arithmetic appreciation,
--       (2) CASE-based reclassification, (3) percentage decline. VERSION AS OF
--       compares before and after each pass.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Portfolio Summary After Rebalancing
-- ============================================================================
-- After all three UPDATE passes (equity +5%, risk reclassification, bond -2%),
-- here's how total value breaks down by asset class.

ASSERT ROW_COUNT = 5
ASSERT VALUE holdings = 8 WHERE asset_class = 'equity'
ASSERT VALUE total_value = 159052.20 WHERE asset_class = 'equity'
SELECT asset_class,
       COUNT(*) AS holdings,
       SUM(market_value) AS total_value,
       ROUND(AVG(market_value), 2) AS avg_value
FROM {{zone_name}}.delta_demos.portfolio_holdings
GROUP BY asset_class
ORDER BY total_value DESC;


-- ============================================================================
-- EXPLORE: Portfolio-Level Totals
-- ============================================================================
-- Each portfolio's total value after the rebalancing cycle.

ASSERT ROW_COUNT = 3
ASSERT VALUE portfolio_value = 149168.50 WHERE portfolio_id = 'PF-ALPHA'
ASSERT VALUE portfolio_value = 120331.15 WHERE portfolio_id = 'PF-BETA'
SELECT portfolio_id,
       COUNT(*) AS holdings,
       SUM(market_value) AS portfolio_value
FROM {{zone_name}}.delta_demos.portfolio_holdings
GROUP BY portfolio_id
ORDER BY portfolio_value DESC;


-- ============================================================================
-- LEARN: CASE WHEN Reclassification Results
-- ============================================================================
-- The risk reclassification UPDATE used CASE WHEN to dynamically assign
-- ratings based on market_value thresholds. Let's see the distribution.
--
-- Thresholds applied:
--   high:   market_value > 35,000
--   medium: market_value > 15,000
--   low:    market_value <= 15,000

ASSERT VALUE count = 3 WHERE risk_rating = 'high'
ASSERT VALUE count = 6 WHERE risk_rating = 'medium'
ASSERT VALUE count = 11 WHERE risk_rating = 'low'
ASSERT ROW_COUNT = 3
SELECT risk_rating,
       COUNT(*) AS count,
       SUM(market_value) AS total_value
FROM {{zone_name}}.delta_demos.portfolio_holdings
GROUP BY risk_rating
ORDER BY total_value DESC;


-- ============================================================================
-- EXPLORE: Top Holdings by Market Value
-- ============================================================================
-- The largest positions after rebalancing. Note that CASH holdings can
-- rank highly because they weren't affected by price changes.

ASSERT ROW_COUNT = 5
ASSERT VALUE market_value = 50000.00 WHERE ticker = 'CASH' AND portfolio_id = 'PF-ALPHA'
SELECT ticker, portfolio_id, market_value, risk_rating
FROM {{zone_name}}.delta_demos.portfolio_holdings
ORDER BY market_value DESC
LIMIT 5;


-- ============================================================================
-- LEARN: VERSION AS OF — Compare Pre- and Post-Appreciation Prices
-- ============================================================================
-- At Version 1 (before any updates), equity prices were at their original
-- levels. This shows the exact prices that were input to the +5% formula.

ASSERT ROW_COUNT = 8
ASSERT VALUE market_price = 185.25 WHERE ticker = 'AAPL'
ASSERT VALUE market_price = 620.75 WHERE ticker = 'NVDA'
SELECT ticker, market_price
FROM {{zone_name}}.delta_demos.portfolio_holdings VERSION AS OF 1
WHERE asset_class = 'equity'
ORDER BY ticker;


-- ============================================================================
-- LEARN: Observing the Update Chain via DESCRIBE HISTORY
-- ============================================================================
-- The transaction log records each UPDATE as a separate version.
-- V0: CREATE TABLE, V1: INSERT, V2: equity +5%, V3: risk reclassification,
-- V4: bond -2%. Each version is a rebalancing checkpoint.

-- Non-deterministic: commit timestamps set at write time
ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.delta_demos.portfolio_holdings;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total rows: 20 holdings (no inserts or deletes, only updates)
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.portfolio_holdings;

-- Verify distinct portfolios: 3 portfolios
ASSERT VALUE cnt = 3
SELECT COUNT(DISTINCT portfolio_id) AS cnt FROM {{zone_name}}.delta_demos.portfolio_holdings;

-- Verify distinct asset classes: 5 asset classes
ASSERT VALUE cnt = 5
SELECT COUNT(DISTINCT asset_class) AS cnt FROM {{zone_name}}.delta_demos.portfolio_holdings;

-- Verify total market value after all rebalancing
ASSERT VALUE total = 362359.70
SELECT SUM(market_value) AS total FROM {{zone_name}}.delta_demos.portfolio_holdings;

-- Verify high-risk count after reclassification
ASSERT VALUE cnt = 3
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.portfolio_holdings WHERE risk_rating = 'high';

-- Verify low-risk count after reclassification
ASSERT VALUE cnt = 11
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.portfolio_holdings WHERE risk_rating = 'low';
