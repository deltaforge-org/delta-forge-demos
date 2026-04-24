-- ============================================================================
-- Iceberg V3 UniForm — Investment Portfolio Audit Trail — Queries
-- ============================================================================
-- Demonstrates time travel and version history on a V3 UniForm table.
-- Each mutation creates a new Delta version AND Iceberg V3 snapshot.
--
-- Mutation timeline:
--   V1: Seed 25 holdings (setup.sql)
--   V2: UPDATE market prices for 5 holdings (rebalancing)
--   V3: UPDATE shares for 3 holdings (dividend reinvestment)
--   V4: DELETE 3 positions (exits)
--   V5: INSERT 3 new holdings (acquisitions)
--
-- After mutations, VERSION AS OF queries read historical snapshots to prove
-- V3 metadata correctly tracks each version.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — 25 Holdings, Version 1
-- ============================================================================

ASSERT ROW_COUNT = 25
ASSERT VALUE ticker = 'AAPL' WHERE holding_id = 1
ASSERT VALUE shares = 150 WHERE holding_id = 1
ASSERT VALUE market_price = 178.5 WHERE holding_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.portfolio_holdings
ORDER BY holding_id;


-- ============================================================================
-- Query 2: Baseline Portfolio Value by Account
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE market_value = 230320.0 WHERE account = 'IRA-401K'
ASSERT VALUE market_value = 89142.5 WHERE account = 'ROTH-IRA'
ASSERT VALUE market_value = 79424.0 WHERE account = 'TAXABLE'
SELECT
    account,
    COUNT(*) AS holding_count,
    ROUND(SUM(shares * market_price), 2) AS market_value
FROM {{zone_name}}.iceberg_demos.portfolio_holdings
GROUP BY account
ORDER BY account;


-- ============================================================================
-- Query 3: Baseline Grand Totals
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_holdings = 25
ASSERT VALUE total_market_value = 398886.5
ASSERT VALUE total_cost_basis = 305655.0
SELECT
    COUNT(*) AS total_holdings,
    ROUND(SUM(shares * market_price), 2) AS total_market_value,
    ROUND(SUM(shares * cost_basis), 2) AS total_cost_basis
FROM {{zone_name}}.iceberg_demos.portfolio_holdings;


-- ============================================================================
-- V2: Rebalance — Update Market Prices (5 Holdings)
-- ============================================================================
-- Quarterly price refresh. Creates a new V3 snapshot.

ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.iceberg_demos.portfolio_holdings
SET market_price = CASE holding_id
    WHEN 1 THEN 192.30
    WHEN 2 THEN 430.50
    WHEN 12 THEN 31.20
    WHEN 16 THEN 42.10
    WHEN 18 THEN 265.40
END
WHERE holding_id IN (1, 2, 12, 16, 18);


-- ============================================================================
-- Query 4: Post-Rebalance Portfolio Value
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_market_value = 405331.5
SELECT
    ROUND(SUM(shares * market_price), 2) AS total_market_value
FROM {{zone_name}}.iceberg_demos.portfolio_holdings;


-- ============================================================================
-- V3: Dividend Reinvestment — Increase Shares (3 Holdings)
-- ============================================================================
-- DRIP adds fractional shares rounded to whole numbers. New snapshot.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.iceberg_demos.portfolio_holdings
SET shares = CASE holding_id
    WHEN 1 THEN 165
    WHEN 3 THEN 88
    WHEN 15 THEN 220
END
WHERE holding_id IN (1, 3, 15);


-- ============================================================================
-- Query 5: Post-Reinvestment Verification
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_market_value = 410706.4
ASSERT VALUE aapl_shares = 165
SELECT
    ROUND(SUM(shares * market_price), 2) AS total_market_value,
    SUM(CASE WHEN ticker = 'AAPL' THEN shares ELSE 0 END) AS aapl_shares
FROM {{zone_name}}.iceberg_demos.portfolio_holdings;


-- ============================================================================
-- V4: Exit Positions — Delete 3 Underperforming Holdings
-- ============================================================================
-- Sell PFE (down from cost), HAL (flat), NKE (down from cost).

ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.iceberg_demos.portfolio_holdings
WHERE holding_id IN (12, 20, 25);


-- ============================================================================
-- Query 6: Post-Exit — 22 Holdings Remain
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_holdings = 22
ASSERT VALUE total_market_value = 389007.4
SELECT
    COUNT(*) AS total_holdings,
    ROUND(SUM(shares * market_price), 2) AS total_market_value
FROM {{zone_name}}.iceberg_demos.portfolio_holdings;


-- ============================================================================
-- V5: New Acquisitions — Insert 3 Holdings
-- ============================================================================

INSERT INTO {{zone_name}}.iceberg_demos.portfolio_holdings VALUES
    (26, 'IRA-401K',  'META',  'Technology', 80,  485.00, 512.40, '2024-01-10'),
    (27, 'ROTH-IRA',  'LLY',   'Healthcare', 30,  580.00, 790.20, '2024-01-15'),
    (28, 'TAXABLE',   'COP',   'Energy',     140, 110.00, 118.50, '2024-01-20');


-- ============================================================================
-- Query 7: Final State — 25 Holdings After All Mutations
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_holdings = 25
ASSERT VALUE total_market_value = 470295.4
ASSERT VALUE total_cost_basis = 354170.0
SELECT
    COUNT(*) AS total_holdings,
    ROUND(SUM(shares * market_price), 2) AS total_market_value,
    ROUND(SUM(shares * cost_basis), 2) AS total_cost_basis
FROM {{zone_name}}.iceberg_demos.portfolio_holdings;


-- ============================================================================
-- Query 8: Time Travel — Read Version 1 (Original 25 Holdings)
-- ============================================================================
-- Before any mutations. AAPL had 150 shares at $178.50.

ASSERT ROW_COUNT = 25
ASSERT VALUE shares = 150 WHERE holding_id = 1
ASSERT VALUE market_price = 178.5 WHERE holding_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.portfolio_holdings VERSION AS OF 1
ORDER BY holding_id;


-- ============================================================================
-- Query 9: Time Travel — Version 1 Portfolio Value
-- ============================================================================
-- The original portfolio total was $398,886.50.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_market_value = 398886.5
SELECT
    ROUND(SUM(shares * market_price), 2) AS total_market_value
FROM {{zone_name}}.iceberg_demos.portfolio_holdings VERSION AS OF 1;


-- ============================================================================
-- Query 10: Time Travel — Version 2 (After Price Rebalance)
-- ============================================================================
-- AAPL price changed from 178.50 to 192.30 but shares still 150.

ASSERT ROW_COUNT = 25
ASSERT VALUE market_price = 192.3 WHERE holding_id = 1
ASSERT VALUE shares = 150 WHERE holding_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.portfolio_holdings VERSION AS OF 2
ORDER BY holding_id;


-- ============================================================================
-- Query 11: Version History
-- ============================================================================
-- Non-deterministic: DESCRIBE HISTORY returns commit timestamps and the
-- version count can vary slightly depending on engine internals. Use a
-- range assertion with WARNING severity.

ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.portfolio_holdings;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_holdings = 25
ASSERT VALUE account_count = 3
ASSERT VALUE sector_count = 5
ASSERT VALUE total_market_value = 470295.4
ASSERT VALUE total_cost_basis = 354170.0
SELECT
    COUNT(*) AS total_holdings,
    COUNT(DISTINCT account) AS account_count,
    COUNT(DISTINCT sector) AS sector_count,
    ROUND(SUM(shares * market_price), 2) AS total_market_value,
    ROUND(SUM(shares * cost_basis), 2) AS total_cost_basis
FROM {{zone_name}}.iceberg_demos.portfolio_holdings;


-- ============================================================================
-- ICEBERG V3 READ-BACK VERIFICATION
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg
USING ICEBERG
LOCATION 'portfolio_holdings';


-- ============================================================================
-- Iceberg Verify 1: Row Count — 25 Holdings
-- ============================================================================

ASSERT ROW_COUNT = 25
SELECT * FROM {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg ORDER BY holding_id;


-- ============================================================================
-- Iceberg Verify 2: Spot-Check AAPL — Mutated Across Multiple Versions
-- ============================================================================
-- AAPL was updated twice: price rebalance (V2) and DRIP shares (V3).
-- Final state: 165 shares at $192.30.

ASSERT ROW_COUNT = 1
ASSERT VALUE ticker = 'AAPL' WHERE holding_id = 1
ASSERT VALUE shares = 165 WHERE holding_id = 1
ASSERT VALUE market_price = 192.3 WHERE holding_id = 1
ASSERT VALUE cost_basis = 145.0 WHERE holding_id = 1
ASSERT VALUE account = 'IRA-401K' WHERE holding_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg
WHERE holding_id = 1;


-- ============================================================================
-- Iceberg Verify 3: Spot-Check NVDA — Untouched High-Value Holding
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE ticker = 'NVDA' WHERE holding_id = 7
ASSERT VALUE shares = 60 WHERE holding_id = 7
ASSERT VALUE market_price = 875.3 WHERE holding_id = 7
ASSERT VALUE sector = 'Technology' WHERE holding_id = 7
SELECT *
FROM {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg
WHERE holding_id = 7;


-- ============================================================================
-- Iceberg Verify 4: Spot-Check META — New Acquisition (V5 Insert)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE ticker = 'META' WHERE holding_id = 26
ASSERT VALUE shares = 80 WHERE holding_id = 26
ASSERT VALUE market_price = 512.4 WHERE holding_id = 26
ASSERT VALUE cost_basis = 485.0 WHERE holding_id = 26
SELECT *
FROM {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg
WHERE holding_id = 26;


-- ============================================================================
-- Iceberg Verify 5: Deleted Holdings Are Gone
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg
WHERE holding_id IN (12, 20, 25);


-- ============================================================================
-- Iceberg Verify 6: Per-Account Aggregates Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE holding_count = 10 WHERE account = 'IRA-401K'
ASSERT VALUE market_value = 279038.9 WHERE account = 'IRA-401K'
ASSERT VALUE holding_count = 8 WHERE account = 'ROTH-IRA'
ASSERT VALUE market_value = 106921.5 WHERE account = 'ROTH-IRA'
ASSERT VALUE holding_count = 7 WHERE account = 'TAXABLE'
ASSERT VALUE market_value = 84335.0 WHERE account = 'TAXABLE'
SELECT
    account,
    COUNT(*) AS holding_count,
    ROUND(SUM(shares * market_price), 2) AS market_value
FROM {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg
GROUP BY account
ORDER BY account;


-- ============================================================================
-- Iceberg Verify 7: Grand Totals Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_holdings = 25
ASSERT VALUE total_market_value = 470295.4
ASSERT VALUE total_cost_basis = 354170.0
SELECT
    COUNT(*) AS total_holdings,
    ROUND(SUM(shares * market_price), 2) AS total_market_value,
    ROUND(SUM(shares * cost_basis), 2) AS total_cost_basis
FROM {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg;
