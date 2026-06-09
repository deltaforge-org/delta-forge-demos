-- ============================================================================
-- Iceberg V3 Position Delete Files — Queries
-- ============================================================================
-- Demonstrates Iceberg position delete file handling: DeltaForge reads the
-- V3 metadata chain (upgraded from V2), discovers the position delete file,
-- and applies row-level deletions before returning results. The dataset
-- contains 480 equity trades with 24 erroneous ALGO-X99 trades retracted
-- via position deletes, leaving 456 valid trades. All queries are read-only.
-- ============================================================================


-- ============================================================================
-- Query 1: Post-Delete Row Count
-- ============================================================================
-- Verifies that DeltaForge correctly applies the position delete file.
-- Original table has 480 rows; 24 are marked for deletion, leaving 456.

ASSERT ROW_COUNT = 456
SELECT * FROM {{zone_name}}.iceberg_demos.equity_trades;


-- ============================================================================
-- Query 2: Erroneous Trader Completely Removed
-- ============================================================================
-- All 24 trades from the malfunctioning algorithm ALGO-X99 should be gone.
-- If position deletes are applied correctly, zero rows match.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.equity_trades
WHERE trader = 'ALGO-X99';


-- ============================================================================
-- Query 3: Per-Exchange Trade Counts
-- ============================================================================
-- Trade distribution across the 4 exchanges after position deletes.

ASSERT ROW_COUNT = 4
ASSERT VALUE trade_count = 120 WHERE exchange = 'LSE'
ASSERT VALUE trade_count = 120 WHERE exchange = 'NASDAQ'
ASSERT VALUE trade_count = 96 WHERE exchange = 'NYSE'
ASSERT VALUE trade_count = 120 WHERE exchange = 'TSE'
SELECT
    exchange,
    COUNT(*) AS trade_count
FROM {{zone_name}}.iceberg_demos.equity_trades
GROUP BY exchange
ORDER BY exchange;


-- ============================================================================
-- Query 4: Distinct Trader Count
-- ============================================================================
-- ALGO-X99 should be removed from the trader set, leaving 10 traders.

ASSERT VALUE trader_count = 10
SELECT
    COUNT(DISTINCT trader) AS trader_count
FROM {{zone_name}}.iceberg_demos.equity_trades;


-- ============================================================================
-- Query 5: Per-Exchange Notional Volume
-- ============================================================================
-- Total notional value (price * quantity) per exchange after deletes.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_notional = 72660043.35 WHERE exchange = 'LSE'
ASSERT VALUE total_notional = 77910888.46 WHERE exchange = 'NASDAQ'
ASSERT VALUE total_notional = 61730838.44 WHERE exchange = 'NYSE'
ASSERT VALUE total_notional = 82249678.63 WHERE exchange = 'TSE'
SELECT
    exchange,
    ROUND(SUM(notional), 2) AS total_notional
FROM {{zone_name}}.iceberg_demos.equity_trades
GROUP BY exchange
ORDER BY exchange;


-- ============================================================================
-- Query 6: Buy vs Sell Counts
-- ============================================================================
-- Side distribution across all valid trades.

ASSERT ROW_COUNT = 2
ASSERT VALUE trade_count = 237 WHERE side = 'BUY'
ASSERT VALUE trade_count = 219 WHERE side = 'SELL'
SELECT
    side,
    COUNT(*) AS trade_count
FROM {{zone_name}}.iceberg_demos.equity_trades
GROUP BY side
ORDER BY side;


-- ============================================================================
-- Query 7: Average Price by Exchange
-- ============================================================================
-- Floating-point aggregation per exchange after position deletes.

ASSERT ROW_COUNT = 4
ASSERT VALUE avg_price = 251.21 WHERE exchange = 'LSE'
ASSERT VALUE avg_price = 233.7 WHERE exchange = 'NASDAQ'
ASSERT VALUE avg_price = 249.17 WHERE exchange = 'NYSE'
ASSERT VALUE avg_price = 256.13 WHERE exchange = 'TSE'
SELECT
    exchange,
    ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.iceberg_demos.equity_trades
GROUP BY exchange
ORDER BY exchange;


-- ============================================================================
-- Query 8: Price Range (Overall)
-- ============================================================================
-- Min, max, and average price across all valid trades.

ASSERT ROW_COUNT = 1
ASSERT VALUE min_price = 10.3
ASSERT VALUE max_price = 499.03
ASSERT VALUE avg_price = 247.47
SELECT
    ROUND(MIN(price), 2) AS min_price,
    ROUND(MAX(price), 2) AS max_price,
    ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.iceberg_demos.equity_trades;


-- ============================================================================
-- Query 9: Per-Exchange Side Breakdown
-- ============================================================================
-- Detailed buy/sell counts per exchange after position deletes.

ASSERT ROW_COUNT = 8
ASSERT VALUE trade_count = 64 WHERE exchange_side = 'LSE/BUY'
ASSERT VALUE trade_count = 56 WHERE exchange_side = 'LSE/SELL'
ASSERT VALUE trade_count = 65 WHERE exchange_side = 'NASDAQ/BUY'
ASSERT VALUE trade_count = 55 WHERE exchange_side = 'NASDAQ/SELL'
ASSERT VALUE trade_count = 42 WHERE exchange_side = 'NYSE/BUY'
ASSERT VALUE trade_count = 54 WHERE exchange_side = 'NYSE/SELL'
ASSERT VALUE trade_count = 66 WHERE exchange_side = 'TSE/BUY'
ASSERT VALUE trade_count = 54 WHERE exchange_side = 'TSE/SELL'
SELECT
    exchange || '/' || side AS exchange_side,
    COUNT(*) AS trade_count
FROM {{zone_name}}.iceberg_demos.equity_trades
GROUP BY exchange, side
ORDER BY exchange, side;


-- ============================================================================
-- Query 10: High-Value Trades (>$100K Notional)
-- ============================================================================
-- Predicate pushdown on double column — counts trades with notional > 100000.

ASSERT ROW_COUNT = 385
SELECT *
FROM {{zone_name}}.iceberg_demos.equity_trades
WHERE notional > 100000
ORDER BY notional DESC;


-- ============================================================================
-- Query 11: Distinct Entity Counts
-- ============================================================================
-- Exercises COUNT(DISTINCT ...) across the post-delete dataset.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_trades = 456
ASSERT VALUE distinct_exchanges = 4
ASSERT VALUE distinct_symbols = 20
ASSERT VALUE distinct_traders = 10
SELECT
    COUNT(DISTINCT trade_id) AS distinct_trades,
    COUNT(DISTINCT exchange) AS distinct_exchanges,
    COUNT(DISTINCT symbol) AS distinct_symbols,
    COUNT(DISTINCT trader) AS distinct_traders
FROM {{zone_name}}.iceberg_demos.equity_trades;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, key counts, and position-delete
-- invariants. A user who runs only this query can verify the Iceberg V3
-- reader correctly applies position delete files.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 456
ASSERT VALUE exchange_count = 4
ASSERT VALUE trader_count = 10
ASSERT VALUE symbol_count = 20
ASSERT VALUE erroneous_rows = 0
ASSERT VALUE buy_count = 237
ASSERT VALUE sell_count = 219
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT exchange) AS exchange_count,
    COUNT(DISTINCT trader) AS trader_count,
    COUNT(DISTINCT symbol) AS symbol_count,
    SUM(CASE WHEN trader = 'ALGO-X99' THEN 1 ELSE 0 END) AS erroneous_rows,
    SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) AS buy_count,
    SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sell_count
FROM {{zone_name}}.iceberg_demos.equity_trades;
