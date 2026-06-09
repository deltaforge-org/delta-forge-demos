-- ============================================================================
-- Delta Advanced SQL — Educational Queries
-- ============================================================================
-- WHAT: Advanced SQL analytics (window functions, CTEs, aggregations) on Delta
-- WHY:  Delta tables support full SQL semantics, enabling warehouse-grade
--       analytics on open table formats without vendor lock-in
-- HOW:  Delta stores data as Parquet files with a transaction log; the SQL
--       engine executes analytics identically to any relational database
-- ============================================================================


-- ============================================================================
-- EXPLORE: Dataset Overview
-- ============================================================================
-- Let's first understand the shape of our stock prices dataset.
-- This table has 5 tech stocks over 20 trading days with deterministic prices.

-- Verify each stock has exactly 20 trading days
ASSERT VALUE trading_days = 20 WHERE symbol = 'AAPL'
ASSERT VALUE trading_days = 20 WHERE symbol = 'GOOGL'
ASSERT VALUE trading_days = 20 WHERE symbol = 'MSFT'
ASSERT VALUE trading_days = 20 WHERE symbol = 'AMZN'
ASSERT VALUE trading_days = 20 WHERE symbol = 'TSLA'
ASSERT ROW_COUNT = 5
SELECT symbol,
       COUNT(*) AS trading_days,
       MIN(trade_date) AS first_date,
       MAX(trade_date) AS last_date,
       ROUND(MIN(close_price), 2) AS min_close,
       ROUND(MAX(close_price), 2) AS max_close
FROM {{zone_name}}.delta_demos.stock_prices
GROUP BY symbol
ORDER BY symbol;


-- ============================================================================
-- LEARN: LAG Window Function — Day-Over-Day Price Changes
-- ============================================================================
-- LAG(column) OVER (PARTITION BY ... ORDER BY ...) accesses a value from the
-- previous row within the same partition. This is the foundation of time-series
-- analysis: comparing today's value to yesterday's without a self-join.
-- The first row in each partition returns NULL (no previous value exists).

-- Verify 20 trading days for AAPL; day 2 (2024-01-03) should show a -0.70 drop from 186.50
ASSERT ROW_COUNT = 20
ASSERT VALUE daily_change = -0.70 WHERE trade_date = '2024-01-03'
SELECT symbol, trade_date, close_price,
       LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close,
       ROUND(close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date), 2) AS daily_change,
       CASE WHEN close_price > LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date)
            THEN 'UP' ELSE 'DOWN' END AS direction
FROM {{zone_name}}.delta_demos.stock_prices
WHERE symbol = 'AAPL'
ORDER BY trade_date;


-- ============================================================================
-- LEARN: Cumulative SUM — Running Total Volume
-- ============================================================================
-- SUM(...) OVER (ORDER BY ...) without explicit frame bounds defaults to
-- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, producing a running total.
-- This is useful for tracking cumulative metrics like total shares traded.

ASSERT ROW_COUNT = 20
ASSERT VALUE cumulative_volume = 1989000000 WHERE trade_date = '2024-01-30'
SELECT symbol, trade_date, volume,
       SUM(volume) OVER (PARTITION BY symbol ORDER BY trade_date) AS cumulative_volume
FROM {{zone_name}}.delta_demos.stock_prices
WHERE symbol = 'TSLA'
ORDER BY trade_date;


-- ============================================================================
-- LEARN: RANK — Finding Best and Worst Trading Days
-- ============================================================================
-- RANK() assigns the same rank to ties and skips subsequent values.
-- PARTITION BY symbol means each stock is ranked independently.
-- This finds which days had the largest intraday gains for MSFT.

ASSERT ROW_COUNT = 5
ASSERT VALUE daily_gain = 3.60 WHERE gain_rank = 1
SELECT symbol, trade_date,
       ROUND(close_price - open_price, 2) AS daily_gain,
       RANK() OVER (PARTITION BY symbol ORDER BY (close_price - open_price) DESC) AS gain_rank
FROM {{zone_name}}.delta_demos.stock_prices
WHERE symbol = 'MSFT'
ORDER BY gain_rank
LIMIT 5;


-- ============================================================================
-- LEARN: CTE — Monthly Return Calculation
-- ============================================================================
-- Common Table Expressions (CTEs) break complex queries into readable steps.
-- Here we use FIRST_VALUE and LAST_VALUE window functions inside a CTE to
-- compute each stock's return over the full period.
-- Note: LAST_VALUE requires an explicit frame (ROWS BETWEEN UNBOUNDED PRECEDING
-- AND UNBOUNDED FOLLOWING) to see the entire partition, not just up to current row.

-- Verify AAPL and TSLA monthly returns
ASSERT VALUE return_pct = 5.20 WHERE symbol = 'AAPL'
ASSERT VALUE return_pct = 9.66 WHERE symbol = 'TSLA'
ASSERT ROW_COUNT = 5
WITH first_last AS (
    SELECT symbol,
           FIRST_VALUE(close_price) OVER (PARTITION BY symbol ORDER BY trade_date) AS first_close,
           LAST_VALUE(close_price) OVER (PARTITION BY symbol ORDER BY trade_date
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
    FROM {{zone_name}}.delta_demos.stock_prices
),
distinct_fl AS (
    SELECT DISTINCT symbol, first_close, last_close FROM first_last
)
SELECT symbol,
       first_close,
       last_close,
       ROUND((last_close - first_close) / first_close * 100, 2) AS return_pct
FROM distinct_fl
ORDER BY return_pct DESC;


-- ============================================================================
-- LEARN: Moving Average — Smoothing Price Data
-- ============================================================================
-- A 3-day moving average smooths out daily noise by averaging the current row
-- with the 2 preceding rows: ROWS BETWEEN 2 PRECEDING AND CURRENT ROW.
-- This is the most common technical analysis indicator in finance.

ASSERT ROW_COUNT = 20
ASSERT VALUE ma_3day = 141.33 WHERE trade_date = '2024-01-04'
SELECT symbol, trade_date, close_price,
       ROUND(AVG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date
                                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ma_3day
FROM {{zone_name}}.delta_demos.stock_prices
WHERE symbol = 'GOOGL'
ORDER BY trade_date;


-- ============================================================================
-- LEARN: CTE + RANK — Volatility Analysis
-- ============================================================================
-- Volatility is measured here as the average daily price range (high - low)
-- as a percentage of the close price. More volatile stocks have wider ranges.
-- We chain two CTEs: one computes per-day ranges, the other averages them.

-- Verify TSLA is the most volatile stock (rank 1)
ASSERT VALUE volatility_rank = 1 WHERE symbol = 'TSLA'
ASSERT ROW_COUNT = 5
WITH daily_range AS (
    SELECT symbol, trade_date,
           ROUND((high_price - low_price) / close_price * 100, 2) AS range_pct
    FROM {{zone_name}}.delta_demos.stock_prices
),
avg_volatility AS (
    SELECT symbol,
           ROUND(AVG(range_pct), 2) AS avg_range_pct
    FROM daily_range
    GROUP BY symbol
)
SELECT symbol, avg_range_pct,
       RANK() OVER (ORDER BY avg_range_pct DESC) AS volatility_rank
FROM avg_volatility
ORDER BY volatility_rank;


-- ============================================================================
-- EXPLORE: ROW_NUMBER — Latest Close Price Per Stock
-- ============================================================================
-- ROW_NUMBER() assigns a unique sequential number within each partition.
-- By ordering DESC and filtering rn = 1, we get the most recent row per stock.
-- Unlike RANK, ROW_NUMBER never produces ties.

ASSERT ROW_COUNT = 5
ASSERT VALUE close_price = 196.20 WHERE symbol = 'AAPL'
ASSERT VALUE close_price = 272.50 WHERE symbol = 'TSLA'
SELECT symbol, trade_date, close_price
FROM (
    SELECT symbol, trade_date, close_price,
           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
    FROM {{zone_name}}.delta_demos.stock_prices
)
WHERE rn = 1
ORDER BY symbol;


-- ============================================================================
-- EXPLORE: Up/Down Day Ratio Per Stock
-- ============================================================================
-- Using FILTER (WHERE ...) with COUNT for conditional aggregation.
-- This calculates the percentage of days each stock closed higher than it opened.

ASSERT ROW_COUNT = 5
ASSERT VALUE up_days = 12 WHERE symbol = 'AAPL'
ASSERT VALUE up_pct = 60.0 WHERE symbol = 'AAPL'
WITH daily_changes AS (
    SELECT symbol, trade_date,
           close_price - open_price AS intraday_change
    FROM {{zone_name}}.delta_demos.stock_prices
)
SELECT symbol,
       COUNT(*) FILTER (WHERE intraday_change > 0) AS up_days,
       COUNT(*) FILTER (WHERE intraday_change < 0) AS down_days,
       COUNT(*) FILTER (WHERE intraday_change = 0) AS flat_days,
       ROUND(COUNT(*) FILTER (WHERE intraday_change > 0)::DOUBLE / COUNT(*) * 100, 1) AS up_pct
FROM daily_changes
GROUP BY symbol
ORDER BY up_pct DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Summary verification ensuring the dataset and analytics produce expected results.

-- Verify total row count
ASSERT ROW_COUNT = 100
SELECT * FROM {{zone_name}}.delta_demos.stock_prices;

-- Verify distinct symbol count
ASSERT VALUE distinct_symbols = 5
SELECT COUNT(DISTINCT symbol) AS distinct_symbols FROM {{zone_name}}.delta_demos.stock_prices;

-- Verify every stock has exactly 20 trading days
ASSERT VALUE bad_stock_count = 0
SELECT COUNT(*) AS bad_stock_count FROM (
    SELECT symbol, COUNT(*) AS c FROM {{zone_name}}.delta_demos.stock_prices GROUP BY symbol
) WHERE c != 20;

-- Verify AAPL monthly return calculation
ASSERT VALUE aapl_return = 5.20
SELECT ROUND((196.20 - 186.50) / 186.50 * 100, 2) AS aapl_return;

-- Verify TSLA is the most volatile stock
ASSERT VALUE most_volatile = 'TSLA'
SELECT symbol AS most_volatile FROM (
    SELECT symbol,
           ROUND(AVG((high_price - low_price) / close_price * 100), 2) AS r
    FROM {{zone_name}}.delta_demos.stock_prices GROUP BY symbol ORDER BY r DESC LIMIT 1
);

-- Verify exactly 9 high-volume trading days (volume > 100M, all TSLA)
ASSERT VALUE high_volume_days = 9
SELECT COUNT(*) AS high_volume_days
FROM {{zone_name}}.delta_demos.stock_prices
WHERE volume > 100000000;

-- Verify AAPL last close price
ASSERT VALUE close_price = 196.20
SELECT close_price FROM {{zone_name}}.delta_demos.stock_prices
WHERE symbol = 'AAPL' AND trade_date = '2024-01-30';
