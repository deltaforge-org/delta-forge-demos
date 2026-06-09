-- ============================================================================
-- Iceberg Native Time Travel (Stock Prices) — Queries
-- ============================================================================
-- Demonstrates reading the final state of a native Iceberg V2 table after
-- 4 snapshot-producing mutations: initial load, tech price correction,
-- IPO insertions, and delisted ticker removals. All queries are read-only
-- and verify the current snapshot correctly reflects all mutations.
--
-- The table uses merge-on-read (position delete files) for UPDATE and
-- DELETE operations, which DeltaForge resolves transparently.
-- ============================================================================


-- ============================================================================
-- Query 1: Full Table Scan — Row Count and Spot-Check Data
-- ============================================================================
-- Started with 120 rows, added 30 (IPO), deleted 12 (delisted) = 138.
-- Spot-check one original, one tech-updated, and one IPO row.

ASSERT ROW_COUNT = 138
ASSERT VALUE company_name = 'Bank of America' WHERE ticker = 'BAC'
ASSERT VALUE sector = 'Technology' WHERE ticker = 'AAPL'
ASSERT VALUE company_name = 'BioTech Innovations' WHERE ticker = 'BIOT'
SELECT * FROM {{zone_name}}.iceberg_demos.stock_prices;


-- ============================================================================
-- Query 2: Per-Sector Breakdown
-- ============================================================================
-- Technology: 10 tickers x 6 days = 60; Healthcare: 5 x 6 = 30;
-- Finance: 5 x 6 = 30; Energy: 2 x 6 = 18. Total = 138.

ASSERT ROW_COUNT = 4
ASSERT VALUE cnt = 60 WHERE sector = 'Technology'
ASSERT VALUE cnt = 30 WHERE sector = 'Healthcare'
ASSERT VALUE cnt = 30 WHERE sector = 'Finance'
ASSERT VALUE cnt = 18 WHERE sector = 'Energy'
SELECT
    sector,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.stock_prices
GROUP BY sector
ORDER BY sector;


-- ============================================================================
-- Query 3: Per-Ticker Aggregation — Verify Every Ticker's Avg Price
-- ============================================================================
-- All 23 tickers should appear with 6 rows each. Average prices reflect
-- the +5% tech correction and IPO inserts.

ASSERT ROW_COUNT = 23
ASSERT VALUE avg_price = 196.40 WHERE ticker = 'AAPL'
ASSERT VALUE avg_price = 438.53 WHERE ticker = 'MSFT'
ASSERT VALUE avg_price = 925.06 WHERE ticker = 'NVDA'
ASSERT VALUE avg_price = 195.18 WHERE ticker = 'JPM'
ASSERT VALUE avg_price = 104.69 WHERE ticker = 'XOM'
ASSERT VALUE avg_price = 78.86 WHERE ticker = 'BIOT'
ASSERT VALUE avg_price = 45.31 WHERE ticker = 'NWAI'
ASSERT VALUE avg_price = 119.67 WHERE ticker = 'QCMP'
SELECT
    ticker,
    ROUND(AVG(price), 2) AS avg_price,
    SUM(volume) AS total_volume,
    COUNT(*) AS row_count
FROM {{zone_name}}.iceberg_demos.stock_prices
GROUP BY ticker
ORDER BY ticker;


-- ============================================================================
-- Query 4: Surviving Original Row — JPM (Finance, Unchanged)
-- ============================================================================
-- JPM was not updated or deleted. Verify all 6 rows with exact prices.

ASSERT ROW_COUNT = 6
ASSERT VALUE price = 196.12 WHERE trade_date = '2025-01-06'
ASSERT VALUE price = 200.49 WHERE trade_date = '2025-01-07'
ASSERT VALUE price = 192.15 WHERE trade_date = '2025-01-08'
ASSERT VALUE price = 191.77 WHERE trade_date = '2025-01-09'
ASSERT VALUE price = 194.66 WHERE trade_date = '2025-01-10'
ASSERT VALUE price = 195.91 WHERE trade_date = '2025-01-13'
SELECT ticker, trade_date, price, volume, market_cap
FROM {{zone_name}}.iceberg_demos.stock_prices
WHERE ticker = 'JPM'
ORDER BY trade_date;


-- ============================================================================
-- Query 5: Tech-Updated Row — MSFT (+5% Price Correction)
-- ============================================================================
-- MSFT prices were bumped +5% in snapshot 2. Verify corrected prices.

ASSERT ROW_COUNT = 6
ASSERT VALUE price = 431.54 WHERE trade_date = '2025-01-06'
ASSERT VALUE price = 442.56 WHERE trade_date = '2025-01-07'
ASSERT VALUE price = 435.46 WHERE trade_date = '2025-01-08'
ASSERT VALUE price = 432.60 WHERE trade_date = '2025-01-09'
ASSERT VALUE price = 439.43 WHERE trade_date = '2025-01-10'
ASSERT VALUE price = 449.61 WHERE trade_date = '2025-01-13'
SELECT ticker, trade_date, price, volume, market_cap
FROM {{zone_name}}.iceberg_demos.stock_prices
WHERE ticker = 'MSFT'
ORDER BY trade_date;


-- ============================================================================
-- Query 6: Verify Delisted Tickers Are Absent
-- ============================================================================
-- COP and SLB were deleted in snapshot 4. Zero rows should match.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.stock_prices
WHERE ticker IN ('COP', 'SLB');


-- ============================================================================
-- Query 7: Surviving Energy Tickers — XOM and CVX
-- ============================================================================
-- COP and SLB were deleted but XOM and CVX must survive. Verify prices.

ASSERT ROW_COUNT = 6
ASSERT VALUE price = 151.81 WHERE trade_date = '2025-01-06'
ASSERT VALUE price = 156.65 WHERE trade_date = '2025-01-10'
SELECT ticker, trade_date, price, volume
FROM {{zone_name}}.iceberg_demos.stock_prices
WHERE ticker = 'CVX'
ORDER BY trade_date;


-- ============================================================================
-- Query 7b: Surviving Energy — XOM
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE price = 108.08 WHERE trade_date = '2025-01-06'
ASSERT VALUE price = 103.08 WHERE trade_date = '2025-01-10'
SELECT ticker, trade_date, price, volume
FROM {{zone_name}}.iceberg_demos.stock_prices
WHERE ticker = 'XOM'
ORDER BY trade_date;


-- ============================================================================
-- Query 8: IPO Ticker Data — NWAI (NewAI Corp)
-- ============================================================================
-- Verify all 6 IPO rows for NWAI with exact prices from snapshot 3.

ASSERT ROW_COUNT = 6
ASSERT VALUE company_name = 'NewAI Corp' WHERE trade_date = '2025-01-06'
ASSERT VALUE price = 45.37 WHERE trade_date = '2025-01-06'
ASSERT VALUE price = 45.67 WHERE trade_date = '2025-01-07'
ASSERT VALUE price = 45.53 WHERE trade_date = '2025-01-08'
ASSERT VALUE price = 44.82 WHERE trade_date = '2025-01-09'
ASSERT VALUE price = 45.07 WHERE trade_date = '2025-01-10'
ASSERT VALUE price = 45.40 WHERE trade_date = '2025-01-13'
SELECT ticker, company_name, trade_date, price, volume, market_cap
FROM {{zone_name}}.iceberg_demos.stock_prices
WHERE ticker = 'NWAI'
ORDER BY trade_date;


-- ============================================================================
-- Query 9: All IPO Tickers — Sector Verification
-- ============================================================================
-- 5 IPO tickers x 6 trading days = 30 rows. Verify each ticker's sector.

ASSERT ROW_COUNT = 30
ASSERT VALUE sector = 'Healthcare' WHERE ticker = 'BIOT'
ASSERT VALUE sector = 'Finance' WHERE ticker = 'FINX'
ASSERT VALUE sector = 'Energy' WHERE ticker = 'GRNH'
ASSERT VALUE sector = 'Technology' WHERE ticker = 'NWAI'
ASSERT VALUE sector = 'Technology' WHERE ticker = 'QCMP'
SELECT ticker, company_name, sector, trade_date, price
FROM {{zone_name}}.iceberg_demos.stock_prices
WHERE ticker IN ('BIOT', 'FINX', 'GRNH', 'NWAI', 'QCMP')
ORDER BY ticker, trade_date;


-- ============================================================================
-- Query 10: Per-Date Aggregation
-- ============================================================================
-- 23 tickers x 6 dates. Verify row counts and avg prices per date.

ASSERT ROW_COUNT = 6
ASSERT VALUE cnt = 23 WHERE trade_date = '2025-01-06'
ASSERT VALUE avg_price = 240.56 WHERE trade_date = '2025-01-06'
ASSERT VALUE avg_price = 239.74 WHERE trade_date = '2025-01-07'
ASSERT VALUE avg_price = 238.48 WHERE trade_date = '2025-01-08'
ASSERT VALUE avg_price = 237.45 WHERE trade_date = '2025-01-09'
ASSERT VALUE avg_price = 240.36 WHERE trade_date = '2025-01-10'
ASSERT VALUE avg_price = 239.66 WHERE trade_date = '2025-01-13'
SELECT
    trade_date,
    COUNT(*) AS cnt,
    ROUND(AVG(price), 2) AS avg_price,
    SUM(volume) AS total_volume
FROM {{zone_name}}.iceberg_demos.stock_prices
GROUP BY trade_date
ORDER BY trade_date;


-- ============================================================================
-- Query 11: Describe History
-- ============================================================================
-- Show snapshot history for the Iceberg table.
-- Expected: 4 snapshots (append, overwrite, append, delete).

ASSERT WARNING ROW_COUNT >= 4
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.stock_prices;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check combining all key invariants.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 138
ASSERT VALUE sector_count = 4
ASSERT VALUE distinct_tickers = 23
ASSERT VALUE ipo_tickers = 30
ASSERT VALUE delisted_tickers = 0
ASSERT VALUE grand_avg_price = 239.37
ASSERT VALUE grand_total_volume = 5255593877
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT sector) AS sector_count,
    COUNT(DISTINCT ticker) AS distinct_tickers,
    SUM(CASE WHEN ticker IN ('BIOT', 'FINX', 'GRNH', 'NWAI', 'QCMP') THEN 1 ELSE 0 END) AS ipo_tickers,
    SUM(CASE WHEN ticker IN ('COP', 'SLB') THEN 1 ELSE 0 END) AS delisted_tickers,
    ROUND(AVG(price), 2) AS grand_avg_price,
    SUM(volume) AS grand_total_volume
FROM {{zone_name}}.iceberg_demos.stock_prices;
