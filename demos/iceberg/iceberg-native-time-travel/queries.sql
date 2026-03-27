-- ============================================================================
-- Iceberg Native Time Travel (Stock Prices) — Queries
-- ============================================================================
-- Demonstrates reading the final state of a native Iceberg V2 table after
-- 4 snapshot-producing mutations: initial load, tech price correction,
-- IPO insertions, and delisted ticker removals. All queries are read-only
-- and verify the current snapshot correctly reflects all mutations.
--
-- The table uses merge-on-read (position delete files) for UPDATE and
-- DELETE operations, which Delta Forge resolves transparently.
-- ============================================================================


-- ============================================================================
-- Query 1: Total Row Count After All Mutations
-- ============================================================================
-- Started with 120 rows, added 30 (IPO), deleted 12 (delisted) = 138.
-- UPDATE doesn't change count (same 120 rows, just different prices).

ASSERT ROW_COUNT = 138
ASSERT VALUE company_name = 'Bank of America' WHERE ticker = 'BAC'
ASSERT VALUE sector = 'Technology' WHERE ticker = 'AAPL'
ASSERT VALUE company_name = 'BioTech Innovations' WHERE ticker = 'BIOT'
SELECT * FROM {{zone_name}}.iceberg.stock_prices;


-- ============================================================================
-- Query 2: Per-Sector Breakdown
-- ============================================================================
-- Technology: 8 original + 2 IPO = 10 tickers x 6 days = 60 rows
-- Healthcare: 4 original + 1 IPO = 5 tickers x 6 days = 30 rows
-- Finance: 4 original + 1 IPO = 5 tickers x 6 days = 30 rows
-- Energy: 4 original - 2 delisted = 2 tickers x 6 days = 18 rows (lost COP, SLB; keep XOM, CVX)
-- Subtotal verification: 60 + 30 + 30 + 18 = 138 (cross-checks with Query 1)

ASSERT ROW_COUNT = 4
ASSERT VALUE cnt = 60 WHERE sector = 'Technology'
ASSERT VALUE cnt = 30 WHERE sector = 'Healthcare'
ASSERT VALUE cnt = 30 WHERE sector = 'Finance'
ASSERT VALUE cnt = 18 WHERE sector = 'Energy'
SELECT
    sector,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg.stock_prices
GROUP BY sector
ORDER BY sector;


-- ============================================================================
-- Query 3: Average Price Per Sector
-- ============================================================================
-- Tech prices were bumped +5% in snapshot 2. Other sectors unchanged.
-- IPO tickers have their own price ranges. Delisted rows removed.

ASSERT ROW_COUNT = 4
ASSERT VALUE avg_price = 96.77 WHERE sector = 'Energy'
ASSERT VALUE avg_price = 154.80 WHERE sector = 'Finance'
ASSERT VALUE avg_price = 184.64 WHERE sector = 'Healthcare'
ASSERT VALUE avg_price = 351.81 WHERE sector = 'Technology'
SELECT
    sector,
    ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.iceberg.stock_prices
GROUP BY sector
ORDER BY sector;


-- ============================================================================
-- Query 4: Volume Aggregations by Sector
-- ============================================================================
-- Total trading volume per sector — exercises LONG/BIGINT summation.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_volume = 649437706 WHERE sector = 'Energy'
ASSERT VALUE total_volume = 1079016621 WHERE sector = 'Finance'
ASSERT VALUE total_volume = 1211754672 WHERE sector = 'Healthcare'
ASSERT VALUE total_volume = 2315384878 WHERE sector = 'Technology'
SELECT
    sector,
    SUM(volume) AS total_volume
FROM {{zone_name}}.iceberg.stock_prices
GROUP BY sector
ORDER BY sector;


-- ============================================================================
-- Query 5: Verify Delisted Tickers Are Absent
-- ============================================================================
-- COP (ConocoPhillips) and SLB (Schlumberger) were deleted in snapshot 4.
-- Zero rows should match.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg.stock_prices
WHERE ticker IN ('COP', 'SLB');


-- ============================================================================
-- Query 6: Verify IPO Tickers Are Present
-- ============================================================================
-- 5 IPO tickers x 6 trading days = 30 rows inserted in snapshot 3.

ASSERT ROW_COUNT = 30
ASSERT VALUE sector = 'Healthcare' WHERE ticker = 'BIOT'
ASSERT VALUE sector = 'Finance' WHERE ticker = 'FINX'
ASSERT VALUE sector = 'Energy' WHERE ticker = 'GRNH'
ASSERT VALUE sector = 'Technology' WHERE ticker = 'NWAI'
ASSERT VALUE sector = 'Technology' WHERE ticker = 'QCMP'
SELECT
    ticker,
    company_name,
    sector,
    trade_date,
    price
FROM {{zone_name}}.iceberg.stock_prices
WHERE ticker IN ('BIOT', 'FINX', 'GRNH', 'NWAI', 'QCMP')
ORDER BY ticker, trade_date;


-- ============================================================================
-- Query 7: Distinct Counts
-- ============================================================================
-- 20 original - 2 delisted + 5 IPO = 23 distinct tickers.
-- 4 sectors, 6 trading days.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_tickers = 23
ASSERT VALUE distinct_sectors = 4
ASSERT VALUE distinct_dates = 6
SELECT
    COUNT(DISTINCT ticker) AS distinct_tickers,
    COUNT(DISTINCT sector) AS distinct_sectors,
    COUNT(DISTINCT trade_date) AS distinct_dates
FROM {{zone_name}}.iceberg.stock_prices;


-- ============================================================================
-- Query 8: Describe History
-- ============================================================================
-- Show snapshot history for the Iceberg table (if supported).
-- Expected: 4 snapshots (append, overwrite, append, delete).

DESCRIBE HISTORY {{zone_name}}.iceberg.stock_prices;


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
FROM {{zone_name}}.iceberg.stock_prices;
