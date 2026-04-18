-- ============================================================================
-- Mira's Mercantile — Retail Analytics Chart Gallery
-- ============================================================================
-- WHAT: Renders all 10 CREATE CHART visualization types (BAR, HBAR, LINE,
--       AREA, SCATTER, PIE, HISTOGRAM, HEATMAP, RADAR, CANDLESTICK) against
--       a unified retail dataset (80 sales rows + 10 weeks of stock OHLC).
-- WHY:  Every chart in the GUI's Query Explorer must render correctly from
--       the same SQL the user can type. Each chart is preceded by a
--       validated SELECT proving the underlying aggregation is correct.
-- HOW:  1) ASSERTs verify the aggregation -> 2) CREATE CHART re-runs the
--       same query and renders SVG.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — full tables
-- ============================================================================
-- Both Delta tables loaded with deterministic data: 80 sales rows across 4
-- stores x 4 categories x 5 weekdays, plus 10 weeks of parent-ticker OHLC.

ASSERT ROW_COUNT = 80
ASSERT VALUE distinct_stores = 4
ASSERT VALUE distinct_categories = 4
ASSERT VALUE distinct_days = 5
SELECT COUNT(*)                          AS row_count_placeholder,
       COUNT(DISTINCT store_name)        AS distinct_stores,
       COUNT(DISTINCT category)          AS distinct_categories,
       COUNT(DISTINCT txn_date)          AS distinct_days
FROM {{zone_name}}.retail.sales_daily;

ASSERT ROW_COUNT = 10
SELECT * FROM {{zone_name}}.retail.stock_prices;


-- ============================================================================
-- CHART 1 / BAR — Revenue by Product Category
-- ============================================================================
-- Bar chart: discrete categorical x-axis, one bar per category.
-- Validates each category's total revenue before rendering the same query.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 36419.70 WHERE category = 'Electronics'
ASSERT VALUE total_revenue = 24078.60 WHERE category = 'Home'
ASSERT VALUE total_revenue = 21730.40 WHERE category = 'Apparel'
ASSERT VALUE total_revenue = 13420.80 WHERE category = 'Beauty'
SELECT category, ROUND(SUM(revenue), 2) AS total_revenue
FROM {{zone_name}}.retail.sales_daily
GROUP BY category
ORDER BY total_revenue DESC;

CREATE CHART BAR FROM (
    SELECT category, ROUND(SUM(revenue), 2) AS total_revenue
    FROM {{zone_name}}.retail.sales_daily
    GROUP BY category
    ORDER BY total_revenue DESC
) X category Y total_revenue
TITLE 'Revenue by Product Category'
SUBTITLE '5-day window, all stores'
XLABEL 'Category' YLABEL 'Revenue (USD)'
VALUES ON;


-- ============================================================================
-- CHART 2 / HBAR — Units Sold by Store (Ranked)
-- ============================================================================
-- Horizontal bar: best for ranked comparisons with long category labels.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_units = 654 WHERE store_name = 'Mall'
ASSERT VALUE total_units = 520 WHERE store_name = 'Downtown'
ASSERT VALUE total_units = 427 WHERE store_name = 'Beach'
ASSERT VALUE total_units = 384 WHERE store_name = 'Airport'
SELECT store_name, SUM(units_sold) AS total_units
FROM {{zone_name}}.retail.sales_daily
GROUP BY store_name
ORDER BY total_units DESC;

CREATE CHART HBAR FROM (
    SELECT store_name, SUM(units_sold) AS total_units
    FROM {{zone_name}}.retail.sales_daily
    GROUP BY store_name
    ORDER BY total_units DESC
) X store_name Y total_units
TITLE 'Units Sold by Store' VALUES ON;


-- ============================================================================
-- CHART 3 / LINE — Daily Revenue Trend (Smoothed)
-- ============================================================================
-- Line with SMOOTH option: shows trend across continuous time dimension.
-- Traffic builds through the week, peaking Friday.

ASSERT ROW_COUNT = 5
ASSERT VALUE day_revenue = 18225.00 WHERE day = '2026-03-02'
ASSERT VALUE day_revenue = 17313.75 WHERE day = '2026-03-03'
ASSERT VALUE day_revenue = 21378.00 WHERE day = '2026-03-06'
SELECT CAST(txn_date AS VARCHAR) AS day,
       ROUND(SUM(revenue), 2)    AS day_revenue
FROM {{zone_name}}.retail.sales_daily
GROUP BY txn_date
ORDER BY txn_date;

CREATE CHART LINE FROM (
    SELECT CAST(txn_date AS VARCHAR) AS day,
           ROUND(SUM(revenue), 2)    AS day_revenue
    FROM {{zone_name}}.retail.sales_daily
    GROUP BY txn_date
    ORDER BY txn_date
) X day Y day_revenue
SMOOTH
TITLE 'Daily Revenue Trend'
SUBTITLE 'All stores combined'
XLABEL 'Date' YLABEL 'Revenue (USD)';


-- ============================================================================
-- CHART 4 / AREA — Daily Revenue by Category (Stacked)
-- ============================================================================
-- Stacked area: show how each category contributes to the daily total.
-- GROUP BY category pivots the single Y column into 4 series.

ASSERT ROW_COUNT = 20
ASSERT VALUE day_cat_revenue = 4857.60 WHERE day = '2026-03-06' AND category = 'Apparel'
ASSERT VALUE day_cat_revenue = 8146.80 WHERE day = '2026-03-06' AND category = 'Electronics'
ASSERT VALUE day_cat_revenue = 5378.40 WHERE day = '2026-03-06' AND category = 'Home'
ASSERT VALUE day_cat_revenue = 2995.20 WHERE day = '2026-03-06' AND category = 'Beauty'
SELECT CAST(txn_date AS VARCHAR) AS day,
       category,
       ROUND(SUM(revenue), 2)    AS day_cat_revenue
FROM {{zone_name}}.retail.sales_daily
GROUP BY txn_date, category
ORDER BY txn_date, category;

CREATE CHART AREA FROM (
    SELECT CAST(txn_date AS VARCHAR) AS day,
           category,
           ROUND(SUM(revenue), 2)    AS day_cat_revenue
    FROM {{zone_name}}.retail.sales_daily
    GROUP BY txn_date, category
    ORDER BY txn_date, category
) X day Y day_cat_revenue GROUP BY category
STACKED SMOOTH
TITLE 'Daily Revenue by Category'
SUBTITLE 'Stacked contribution to daily total'
XLABEL 'Date' YLABEL 'Revenue (USD)';


-- ============================================================================
-- CHART 5 / SCATTER — Discount % vs Units Sold
-- ============================================================================
-- Scatter: show correlation between two numeric dimensions across all rows.
-- Heavily-discounted categories (Beauty at 20-22%) move more units.

ASSERT ROW_COUNT = 1
ASSERT VALUE corr_val > 0.5
ASSERT VALUE corr_val < 0.9
SELECT ROUND(corr(discount_pct, CAST(units_sold AS DOUBLE)), 4) AS corr_val
FROM {{zone_name}}.retail.sales_daily;

CREATE CHART SCATTER FROM (
    SELECT CAST(ROUND(discount_pct * 100, 0) AS VARCHAR) AS discount_label,
           units_sold
    FROM {{zone_name}}.retail.sales_daily
    ORDER BY discount_pct
) X discount_label Y units_sold
TITLE 'Discount % vs Units Sold (all rows)'
XLABEL 'Discount %' YLABEL 'Units';


-- ============================================================================
-- CHART 6 / PIE — Revenue Share by Category
-- ============================================================================
-- Pie: each slice's share of the whole. Bucket ordering matters; the executor
-- auto-sorts slices and buckets tail <12 into "Other" (we only have 4).

ASSERT VALUE pct_electronics = 38.08
ASSERT VALUE pct_home = 25.17
ASSERT VALUE pct_apparel = 22.72
ASSERT VALUE pct_beauty = 14.03
SELECT
    ROUND(SUM(CASE WHEN category = 'Electronics' THEN revenue ELSE 0 END) * 100.0 / SUM(revenue), 2) AS pct_electronics,
    ROUND(SUM(CASE WHEN category = 'Home'        THEN revenue ELSE 0 END) * 100.0 / SUM(revenue), 2) AS pct_home,
    ROUND(SUM(CASE WHEN category = 'Apparel'     THEN revenue ELSE 0 END) * 100.0 / SUM(revenue), 2) AS pct_apparel,
    ROUND(SUM(CASE WHEN category = 'Beauty'      THEN revenue ELSE 0 END) * 100.0 / SUM(revenue), 2) AS pct_beauty
FROM {{zone_name}}.retail.sales_daily;

CREATE CHART PIE FROM (
    SELECT category, ROUND(SUM(revenue), 2) AS total_revenue
    FROM {{zone_name}}.retail.sales_daily
    GROUP BY category
) X category Y total_revenue
TITLE 'Revenue Share by Category'
VALUES ON LEGEND RIGHT;


-- ============================================================================
-- CHART 7 / HISTOGRAM — Distribution of Per-Row Revenue
-- ============================================================================
-- Histogram with BINS 8: bucket the 80 per-row revenue values into 8 bins
-- and count frequency. Proves the shape of the revenue distribution.

ASSERT VALUE rows_below_600 = 8
ASSERT VALUE rows_600_to_1500 = 52
ASSERT VALUE rows_above_2000 = 7
ASSERT VALUE min_revenue = 456.0
ASSERT VALUE max_revenue = 2678.40
SELECT
    SUM(CASE WHEN revenue < 600.0                      THEN 1 ELSE 0 END) AS rows_below_600,
    SUM(CASE WHEN revenue >= 600.0 AND revenue < 1500.0 THEN 1 ELSE 0 END) AS rows_600_to_1500,
    SUM(CASE WHEN revenue >= 2000.0                     THEN 1 ELSE 0 END) AS rows_above_2000,
    ROUND(MIN(revenue), 2) AS min_revenue,
    ROUND(MAX(revenue), 2) AS max_revenue
FROM {{zone_name}}.retail.sales_daily;

CREATE CHART HISTOGRAM FROM (
    SELECT revenue FROM {{zone_name}}.retail.sales_daily
) X revenue BINS 8
TITLE 'Per-Row Revenue Distribution'
XLABEL 'Revenue bucket' YLABEL 'Frequency';


-- ============================================================================
-- CHART 8 / HEATMAP — Revenue by Store x Category
-- ============================================================================
-- Heatmap: matrix visualization with color-encoded values.
-- 4 stores x 4 categories = 16 cells.

ASSERT ROW_COUNT = 16
ASSERT VALUE revenue = 11973.60 WHERE store_name = 'Mall'     AND category = 'Electronics'
ASSERT VALUE revenue =  9978.00 WHERE store_name = 'Downtown' AND category = 'Electronics'
ASSERT VALUE revenue =  2516.40 WHERE store_name = 'Airport'  AND category = 'Beauty'
ASSERT VALUE revenue =  5350.80 WHERE store_name = 'Beach'    AND category = 'Home'
SELECT store_name, category, ROUND(SUM(revenue), 2) AS revenue
FROM {{zone_name}}.retail.sales_daily
GROUP BY store_name, category
ORDER BY store_name, category;

CREATE CHART HEATMAP FROM (
    SELECT store_name, category, ROUND(SUM(revenue), 2) AS revenue
    FROM {{zone_name}}.retail.sales_daily
    GROUP BY store_name, category
    ORDER BY store_name, category
) X store_name Y revenue GROUP BY category
TITLE 'Revenue Heatmap: Store x Category';


-- ============================================================================
-- CHART 9 / RADAR — Per-Store KPI Comparison
-- ============================================================================
-- Radar: compare multiple entities across multiple KPIs. Each indicator axis
-- auto-scales to its own max, so KPIs with different units display fairly.
-- 4 KPIs (Revenue, Units, Customers, AvgBasket) x 4 stores.

ASSERT ROW_COUNT = 16
ASSERT VALUE value = 25690.50 WHERE kpi = 'Revenue'   AND store_name = 'Downtown'
ASSERT VALUE value = 31253.30 WHERE kpi = 'Revenue'   AND store_name = 'Mall'
ASSERT VALUE value =   654.00 WHERE kpi = 'Units'     AND store_name = 'Mall'
ASSERT VALUE value =   187.00 WHERE kpi = 'Customers' AND store_name = 'Airport'
ASSERT VALUE value =   102.72 WHERE kpi = 'AvgBasket' AND store_name = 'Airport'
SELECT 'Revenue' AS kpi, store_name, ROUND(SUM(revenue), 2) AS value
FROM {{zone_name}}.retail.sales_daily GROUP BY store_name
UNION ALL
SELECT 'Units' AS kpi, store_name, CAST(SUM(units_sold) AS DOUBLE) AS value
FROM {{zone_name}}.retail.sales_daily GROUP BY store_name
UNION ALL
SELECT 'Customers' AS kpi, store_name, CAST(SUM(customers) AS DOUBLE) AS value
FROM {{zone_name}}.retail.sales_daily GROUP BY store_name
UNION ALL
SELECT 'AvgBasket' AS kpi, store_name, ROUND(SUM(revenue) / SUM(customers), 2) AS value
FROM {{zone_name}}.retail.sales_daily GROUP BY store_name;

CREATE CHART RADAR FROM (
    SELECT 'Revenue' AS kpi, store_name, ROUND(SUM(revenue), 2) AS value
    FROM {{zone_name}}.retail.sales_daily GROUP BY store_name
    UNION ALL
    SELECT 'Units' AS kpi, store_name, CAST(SUM(units_sold) AS DOUBLE) AS value
    FROM {{zone_name}}.retail.sales_daily GROUP BY store_name
    UNION ALL
    SELECT 'Customers' AS kpi, store_name, CAST(SUM(customers) AS DOUBLE) AS value
    FROM {{zone_name}}.retail.sales_daily GROUP BY store_name
    UNION ALL
    SELECT 'AvgBasket' AS kpi, store_name, ROUND(SUM(revenue) / SUM(customers), 2) AS value
    FROM {{zone_name}}.retail.sales_daily GROUP BY store_name
) X kpi Y value GROUP BY store_name
TITLE 'Per-Store KPI Comparison';


-- ============================================================================
-- CHART 10 / CANDLESTICK — MIRA Weekly Stock OHLC
-- ============================================================================
-- Candlestick requires exactly 4 Y columns in order: open, close, high, low.
-- Green body = up week (close > open), red = down week. 10 weeks Jan-Mar 2026.

ASSERT ROW_COUNT = 10
ASSERT VALUE up_weeks = 8
ASSERT VALUE first_open = 52.0
ASSERT VALUE last_close = 60.8
ASSERT VALUE max_high = 61.4
ASSERT VALUE min_low = 48.5
SELECT
    SUM(CASE WHEN close_price > open_price THEN 1 ELSE 0 END) AS up_weeks,
    MIN(open_price)  FILTER (WHERE week_start = DATE '2026-01-05') AS first_open,
    MAX(close_price) FILTER (WHERE week_start = DATE '2026-03-09') AS last_close,
    MAX(high_price)  AS max_high,
    MIN(low_price)   AS min_low
FROM {{zone_name}}.retail.stock_prices;

CREATE CHART CANDLESTICK FROM (
    SELECT CAST(week_start AS VARCHAR) AS week,
           open_price, close_price, high_price, low_price
    FROM {{zone_name}}.retail.stock_prices
    ORDER BY week_start
) X week Y open_price, close_price, high_price, low_price
TITLE 'MIRA Weekly Stock Performance'
SUBTITLE '10 weeks: Jan-Mar 2026'
XLABEL 'Week' YLABEL 'Price (USD)';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check covering key invariants from the entire demo.

ASSERT VALUE sales_rows = 80
ASSERT VALUE stock_rows = 10
ASSERT VALUE total_revenue = 95649.50
ASSERT VALUE total_units = 1985
ASSERT VALUE total_customers = 975
ASSERT VALUE best_category = 'Electronics'
ASSERT VALUE best_store = 'Mall'
ASSERT VALUE stock_net_gain = 8.80
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.retail.sales_daily)                   AS sales_rows,
    (SELECT COUNT(*) FROM {{zone_name}}.retail.stock_prices)                  AS stock_rows,
    (SELECT ROUND(SUM(revenue), 2) FROM {{zone_name}}.retail.sales_daily)     AS total_revenue,
    (SELECT SUM(units_sold) FROM {{zone_name}}.retail.sales_daily)            AS total_units,
    (SELECT SUM(customers) FROM {{zone_name}}.retail.sales_daily)             AS total_customers,
    (SELECT category FROM (
        SELECT category, SUM(revenue) AS r
        FROM {{zone_name}}.retail.sales_daily
        GROUP BY category ORDER BY r DESC LIMIT 1
    ))                                                                        AS best_category,
    (SELECT store_name FROM (
        SELECT store_name, SUM(revenue) AS r
        FROM {{zone_name}}.retail.sales_daily
        GROUP BY store_name ORDER BY r DESC LIMIT 1
    ))                                                                        AS best_store,
    ROUND(
        (SELECT close_price FROM {{zone_name}}.retail.stock_prices WHERE week_start = DATE '2026-03-09')
        - (SELECT open_price FROM {{zone_name}}.retail.stock_prices WHERE week_start = DATE '2026-01-05'),
        2)                                                                    AS stock_net_gain;
