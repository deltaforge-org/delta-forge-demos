-- ============================================================================
-- Demo: Pacific Retail Group: Power BI Star Warehouse
-- ============================================================================
-- Every assertion is closed-form: it depends only on the generation size, the
-- row-index primary keys, the fixed cyclic cardinalities, the deterministic
-- flags, or a bound/inequality that holds for ANY realisation of the seeded
-- distributions. None of them pin a seed-specific magic number, so the suite
-- stays meaningful as a regression guard and any drift is a real regression.
--
-- Query map:
--   Q1      dim_date integrity
--   Q2      dim_store integrity + geography cardinality
--   Q3      dim_product integrity
--   Q4      dim_customer integrity
--   Q5      dim_customer: number_of_children is realistic + decorrelated
--   Q6      dim_customer: geography is population-skewed
--   Q7      fact_sales integrity
--   Q8      fact_sales measure ranges (skewed but bounded)
--   Q9      fact_sales by channel (slicer)
--   Q10     fact_sales recency skew (growing business)
--   Q11     fact_sales x dim_product JOIN (brand line counts)
--   Q12     fact_inventory_snapshot integrity
--   Q13     fact_web_events integrity
--   Q14     fact_web_events funnel skew + conversion integrity
--   VERIFY  cross-cutting (count, key sum) per table
-- ============================================================================

-- ============================================================================
-- Query 1: dim_date integrity
-- ============================================================================
-- 7,305 rows for 2010-01-01 .. 2029-12-31. 20 distinct years. Exactly one
-- month-end per month: 12 * 20 = 240. Four seasons.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 7305
ASSERT VALUE n_years = 20
ASSERT VALUE min_date_key = 20100101
ASSERT VALUE max_date_key = 20291231
ASSERT VALUE n_month_ends = 240
ASSERT VALUE n_seasons = 4
SELECT
    COUNT(*)                                            AS n_rows,
    COUNT(DISTINCT year)                                AS n_years,
    MIN(date_key)                                       AS min_date_key,
    MAX(date_key)                                       AS max_date_key,
    SUM(CASE WHEN is_month_end THEN 1 ELSE 0 END)       AS n_month_ends,
    COUNT(DISTINCT season)                              AS n_seasons
FROM {{zone_name}}.retail.dim_date;

-- ============================================================================
-- Query 2: dim_store integrity and geography cardinality
-- ============================================================================
-- 25,000 rows. SUM(store_key) = 25000*25001/2 = 312,512,500. Stores sit in
-- 20 metros across 4 census regions; store_type and banner cycle over 5.
-- square_feet is right-skewed but clamped to [3000, 200000].

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 25000
ASSERT VALUE sum_store_key = 312512500
ASSERT VALUE n_store_types = 5
ASSERT VALUE n_banners = 5
ASSERT VALUE n_cities = 20
ASSERT VALUE n_regions = 4
ASSERT VALUE min_sqft >= 3000
ASSERT VALUE max_sqft <= 200000
SELECT
    COUNT(*)                    AS n_rows,
    SUM(store_key)               AS sum_store_key,
    COUNT(DISTINCT store_type)  AS n_store_types,
    COUNT(DISTINCT banner)      AS n_banners,
    COUNT(DISTINCT city)        AS n_cities,
    COUNT(DISTINCT region)      AS n_regions,
    MIN(square_feet)            AS min_sqft,
    MAX(square_feet)            AS max_sqft
FROM {{zone_name}}.retail.dim_store;

-- ============================================================================
-- Query 3: dim_product integrity
-- ============================================================================
-- 1,000,000 rows. SUM(product_key) = 1M*1,000,001/2 = 500,000,500,000.
-- 50 brands, 10 / 20 / 50 category levels, 4 ABC classes. unit_cost is
-- right-skewed and clamped to [0.5, 3000]; list_price is a 1.6x markup.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 1000000
ASSERT VALUE sum_product_key = 500000500000
ASSERT VALUE n_brands = 50
ASSERT VALUE n_l1 = 10
ASSERT VALUE n_l2 = 20
ASSERT VALUE n_l3 = 50
ASSERT VALUE n_abc = 4
ASSERT VALUE min_cost >= 0.5
ASSERT VALUE max_cost <= 3000
SELECT
    COUNT(*)                        AS n_rows,
    SUM(product_key)                 AS sum_product_key,
    COUNT(DISTINCT brand)           AS n_brands,
    COUNT(DISTINCT category_l1)     AS n_l1,
    COUNT(DISTINCT category_l2)     AS n_l2,
    COUNT(DISTINCT category_l3)     AS n_l3,
    COUNT(DISTINCT abc_class)       AS n_abc,
    MIN(unit_cost_usd)              AS min_cost,
    MAX(unit_cost_usd)              AS max_cost
FROM {{zone_name}}.retail.dim_product;

-- ============================================================================
-- Query 4: dim_customer integrity
-- ============================================================================
-- 5,000,000 rows. SUM(customer_key) = 5M*5,000,001/2 = 12,500,002,500,000.
-- 5 loyalty tiers, 5 segments, 20 cities, 4 regions. number_of_children is
-- in [0, 4] with all five values present; household_size is in [1, 6].

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 5000000
ASSERT VALUE sum_customer_key = 12500002500000
ASSERT VALUE n_tiers = 5
ASSERT VALUE n_segments = 5
ASSERT VALUE n_cities = 20
ASSERT VALUE n_regions = 4
ASSERT VALUE n_child_values = 5
ASSERT VALUE min_children = 0
ASSERT VALUE max_children = 4
ASSERT VALUE min_household >= 1
ASSERT VALUE max_household <= 6
SELECT
    COUNT(*)                            AS n_rows,
    SUM(customer_key)                    AS sum_customer_key,
    COUNT(DISTINCT loyalty_tier)        AS n_tiers,
    COUNT(DISTINCT segment)             AS n_segments,
    COUNT(DISTINCT city)                AS n_cities,
    COUNT(DISTINCT region)              AS n_regions,
    COUNT(DISTINCT number_of_children)  AS n_child_values,
    MIN(number_of_children)             AS min_children,
    MAX(number_of_children)             AS max_children,
    MIN(household_size)                 AS min_household,
    MAX(household_size)                 AS max_household
FROM {{zone_name}}.retail.dim_customer;

-- ============================================================================
-- Query 5: number_of_children is realistic AND decorrelated from city
-- ============================================================================
-- This is the headline data-quality check. In the old generator every
-- customer in a city had the IDENTICAL child count (period-5 aliased onto
-- period-50 city), so per-city COUNT(DISTINCT number_of_children) was 1.
-- Now children is a separate seeded draw, so every one of the 20 cities
-- shows all 5 child values: min_distinct_per_city = 5. The distribution is
-- right-skewed, so households with 0 children outnumber households with 4
-- (n0_minus_n4 > 0).

ASSERT ROW_COUNT = 1
ASSERT VALUE min_distinct_per_city = 5
ASSERT VALUE max_distinct_per_city = 5
ASSERT VALUE n0_minus_n4 > 0
SELECT
    MIN(distinct_children)  AS min_distinct_per_city,
    MAX(distinct_children)  AS max_distinct_per_city,
    SUM(CASE WHEN city IS NOT NULL THEN n0 ELSE 0 END) - SUM(CASE WHEN city IS NOT NULL THEN n4 ELSE 0 END) AS n0_minus_n4
FROM (
    SELECT
        city,
        COUNT(DISTINCT number_of_children)                  AS distinct_children,
        SUM(CASE WHEN number_of_children = 0 THEN 1 ELSE 0 END) AS n0,
        SUM(CASE WHEN number_of_children = 4 THEN 1 ELSE 0 END) AS n4
    FROM {{zone_name}}.retail.dim_customer
    GROUP BY city
) per_city;

-- ============================================================================
-- Query 6: geography is population-skewed (the per-city totals now differ)
-- ============================================================================
-- Customers are assigned to metros by a seeded Zipf rank, so the largest
-- metro (New York, rank 1) carries materially more customers than the
-- smallest (Detroit, rank 20). ny_minus_detroit > 0 proves the skew, which
-- is what makes per-city aggregates (counts, revenue) vary in Power BI
-- instead of collapsing to one identical number per city.

ASSERT ROW_COUNT = 1
ASSERT VALUE ny_minus_detroit > 0
ASSERT VALUE ny_customers > 250000
ASSERT VALUE total_customers = 5000000
SELECT
    SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END)                                  AS ny_customers,
    SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END)
        - SUM(CASE WHEN city = 'Detroit' THEN 1 ELSE 0 END)                             AS ny_minus_detroit,
    COUNT(*)                                                                            AS total_customers
FROM {{zone_name}}.retail.dim_customer;

-- ============================================================================
-- Query 7: fact_sales integrity (the headline ODBC scan)
-- ============================================================================
-- 200,000,000 rows. SUM(sale_id) = 200M*200,000,001/2 = 20,000,000,100,000,000.
-- return_flag is deterministic ((sale_id-1) % 20 = 0) so exactly N/20 =
-- 10,000,000 returns. 5 channels, 6 payment methods, 4 order statuses, 1
-- currency.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 200000000
ASSERT VALUE sum_sale_id = 20000000100000000
ASSERT VALUE n_returns = 10000000
ASSERT VALUE n_channels = 5
ASSERT VALUE n_payment_methods = 6
ASSERT VALUE n_order_statuses = 4
ASSERT VALUE n_currencies = 1
SELECT
    COUNT(*)                                            AS n_rows,
    SUM(sale_id)                                        AS sum_sale_id,
    SUM(CASE WHEN return_flag THEN 1 ELSE 0 END)        AS n_returns,
    COUNT(DISTINCT sales_channel)                       AS n_channels,
    COUNT(DISTINCT payment_method)                      AS n_payment_methods,
    COUNT(DISTINCT order_status)                        AS n_order_statuses,
    COUNT(DISTINCT currency_code)                       AS n_currencies
FROM {{zone_name}}.retail.fact_sales;

-- ============================================================================
-- Query 8: fact_sales measure ranges (skewed but bounded)
-- ============================================================================
-- Quantities are a Zipf draw over [1, 8]; unit_price is right-skewed and
-- clamped to [1, 1500]; discount_pct to [0, 0.6]; hour_of_day to [0, 23].
-- total_amount is strictly positive. The bounds hold for any seed.

ASSERT ROW_COUNT = 1
ASSERT VALUE min_qty >= 1
ASSERT VALUE max_qty <= 8
ASSERT VALUE n_qty_values = 8
ASSERT VALUE min_price >= 1
ASSERT VALUE max_price <= 1500
ASSERT VALUE min_discount >= 0
ASSERT VALUE max_discount <= 0.6
ASSERT VALUE min_hour >= 0
ASSERT VALUE max_hour <= 23
ASSERT VALUE min_total > 0
SELECT
    MIN(quantity)                   AS min_qty,
    MAX(quantity)                   AS max_qty,
    COUNT(DISTINCT quantity)        AS n_qty_values,
    MIN(unit_price_usd)             AS min_price,
    MAX(unit_price_usd)             AS max_price,
    MIN(discount_pct)               AS min_discount,
    MAX(discount_pct)               AS max_discount,
    MIN(hour_of_day)                AS min_hour,
    MAX(hour_of_day)                AS max_hour,
    MIN(total_amount_usd)           AS min_total
FROM {{zone_name}}.retail.fact_sales;

-- ============================================================================
-- Query 9: fact_sales by sales_channel (Power BI channel slicer)
-- ============================================================================
-- 5 channels cycle so each holds exactly 40,000,000 rows. Revenue per channel
-- is left to fluctuate with the seeded measures (not asserted), but the row
-- distribution is closed-form.

ASSERT ROW_COUNT = 5
ASSERT RESULT SET INCLUDES
    ('In-Store',     40000000),
    ('Marketplace',  40000000),
    ('Mobile App',   40000000),
    ('Online',       40000000),
    ('Phone',        40000000)
SELECT sales_channel, COUNT(*) AS n_sales
FROM {{zone_name}}.retail.fact_sales
GROUP BY sales_channel
ORDER BY sales_channel;

-- ============================================================================
-- Query 10: fact_sales recency skew (a growing business)
-- ============================================================================
-- order_date is recency-skewed (exponential days-ago), so the most recent
-- full year (2024) carries more volume than the oldest (2020). y2024 > y2020
-- proves the trend that a flat, uniform-over-time generator could not show.

ASSERT ROW_COUNT = 1
ASSERT VALUE y2024_minus_y2020 > 0
ASSERT VALUE min_year = 2020
ASSERT VALUE max_year = 2024
SELECT
    SUM(CASE WHEN year(order_date) = 2024 THEN 1 ELSE 0 END)
        - SUM(CASE WHEN year(order_date) = 2020 THEN 1 ELSE 0 END)  AS y2024_minus_y2020,
    MIN(year(order_date))                                           AS min_year,
    MAX(year(order_date))                                           AS max_year
FROM {{zone_name}}.retail.fact_sales;

-- ============================================================================
-- Query 11: fact_sales x dim_product JOIN (brand line counts)
-- ============================================================================
-- product_key = (i*13) % 1,000,000 + 1 hits every product exactly 200 times
-- (200M / 1M). Each of the 50 brands owns 20,000 products, so each brand has
-- exactly 20,000 * 200 = 4,000,000 fact lines. The join validates FK
-- integrity and the closed-form distribution at once.

ASSERT ROW_COUNT = 50
ASSERT VALUE n_lines = 4000000  WHERE brand = 'Acme'
ASSERT VALUE n_lines = 4000000  WHERE brand = 'Wonka'
ASSERT VALUE n_lines = 4000000  WHERE brand = 'Nakatomi'
SELECT p.brand, COUNT(*) AS n_lines
FROM {{zone_name}}.retail.fact_sales f
JOIN {{zone_name}}.retail.dim_product p
  ON f.product_key = p.product_key
GROUP BY p.brand
ORDER BY p.brand;

-- ============================================================================
-- Query 12: fact_inventory_snapshot integrity
-- ============================================================================
-- 100,000,000 rows. SUM(inventory_snapshot_id) = 100M*100,000,001/2 =
-- 5,000,000,050,000,000. 365 snapshot days, 5 stock statuses, 4 ABC classes,
-- 5 store regions. on_hand_units is right-skewed and clamped to [0, 5000].

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 100000000
ASSERT VALUE sum_id = 5000000050000000
ASSERT VALUE n_days = 365
ASSERT VALUE n_status = 5
ASSERT VALUE n_abc = 4
ASSERT VALUE n_regions = 5
ASSERT VALUE min_on_hand >= 0
ASSERT VALUE max_on_hand <= 5000
SELECT
    COUNT(*)                                    AS n_rows,
    SUM(inventory_snapshot_id)                  AS sum_id,
    COUNT(DISTINCT snapshot_date)               AS n_days,
    COUNT(DISTINCT stock_status)                AS n_status,
    COUNT(DISTINCT abc_classification)          AS n_abc,
    COUNT(DISTINCT store_region)                AS n_regions,
    MIN(on_hand_units)                          AS min_on_hand,
    MAX(on_hand_units)                          AS max_on_hand
FROM {{zone_name}}.retail.fact_inventory_snapshot;

-- ============================================================================
-- Query 13: fact_web_events integrity
-- ============================================================================
-- 200,000,000 rows. SUM(event_id) = 20,000,000,100,000,000. is_bounce is
-- deterministic ((event_id-1) % 10 = 0) so exactly N/10 = 20,000,000 bounces.
-- 10 event types, 4 device types, 6 browsers, 365 event days.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 200000000
ASSERT VALUE sum_id = 20000000100000000
ASSERT VALUE n_event_types = 10
ASSERT VALUE n_devices = 4
ASSERT VALUE n_browsers = 6
ASSERT VALUE n_days = 365
ASSERT VALUE n_bounces = 20000000
SELECT
    COUNT(*)                                            AS n_rows,
    SUM(event_id)                                       AS sum_id,
    COUNT(DISTINCT event_type)                          AS n_event_types,
    COUNT(DISTINCT device_type)                         AS n_devices,
    COUNT(DISTINCT browser)                             AS n_browsers,
    COUNT(DISTINCT event_date)                          AS n_days,
    SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END)          AS n_bounces
FROM {{zone_name}}.retail.fact_web_events;

-- ============================================================================
-- Query 14: fact_web_events funnel skew + conversion / search integrity
-- ============================================================================
-- event_type follows a funnel: page_view (most common) far exceeds
-- checkout_complete (rare), so pv_minus_checkout > 0. conversion_value is
-- non-zero ONLY on checkout_complete events and search_query is non-null ONLY
-- on search events, so the two leakage counts are exactly 0.

ASSERT ROW_COUNT = 1
ASSERT VALUE pv_minus_checkout > 0
ASSERT VALUE bad_conversions = 0
ASSERT VALUE bad_searches = 0
SELECT
    SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)
        - SUM(CASE WHEN event_type = 'checkout_complete' THEN 1 ELSE 0 END)             AS pv_minus_checkout,
    SUM(CASE WHEN conversion_value_usd > 0 AND event_type <> 'checkout_complete' THEN 1 ELSE 0 END) AS bad_conversions,
    SUM(CASE WHEN search_query IS NOT NULL AND event_type <> 'search' THEN 1 ELSE 0 END) AS bad_searches
FROM {{zone_name}}.retail.fact_web_events;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One row per table pinned to its closed-form (count, key sum). If any value
-- drifts the table name in that row points to the regressing path. dim_date
-- has no row-index key, so its key-sum slot is 0.

ASSERT ROW_COUNT = 7
ASSERT RESULT SET INCLUDES
    ('dim_date',                     7305,                      0),
    ('dim_store',                   25000,              312512500),
    ('dim_product',               1000000,           500000500000),
    ('dim_customer',              5000000,         12500002500000),
    ('fact_sales',              200000000,      20000000100000000),
    ('fact_inventory_snapshot', 100000000,       5000000050000000),
    ('fact_web_events',         200000000,      20000000100000000)
SELECT 'dim_date'                   AS tbl, COUNT(*) AS n, CAST(0 AS BIGINT)        AS s FROM {{zone_name}}.retail.dim_date
UNION ALL SELECT 'dim_store',                  COUNT(*), SUM(store_key)               FROM {{zone_name}}.retail.dim_store
UNION ALL SELECT 'dim_product',                COUNT(*), SUM(product_key)             FROM {{zone_name}}.retail.dim_product
UNION ALL SELECT 'dim_customer',               COUNT(*), SUM(customer_key)            FROM {{zone_name}}.retail.dim_customer
UNION ALL SELECT 'fact_sales',                 COUNT(*), SUM(sale_id)                FROM {{zone_name}}.retail.fact_sales
UNION ALL SELECT 'fact_inventory_snapshot',    COUNT(*), SUM(inventory_snapshot_id)  FROM {{zone_name}}.retail.fact_inventory_snapshot
UNION ALL SELECT 'fact_web_events',            COUNT(*), SUM(event_id)               FROM {{zone_name}}.retail.fact_web_events;
