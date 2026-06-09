-- ============================================================================
-- Delta Time Travel — Point-in-Time Joins — Educational Queries
-- ============================================================================
-- WHAT: JOIN a fact table (trades) against a dimension table (exchange rates)
--       pinned to a specific historical version using VERSION AS OF.
-- WHY:  Exchange rates change throughout the day. A standard JOIN uses only
--       the latest rate, producing incorrect trade valuations. Regulators
--       require each trade to be valued at the rate active when it executed.
-- HOW:  Use JOIN table VERSION AS OF N to pin the dimension table to the
--       version that was live at the time of each trade batch.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Current Exchange Rates (Latest Version)
-- ============================================================================
-- These are the afternoon rates — the final update of the trading day.
-- If we value all trades at these rates, morning and midday trades get
-- the wrong valuation.

ASSERT VALUE rate = 1.082 WHERE pair = 'EUR/USD'
ASSERT VALUE rate = 1.27 WHERE pair = 'GBP/USD'
ASSERT VALUE rate = 0.00665 WHERE pair = 'JPY/USD'
ASSERT ROW_COUNT = 3
SELECT pair, rate, effective_from
FROM {{zone_name}}.delta_demos.fx_rates
ORDER BY pair;


-- ============================================================================
-- LEARN: The Wrong Way — All Trades at Current Rates
-- ============================================================================
-- This is what happens with a naive JOIN: every trade gets today's closing
-- rate regardless of when it was executed. The total is $616,930.00 — but
-- the correct total (using point-in-time rates) is $617,335.00. A $405
-- discrepancy that regulators will flag.

-- Non-deterministic: ROUND(SUM(DOUBLE*DOUBLE), 2) may vary ±0.01 due to floating-point accumulation
ASSERT WARNING VALUE wrong_total BETWEEN 616929.00 AND 616931.00
ASSERT ROW_COUNT = 1
SELECT ROUND(SUM(t.amount * r.rate), 2) AS wrong_total
FROM {{zone_name}}.delta_demos.fx_trades t
JOIN {{zone_name}}.delta_demos.fx_rates r ON t.pair = r.pair;


-- ============================================================================
-- LEARN: Morning Trades at Morning Rates (VERSION AS OF 1)
-- ============================================================================
-- The first 4 trades executed between 09:00 and 10:00. At that time,
-- the rate table was at version 1 (morning rates). By pinning the JOIN
-- to VERSION AS OF 1, we get the correct valuations.

ASSERT VALUE usd_value = 54250.0 WHERE trade_id = 1
ASSERT ROW_COUNT = 4
SELECT t.trade_id, t.pair, t.amount, t.direction,
       r.rate AS morning_rate,
       ROUND(t.amount * r.rate, 2) AS usd_value
FROM {{zone_name}}.delta_demos.fx_trades t
JOIN {{zone_name}}.delta_demos.fx_rates VERSION AS OF 1 r
  ON t.pair = r.pair
WHERE t.traded_at LIKE '2025-01-15 09%'
ORDER BY t.trade_id;


-- ============================================================================
-- LEARN: Midday Trades at Midday Rates (VERSION AS OF 2)
-- ============================================================================
-- Trades 5-8 executed between 12:00 and 13:00. The rate table was at V2.

ASSERT VALUE usd_value = 126800.0 WHERE trade_id = 5
ASSERT ROW_COUNT = 4
SELECT t.trade_id, t.pair, t.amount, t.direction,
       r.rate AS midday_rate,
       ROUND(t.amount * r.rate, 2) AS usd_value
FROM {{zone_name}}.delta_demos.fx_trades t
JOIN {{zone_name}}.delta_demos.fx_rates VERSION AS OF 2 r
  ON t.pair = r.pair
WHERE t.traded_at LIKE '2025-01-15 12%'
ORDER BY t.trade_id;


-- ============================================================================
-- LEARN: Afternoon Trades at Afternoon Rates (VERSION AS OF 3 / Current)
-- ============================================================================
-- Trades 9-12 executed between 15:00 and 16:00. The rate table is at V3,
-- which is also the current version — so a standard JOIN works here too.

ASSERT VALUE usd_value = 13300.0 WHERE trade_id = 9
ASSERT ROW_COUNT = 4
SELECT t.trade_id, t.pair, t.amount, t.direction,
       r.rate AS afternoon_rate,
       ROUND(t.amount * r.rate, 2) AS usd_value
FROM {{zone_name}}.delta_demos.fx_trades t
JOIN {{zone_name}}.delta_demos.fx_rates VERSION AS OF 3 r
  ON t.pair = r.pair
WHERE t.traded_at LIKE '2025-01-15 15%'
ORDER BY t.trade_id;


-- ============================================================================
-- EXPLORE: Correct vs Incorrect Valuation
-- ============================================================================
-- Side-by-side comparison: the naive approach (all at current rates) vs the
-- correct approach (point-in-time rates per period). The $405 difference
-- demonstrates why temporal JOINs matter for regulatory compliance.

-- Non-deterministic: ROUND(SUM(DOUBLE*DOUBLE), 2) may vary ±0.01 due to floating-point accumulation
ASSERT WARNING VALUE incorrect_total BETWEEN 616929.00 AND 616931.00
ASSERT WARNING VALUE correct_total BETWEEN 617334.00 AND 617336.00
ASSERT ROW_COUNT = 1
SELECT ROUND(wrong.total, 2)  AS incorrect_total,
       ROUND(correct.total, 2) AS correct_total,
       ROUND(wrong.total - correct.total, 2) AS valuation_error
FROM (SELECT SUM(t.amount * r.rate) AS total
      FROM {{zone_name}}.delta_demos.fx_trades t
      JOIN {{zone_name}}.delta_demos.fx_rates r ON t.pair = r.pair) wrong,
     (SELECT
        (SELECT SUM(t.amount * r.rate) FROM {{zone_name}}.delta_demos.fx_trades t
         JOIN {{zone_name}}.delta_demos.fx_rates VERSION AS OF 1 r ON t.pair = r.pair
         WHERE t.traded_at LIKE '2025-01-15 09%')
      + (SELECT SUM(t.amount * r.rate) FROM {{zone_name}}.delta_demos.fx_trades t
         JOIN {{zone_name}}.delta_demos.fx_rates VERSION AS OF 2 r ON t.pair = r.pair
         WHERE t.traded_at LIKE '2025-01-15 12%')
      + (SELECT SUM(t.amount * r.rate) FROM {{zone_name}}.delta_demos.fx_trades t
         JOIN {{zone_name}}.delta_demos.fx_rates VERSION AS OF 3 r ON t.pair = r.pair
         WHERE t.traded_at LIKE '2025-01-15 15%')
      AS total) correct;


-- ============================================================================
-- EXPLORE: Rate Movement Across the Day
-- ============================================================================
-- Track how each currency pair moved from morning (V1) to afternoon (V3).
-- EUR/USD weakened (-0.003), GBP/USD strengthened (+0.005), JPY/USD
-- slightly weakened (-0.00002).

ASSERT VALUE morning_rate = 1.085 WHERE pair = 'EUR/USD'
ASSERT VALUE afternoon_rate = 1.082 WHERE pair = 'EUR/USD'
ASSERT ROW_COUNT = 3
SELECT v1.pair,
       v1.rate AS morning_rate,
       v2.rate AS midday_rate,
       v3.rate AS afternoon_rate,
       ROUND(v3.rate - v1.rate, 5) AS day_change
FROM {{zone_name}}.delta_demos.fx_rates VERSION AS OF 1 v1
JOIN {{zone_name}}.delta_demos.fx_rates VERSION AS OF 2 v2 ON v1.pair = v2.pair
JOIN {{zone_name}}.delta_demos.fx_rates VERSION AS OF 3 v3 ON v1.pair = v3.pair
ORDER BY v1.pair;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify 12 trades exist
ASSERT ROW_COUNT = 12
SELECT * FROM {{zone_name}}.delta_demos.fx_trades;

-- Verify 3 exchange rate pairs
ASSERT VALUE pair_count = 3
SELECT COUNT(*) AS pair_count FROM {{zone_name}}.delta_demos.fx_rates;

-- Verify morning rate for EUR/USD was 1.085
ASSERT VALUE rate = 1.085
SELECT rate FROM {{zone_name}}.delta_demos.fx_rates VERSION AS OF 1 WHERE pair = 'EUR/USD';

-- Verify current EUR/USD rate is 1.082
ASSERT VALUE rate = 1.082
SELECT rate FROM {{zone_name}}.delta_demos.fx_rates WHERE pair = 'EUR/USD';

-- Verify trade 1 amount
ASSERT VALUE amount = 50000.0
SELECT amount FROM {{zone_name}}.delta_demos.fx_trades WHERE trade_id = 1;
