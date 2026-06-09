-- ============================================================================
-- Delta Time Travel — Point-in-Time Joins — Setup Script
-- ============================================================================
-- A multinational trading desk executes 12 currency trades across 3 time
-- periods (morning, midday, afternoon). Exchange rates are updated at each
-- period, creating new Delta versions. Regulatory reporting requires each
-- trade to be valued at the rate that was active when it traded.
--
-- Version History (fx_rates):
--   V0: CREATE TABLE (empty)
--   V1: INSERT morning rates  — EUR/USD=1.0850, GBP/USD=1.2650, JPY/USD=0.00667
--   V2: UPDATE midday rates   — EUR/USD=1.0875, GBP/USD=1.2680, JPY/USD=0.00670
--   V3: UPDATE afternoon rates— EUR/USD=1.0820, GBP/USD=1.2700, JPY/USD=0.00665
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TRADES TABLE — 12 trades across 3 currency pairs and 3 time periods
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.fx_trades (
    trade_id   INT,
    pair       VARCHAR,
    amount     DOUBLE,
    direction  VARCHAR,
    traded_at  VARCHAR
) LOCATION 'delta-time-travel-point-in-time-join/fx_trades';


INSERT INTO {{zone_name}}.delta_demos.fx_trades VALUES
    -- Morning trades (09:xx) — should be valued at V1 rates
    (1,  'EUR/USD', 50000,   'BUY',  '2025-01-15 09:15:00'),
    (2,  'GBP/USD', 25000,   'SELL', '2025-01-15 09:30:00'),
    (3,  'JPY/USD', 1000000, 'BUY',  '2025-01-15 09:45:00'),
    (4,  'EUR/USD', 75000,   'SELL', '2025-01-15 09:55:00'),
    -- Midday trades (12:xx) — should be valued at V2 rates
    (5,  'GBP/USD', 100000,  'BUY',  '2025-01-15 12:10:00'),
    (6,  'JPY/USD', 5000000, 'SELL', '2025-01-15 12:30:00'),
    (7,  'EUR/USD', 30000,   'BUY',  '2025-01-15 12:45:00'),
    (8,  'GBP/USD', 40000,   'SELL', '2025-01-15 12:55:00'),
    -- Afternoon trades (15:xx) — should be valued at V3 rates
    (9,  'JPY/USD', 2000000, 'BUY',  '2025-01-15 15:10:00'),
    (10, 'EUR/USD', 60000,   'SELL', '2025-01-15 15:25:00'),
    (11, 'GBP/USD', 80000,   'BUY',  '2025-01-15 15:40:00'),
    (12, 'JPY/USD', 3000000, 'SELL', '2025-01-15 15:50:00');


-- ============================================================================
-- EXCHANGE RATES TABLE — Updated 3 times throughout the trading day
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.fx_rates (
    pair           VARCHAR,
    rate           DOUBLE,
    effective_from VARCHAR
) LOCATION 'delta-time-travel-point-in-time-join/fx_rates';


-- VERSION 1: Morning rates
INSERT INTO {{zone_name}}.delta_demos.fx_rates VALUES
    ('EUR/USD', 1.0850,  '2025-01-15 08:00:00'),
    ('GBP/USD', 1.2650,  '2025-01-15 08:00:00'),
    ('JPY/USD', 0.00667, '2025-01-15 08:00:00');

-- VERSION 2: Midday rate update
UPDATE {{zone_name}}.delta_demos.fx_rates
SET rate = CASE pair
    WHEN 'EUR/USD' THEN 1.0875
    WHEN 'GBP/USD' THEN 1.2680
    WHEN 'JPY/USD' THEN 0.00670
  END,
  effective_from = '2025-01-15 12:00:00';

-- VERSION 3: Afternoon rate update
UPDATE {{zone_name}}.delta_demos.fx_rates
SET rate = CASE pair
    WHEN 'EUR/USD' THEN 1.0820
    WHEN 'GBP/USD' THEN 1.2700
    WHEN 'JPY/USD' THEN 0.00665
  END,
  effective_from = '2025-01-15 15:00:00';
