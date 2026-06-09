-- ============================================================================
-- Demo: Frankfurter FX Rate Catalog, Queries
-- ============================================================================
-- Exercises the two API endpoints end to end. Registry inspection
-- (post-ALTER), DESCRIBE on the renamed leaf, both INVOKE calls,
-- per-endpoint run audits, bronze external table creation, schema
-- detection, and the bronze->silver promotion all live here. The bronze
-- external table is created here (not in setup.sql) after INVOKE,
-- because CREATE EXTERNAL TABLE validates and detects schema against the
-- landed files.
--
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used.
-- Live FX feeds change every ECB publication day.
-- ============================================================================

-- ============================================================================
-- API surface: inspect, invoke, audit
-- ============================================================================

-- Confirm both endpoints exist after the ALTER + RENAME in setup.sql.
SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.frankfurter_fx;

-- Inspect the renamed endpoint's post-ALTER URL and options.
DESCRIBE API ENDPOINT {{zone_name}}.frankfurter_fx.euro_launch_day_rates;

-- Issue both HTTPS GETs. Each writes one JSON page to its endpoint folder;
-- the bronze table picks them both up via recursive scan.
INVOKE API ENDPOINT {{zone_name}}.frankfurter_fx.latest_eur_basket;
INVOKE API ENDPOINT {{zone_name}}.frankfurter_fx.euro_launch_day_rates;

-- Per-endpoint run audit.
SHOW API ENDPOINT RUNS {{zone_name}}.frankfurter_fx.latest_eur_basket LIMIT 5;
SHOW API ENDPOINT RUNS {{zone_name}}.frankfurter_fx.euro_launch_day_rates LIMIT 5;

-- Bronze external table is created HERE, after INVOKE, because
-- CREATE EXTERNAL TABLE validates and detects schema against the landed
-- files. The INVOKE block above must run first so the JSON pages exist.
-- Frankfurter's response is {amount, base, date, rates: {...}}. Only
-- the top-level scalar fields are stored flat, the `rates` nested
-- object has dynamic currency-code keys and is left out of the
-- flatten (each row still knows its base_currency and rate_date).
-- LOCATION points at the connection's base_path so both endpoint
-- subfolders are picked up.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.frankfurter_fx.fx_rates_bronze
USING JSON
LOCATION 'frankfurter-fx-rate-catalog/frankfurter_fx'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.amount",
            "$.base",
            "$.date"
        ],
        "column_mappings": {
            "$.amount": "base_amount",
            "$.base":   "base_currency",
            "$.date":   "rate_date"
        },
        "max_depth": 2,
        "separator": "_",
        "infer_types": true
    }'
);

-- Resolve the bronze schema from the freshly written JSON pages.
DETECT SCHEMA FOR TABLE {{zone_name}}.frankfurter_fx.fx_rates_bronze;

-- ============================================================================
-- Query 1: Bronze feed landed
-- ============================================================================
-- ROW_COUNT > 0 confirms both INVOKEs wrote data the bronze table can read.

ASSERT ROW_COUNT > 0
SELECT
    base_currency,
    rate_date,
    base_amount
FROM {{zone_name}}.frankfurter_fx.fx_rates_bronze;

-- ============================================================================
-- Query 2: Bronze -> silver promotion
-- ============================================================================
-- Silver is the typed-column layer dashboards point at.

INSERT INTO {{zone_name}}.frankfurter_fx.fx_rates_silver
SELECT
    base_currency,
    CAST(rate_date AS DATE)     AS rate_date,
    CAST(base_amount AS DOUBLE) AS base_amount
FROM {{zone_name}}.frankfurter_fx.fx_rates_bronze;

-- ============================================================================
-- Query 3: Silver curated rates
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    base_currency,
    rate_date,
    base_amount
FROM {{zone_name}}.frankfurter_fx.fx_rates_silver
ORDER BY rate_date;

-- ============================================================================
-- Query 4: Base-currency split
-- ============================================================================
-- latest_eur_basket uses EUR; euro_launch_day_rates was URL-ALTERed to USD.

SELECT
    base_currency,
    rate_date,
    base_amount
FROM {{zone_name}}.frankfurter_fx.fx_rates_bronze
ORDER BY base_currency;

-- ============================================================================
-- Query 5: Silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.frankfurter_fx.fx_rates_silver;
