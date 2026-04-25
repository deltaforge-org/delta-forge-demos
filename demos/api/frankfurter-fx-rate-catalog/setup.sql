-- ============================================================================
-- Demo: Frankfurter FX Rate Catalog, ALTER API ENDPOINT + Multi-Endpoint
-- Feature: ALTER API ENDPOINT (SET URL, SET OPTIONS, RENAME TO),
--          multiple endpoints under one connection
-- ============================================================================
--
-- Real-world story: a fintech risk desk maintains a small FX-rate
-- catalog for two downstream consumers:
--
--   - The daily settlement engine needs the latest rate from EUR to a
--     basket of settlement currencies (USD, GBP, NOK, SEK, DKK, CHF).
--   - The cross-currency benchmark report uses 1999-01-04 (the day the
--     euro was introduced) as a historical anchor.
--
-- Both live under ONE connection. First the endpoints are defined;
-- then an ALTER walks the team through evolving an endpoint in
-- production without recreating it, changing the URL to point at a
-- different date, relaxing the rate limit, and renaming the leaf to
-- reflect its new purpose.
--
-- This file declares the catalog objects only. The two INVOKE calls,
-- the per-endpoint SHOW API ENDPOINT RUNS audit reads, the schema
-- detection, and the bronze->silver promotion all live in queries.sql
-- so the user can see how the multi-endpoint connection is driven from
-- SQL.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.frankfurter_fx
    COMMENT 'FX rate catalog, latest + historical snapshots';

-- --------------------------------------------------------------------------
-- 2. Connection, Frankfurter public ECB-backed FX service
-- --------------------------------------------------------------------------

CREATE CONNECTION IF NOT EXISTS frankfurter_fx
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://api.frankfurter.dev',
        auth_mode    = 'none',
        storage_zone = '{{zone_name}}',
        base_path    = 'frankfurter_fx',
        timeout_secs = '30'
    );

-- --------------------------------------------------------------------------
-- 3. Endpoint 1, latest rates for the euro basket
-- --------------------------------------------------------------------------
-- Direct, final-form endpoint. No ALTER needed on this one; the daily
-- settlement engine runs it every morning against the same URL.

CREATE API ENDPOINT {{zone_name}}.frankfurter_fx.latest_eur_basket
    URL '/v1/latest?from=EUR&to=USD,GBP,NOK,SEK,DKK,CHF'
    RESPONSE FORMAT JSON
    OPTIONS (
        rate_limit_rps = '2'
    );

-- --------------------------------------------------------------------------
-- 4. Endpoint 2, created as a quick USD lookup, ALTERed below
-- --------------------------------------------------------------------------
-- Initial definition is a "temporary" USD-to-basket latest query. The
-- team is about to repurpose this endpoint for the historical anchor
-- rate instead. Rather than DROP + CREATE, they'll ALTER it in place
-- below, preserving any grants, run history, and downstream
-- references.

CREATE API ENDPOINT {{zone_name}}.frankfurter_fx.historical_stub
    URL '/v1/latest?from=USD&to=EUR,GBP,JPY'
    RESPONSE FORMAT JSON
    OPTIONS (
        rate_limit_rps = '2'
    );

-- --------------------------------------------------------------------------
-- 5. ALTER, evolve historical_stub into euro_launch_day_rates
-- --------------------------------------------------------------------------
-- Three actions in one statement:
--   - SET URL            ,  repoint at 1999-01-04 (euro launch day)
--   - SET OPTIONS        ,  tighten rate_limit_rps, bump retry_max_attempts
--                          (historical endpoints are less critical and
--                           can tolerate longer tail latency)
--   - RENAME TO          ,  new leaf name matches new purpose
--
-- Action list is order-preserving; later actions override earlier ones
-- on field overlap (two SET URLs: last wins). Here every action
-- touches a different field so the order doesn't matter semantically.

ALTER API ENDPOINT {{zone_name}}.frankfurter_fx.historical_stub
    SET URL '/v1/1999-01-04?from=USD&to=EUR,GBP,JPY,CHF'
    SET OPTIONS (rate_limit_rps = '1', retry_max_attempts = '4')
    RENAME TO euro_launch_day_rates;

-- --------------------------------------------------------------------------
-- 6. Bronze external table, top-level FX response shape
-- --------------------------------------------------------------------------
-- Frankfurter's response is {amount, base, date, rates: {...}}. Only
-- the top-level scalar fields are stored flat, the `rates` nested
-- object has dynamic currency-code keys and is left out of the
-- flatten (each row still knows its base_currency and rate_date).
--
-- LOCATION points at the connection's base_path so both endpoint
-- subfolders are picked up.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.frankfurter_fx.fx_rates_bronze
USING JSON
LOCATION 'frankfurter_fx'
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

-- --------------------------------------------------------------------------
-- 7. Silver Delta table, schema-only declaration
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.frankfurter_fx.fx_rates_silver (
    base_currency  STRING,
    rate_date      DATE,
    base_amount    DOUBLE
)
LOCATION 'silver/fx_rates';
