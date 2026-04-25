-- ============================================================================
-- Demo: Public Holiday Calendar Sync, Setup
-- Feature: Catalog objects for the script-parameter incremental ingest
-- ============================================================================
--
-- Real-world story: a multinational HR platform syncs public-holiday
-- calendars per (country, year) so its time-off tracker and billable-day
-- calculator stay accurate. At launch, a Nordic pair was hand-seeded
-- (Norway 2024 and Sweden 2024). Every December the ops team runs the
-- next wave: each onboarded country gets its next-year calendar fetched
-- from Nager.Date and merged into the silver catalog.
--
-- This file declares the catalog objects only:
--   - Credential, zone, schema
--   - Silver Delta target with the launch seed
--   - REST connection
--   - API endpoint with `{year}` / `{country_code}` placeholders
--   - Bronze external table over the eventual landing path
--
-- The actual wave (next-year capture via SELECT ... INTO,
-- INVOKE ... USING, schema detection, anti-join merge) lives in
-- queries.sql. It must run as a single multi-statement script because
-- the script-scoped `$next_country` / `$next_year` parameters are
-- shared across the SELECT INTO, the INVOKE USING, and the merge
-- INSERT, the param bag is cleared between script invocations.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Credential (OS keychain, the always-on default vault)
-- --------------------------------------------------------------------------

CREATE CREDENTIAL IF NOT EXISTS holiday_api_token
    TYPE = CREDENTIAL
    SECRET 'demo-placeholder-nager-is-public'
    DESCRIPTION 'Bearer placeholder for the HR-platform holiday calendar sync';

-- --------------------------------------------------------------------------
-- 2. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.holiday_calendar
    COMMENT 'HR platform public-holiday calendars, wave-loaded per country/year';

-- --------------------------------------------------------------------------
-- 3. Silver catalog, seeded with the launch Nordic pair (2024)
-- --------------------------------------------------------------------------
-- The launch seed is part of the catalog baseline (declarative startup
-- state). Every wave after launch is driven from queries.sql.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.holiday_calendar.country_holidays (
    country_code   STRING,
    holiday_year   INT,
    holiday_date   DATE,
    local_name     STRING,
    english_name   STRING,
    is_fixed       BOOLEAN,
    is_global      BOOLEAN,
    source_batch   STRING
)
LOCATION 'silver/country_holidays';

INSERT INTO {{zone_name}}.holiday_calendar.country_holidays VALUES
    ('NO', 2024, DATE '2024-01-01', 'Forste nyttarsdag',    'New Year''s Day',   true, true, 'launch_seed'),
    ('NO', 2024, DATE '2024-05-01', 'Arbeidernes dag',      'Labour Day',        true, true, 'launch_seed'),
    ('NO', 2024, DATE '2024-05-17', 'Grunnlovsdag',         'Constitution Day',  true, true, 'launch_seed'),
    ('NO', 2024, DATE '2024-12-25', 'Forste juledag',       'Christmas Day',     true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-01-01', 'Nyarsdagen',           'New Year''s Day',   true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-05-01', 'Forsta maj',           'Labour Day',        true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-06-06', 'Sveriges nationaldag', 'National Day',      true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-12-25', 'Juldagen',             'Christmas Day',     true, true, 'launch_seed');

-- --------------------------------------------------------------------------
-- 4. REST API connection
-- --------------------------------------------------------------------------

CREATE CONNECTION IF NOT EXISTS holiday_calendar
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://date.nager.at',
        auth_mode    = 'bearer',
        storage_zone = '{{zone_name}}',
        base_path    = 'holiday_calendar',
        timeout_secs = '30'
    )
    CREDENTIAL = holiday_api_token;

-- --------------------------------------------------------------------------
-- 5. API endpoint, URL template only, NO stored path_params
-- --------------------------------------------------------------------------
-- `{year}` and `{country_code}` are placeholders the engine resolves
-- at INVOKE time from the USING clause in queries.sql. The endpoint
-- row never carries the per-wave values, the same row is reusable
-- across every wave.

CREATE API ENDPOINT {{zone_name}}.holiday_calendar.public_holidays
    URL '/api/v3/PublicHolidays/{year}/{country_code}'
    RESPONSE FORMAT JSON;

-- --------------------------------------------------------------------------
-- 6. Bronze external table over the landed JSON
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.holiday_calendar.public_holidays_bronze
USING JSON
LOCATION 'holiday_calendar/public_holidays'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.date",
            "$.localName",
            "$.name",
            "$.countryCode",
            "$.fixed",
            "$.global"
        ],
        "column_mappings": {
            "$.date":        "holiday_date",
            "$.localName":   "local_name",
            "$.name":        "english_name",
            "$.countryCode": "country_code",
            "$.fixed":       "is_fixed",
            "$.global":      "is_global"
        },
        "max_depth": 2,
        "separator": "_",
        "infer_types": true
    }'
);
