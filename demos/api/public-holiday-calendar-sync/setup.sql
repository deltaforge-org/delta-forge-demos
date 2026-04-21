-- ============================================================================
-- Demo: Public Holiday Calendar Sync — Script-Parameter Incremental Enrichment
-- Feature: Target-table-driven INVOKE USING via SET / INTO / $variables
-- ============================================================================
--
-- Real-world story: a multinational HR platform syncs public-holiday
-- calendars per (country, year) so its time-off tracker and billable-day
-- calculator stay accurate. At launch, a Nordic pair was hand-seeded
-- (Norway 2024 and Sweden 2024). Every December the ops team runs the
-- next wave: each onboarded country gets its next-year calendar fetched
-- from Nager.Date and merged into the silver catalog.
--
-- This demo runs one such wave — the next missing Norway year — using
-- the script-parameter pattern end-to-end:
--
--   1. Read silver: `SELECT 'NO', MAX(year)+1 FROM silver ...
--                     INTO $next_country, $next_year;`
--      Captures the next wave's gap into session-scoped script params.
--
--   2. INVOKE with runtime overrides: `USING (path_param.year = $next_year,
--                                             path_param.country_code = $next_country);`
--      The engine resolves $next_year / $next_country from the script
--      bag against the actual ScalarValues, merges them into the endpoint's
--      path_param map, and issues the HTTPS GET against:
--         https://date.nager.at/api/v3/PublicHolidays/2025/NO
--
--   3. Merge bronze → silver, stamping each new row with $next_year.
--      NOT EXISTS on (country_code, holiday_year, holiday_date) keeps
--      the merge idempotent on replay.
--
-- IMPORTANT: this demo's statements share script-scoped parameters. The
-- demo harness MUST execute this file as a SINGLE multi-statement script
-- (one `execute_script_stream` call), not statement-by-statement. The
-- script param bag is cleared between script invocations, so splitting
-- the INTO / INVOKE / INSERT across separate HTTP calls would wipe
-- $next_year / $next_country between step 6 and step 7.
--
-- NOTE: requires internet. INVOKE issues a real GET against
-- https://date.nager.at/api/v3/PublicHolidays/<year>/<country>.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Credential (OS keychain — the always-on default vault)
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

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.hr_calendar
    COMMENT 'HR platform public-holiday calendars, wave-loaded per country/year';

-- --------------------------------------------------------------------------
-- 3. Silver catalog — seeded with the launch Nordic pair (2024)
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.hr_calendar.country_holidays (
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

INSERT INTO {{zone_name}}.hr_calendar.country_holidays VALUES
    ('NO', 2024, DATE '2024-01-01', 'Forste nyttarsdag',    'New Year''s Day',   true, true, 'launch_seed'),
    ('NO', 2024, DATE '2024-05-01', 'Arbeidernes dag',      'Labour Day',        true, true, 'launch_seed'),
    ('NO', 2024, DATE '2024-05-17', 'Grunnlovsdag',         'Constitution Day',  true, true, 'launch_seed'),
    ('NO', 2024, DATE '2024-12-25', 'Forste juledag',       'Christmas Day',     true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-01-01', 'Nyarsdagen',           'New Year''s Day',   true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-05-01', 'Forsta maj',           'Labour Day',        true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-06-06', 'Sveriges nationaldag', 'National Day',      true, true, 'launch_seed'),
    ('SE', 2024, DATE '2024-12-25', 'Juldagen',             'Christmas Day',     true, true, 'launch_seed');

GRANT ADMIN ON TABLE {{zone_name}}.hr_calendar.country_holidays TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 4. REST API connection
-- --------------------------------------------------------------------------

CREATE CONNECTION IF NOT EXISTS nager_date_holidays
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://date.nager.at',
        auth_mode    = 'bearer',
        storage_zone = '{{zone_name}}',
        base_path    = 'nager_date_holidays',
        timeout_secs = '30'
    )
    CREDENTIAL = holiday_api_token;

-- --------------------------------------------------------------------------
-- 5. API endpoint — URL template only, NO stored path_params
-- --------------------------------------------------------------------------

CREATE API ENDPOINT {{zone_name}}.nager_date_holidays.public_holidays
    URL '/api/v3/PublicHolidays/{year}/{country_code}'
    RESPONSE FORMAT JSON;

-- --------------------------------------------------------------------------
-- 6. Capture next-wave params from target silver
-- --------------------------------------------------------------------------
-- Single aggregate SELECT binds two session-scoped script parameters
-- ($next_country, $next_year). Because MAX() is aggregate, the query
-- always returns exactly one row — safe for the INTO grammar's
-- single-row contract. COALESCE bootstraps at 2024 when silver has
-- no Norway rows yet (first wave fetches 2025).

SELECT
    'NO'                                                  AS next_country,
    COALESCE(MAX(holiday_year), 2024) + 1                 AS next_year
FROM {{zone_name}}.hr_calendar.country_holidays
WHERE country_code = 'NO'
INTO $next_country, $next_year;

-- --------------------------------------------------------------------------
-- 7. INVOKE with runtime-resolved path params via USING (...)
-- --------------------------------------------------------------------------
-- The engine resolves $next_year / $next_country against the script
-- param bag populated in step 6, merges the ScalarValues into the
-- endpoint's path_param map, and assembles the URL:
--     https://date.nager.at/api/v3/PublicHolidays/2025/NO
-- The SAME endpoint row is reusable across every wave — only the USING
-- clause's resolved values change between calls.

INVOKE API ENDPOINT {{zone_name}}.nager_date_holidays.public_holidays
    USING (
        path_param.year         = $next_year,
        path_param.country_code = $next_country
    );

-- --------------------------------------------------------------------------
-- 8. Bronze external table over the landed JSON
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hr_calendar.public_holidays_bronze
USING JSON
LOCATION 'nager_date_holidays/public_holidays'
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

DETECT SCHEMA FOR TABLE {{zone_name}}.hr_calendar.public_holidays_bronze;
GRANT ADMIN ON TABLE {{zone_name}}.hr_calendar.public_holidays_bronze TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 9. Anti-join merge — bronze → silver on composite key
-- --------------------------------------------------------------------------
-- $next_year is stamped onto every merged row so silver records the
-- year we asked for, not a value re-derived from the payload. NOT EXISTS
-- on (country_code, holiday_year, holiday_date) keeps replays idempotent.

INSERT INTO {{zone_name}}.hr_calendar.country_holidays
SELECT
    b.country_code,
    $next_year                     AS holiday_year,
    CAST(b.holiday_date AS DATE)   AS holiday_date,
    b.local_name,
    b.english_name,
    CAST(b.is_fixed  AS BOOLEAN)   AS is_fixed,
    CAST(b.is_global AS BOOLEAN)   AS is_global,
    'nager_api'                    AS source_batch
FROM {{zone_name}}.hr_calendar.public_holidays_bronze b
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.hr_calendar.country_holidays s
    WHERE s.country_code = b.country_code
      AND s.holiday_year = $next_year
      AND s.holiday_date = CAST(b.holiday_date AS DATE)
);
