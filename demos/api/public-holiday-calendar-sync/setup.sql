-- ============================================================================
-- Demo: Public Holiday Calendar Sync — Path-Parameter Incremental Enrichment
-- Feature: Path-parameter-driven REST API ingest + target-table gap lookup
-- ============================================================================
--
-- Real-world story: a multinational HR platform syncs public-holiday
-- calendars per (country, year) so its time-off tracker and billable-day
-- calculator stay accurate. At launch, a Nordic pair was hand-seeded
-- (Norway 2024 and Sweden 2024 — the core holidays payroll cares about).
-- Every December the ops team runs the next wave: each onboarded country
-- gets its next-year calendar fetched from Nager.Date and merged into
-- the silver catalog. This demo runs one such wave — Norway 2025 — and
-- walks the path-parameter + target-table-gap pattern end to end.
--
-- The incremental pattern has three phases, shown directly in SQL:
--   1. INSPECT the target silver table for the (country, year) combos
--      already loaded. queries.sql Query 3 is the exact anti-join a
--      production pipeline runs to compute "wanted minus loaded".
--   2. CONFIGURE the ingest's path_param.year + path_param.country_code
--      to the next missing combo. A real pipeline does this with:
--        ALTER API INGEST ... SET OPTIONS (path_param.year = '2026', ...);
--      here we encode it on CREATE so the demo is self-contained and
--      idempotent across replays.
--   3. INVOKE the Nager.Date REST endpoint. path_param.* substitutions
--      slot into the URL template:
--        /api/v3/PublicHolidays/{year}/{country_code}
--      producing /api/v3/PublicHolidays/2025/NO on the wire.
--
-- Delta Forge mechanics exercised:
--   • Bearer credential in the OS keychain (CREATE CREDENTIAL)
--   • REST API data source (CREATE CONNECTION TYPE = rest_api)
--   • PATH-parameter binding via OPTIONS (path_param.<name> = ...) on
--     CREATE API INGEST — the headline "parameter" feature, substituted
--     into the endpoint template at INVOKE time
--   • json_flatten_config on a top-level JSON array response (Nager.Date
--     returns a bare array, not a wrapped object)
--   • Anti-join NOT EXISTS INSERT pattern for idempotent incremental
--     merge on a composite key (country_code, holiday_year, holiday_date)
--
-- Public API: Nager.Date (https://date.nager.at) — a no-auth public
-- holiday service used by scheduling tools and HR platforms worldwide.
-- Picked precisely BECAUSE it is distinct from the reference-catalog
-- demo's REST Countries source: different provider, different parameter
-- style (path vs query), different JSON response shape.
--
-- NOTE: requires internet. INVOKE issues a real GET against
-- https://date.nager.at/api/v3/PublicHolidays/2025/NO.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Credential (OS keychain — the always-on default vault)
-- --------------------------------------------------------------------------
-- Nager.Date's public endpoints don't require auth, so the literal
-- SECRET below is a placeholder. The wiring matches a real bearer
-- flow verbatim; only the literal secret value changes when you
-- repoint this demo at a gated HR API (Workday, BambooHR, etc.).

CREATE CREDENTIAL IF NOT EXISTS holiday_api_token
    TYPE = CREDENTIAL
    SECRET 'demo-placeholder-nager-is-public'
    DESCRIPTION 'Bearer placeholder for the HR-platform holiday calendar sync';

-- --------------------------------------------------------------------------
-- 2. Zone + schema
-- --------------------------------------------------------------------------
-- `hr_calendar` is a distinct schema from the travel-catalog demos, so
-- multiple API demos can coexist in the same `bronze` zone without
-- table-name collisions.

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.hr_calendar
    COMMENT 'HR platform public-holiday calendars, wave-loaded per country/year';

-- --------------------------------------------------------------------------
-- 3. Silver catalog — seeded with the launch Nordic pair (2024)
-- --------------------------------------------------------------------------
-- The silver table is the SOURCE OF INCREMENTAL TRUTH. Every subsequent
-- wave queries THIS table to decide which (country, year) combo it
-- still needs to fetch from Nager.Date. The launch seed is hand-
-- picked: four highest-signal public holidays for Norway 2024 and
-- Sweden 2024 — the ones payroll, time-off, and capacity planning all
-- key off. In production the seed is typically the most recent full
-- year for every onboarded country; here we keep it compact so the
-- wave deltas stay easy to eyeball.
--
-- `source_batch` labels each row with the wave that loaded it, giving
-- every row a provenance tag for the assertions in queries.sql.

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
-- 4. REST API connection — Nager.Date public endpoint
-- --------------------------------------------------------------------------
-- `base_path = 'nager_date_holidays'` roots this demo's landing under
-- the bronze zone, independent from other API demos living in the same
-- zone. timeout_secs = 30 is generous for a < 5 kB response but keeps
-- the demo resilient against transient latency spikes.

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
-- 5. API ingest — /api/v3/PublicHolidays/{year}/{country_code}
-- --------------------------------------------------------------------------
-- This is the headline path-parameter demonstration. The endpoint
-- template has TWO placeholders — {year} and {country_code} — and
-- path_param.<name> OPTIONS fill them both at INVOKE time. The engine
-- substitutes them into the URL template before the request is
-- signed and dispatched, producing:
--
--     https://date.nager.at/api/v3/PublicHolidays/2025/NO
--
-- path_param.year + path_param.country_code below encode the FIRST
-- wave to run: Norway 2025. That specific (country, year) combo was
-- chosen because it is precisely the one NOT present in the Nordic
-- silver seed — the anti-join in queries.sql Query 3 computes the
-- exact same gap.
--
-- In a production HR pipeline, the ratchet is automated:
--
--     ALTER API INGEST bronze.nager_date_holidays.public_holidays
--         SET OPTIONS (
--             path_param.year         = '<next-year-from-anti-join>',
--             path_param.country_code = '<next-country-from-anti-join>'
--         );
--     INVOKE API INGEST bronze.nager_date_holidays.public_holidays;
--
-- The scheduler computes the next wave from the target table, ALTERs
-- the path params, and INVOKEs. We encode the wave at CREATE time so
-- the demo stays idempotent — every re-run fetches the same combo,
-- the NOT EXISTS guard at the merge step absorbs the overlap, and
-- silver ends up in the same state regardless of run count.

CREATE API INGEST {{zone_name}}.nager_date_holidays.public_holidays
    ENDPOINT '/api/v3/PublicHolidays/{year}/{country_code}'
    RESPONSE FORMAT JSON
    OPTIONS (
        path_param.year         = '2025',
        path_param.country_code = 'NO'
    );

-- --------------------------------------------------------------------------
-- 6. INVOKE — live HTTPS fetch with path params substituted
-- --------------------------------------------------------------------------
-- The engine assembles the URL from the template + path_param map,
-- issues the GET, and writes the JSON response to
--   <zone-root>/nager_date_holidays/public_holidays/<run-ts>/page_0001.json.

INVOKE API INGEST {{zone_name}}.nager_date_holidays.public_holidays;

-- --------------------------------------------------------------------------
-- 7. Bronze external table over the landed JSON
-- --------------------------------------------------------------------------
-- Nager.Date returns a bare top-level array of holiday objects, not a
-- wrapped envelope. root_path = "$" walks that array element-by-element
-- and maps the six fields we'll merge into silver. `recursive` walks
-- the timestamped per-run subfolders so a future wave's pages are
-- picked up automatically without schema changes.

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
-- 8. Anti-join merge — bronze → silver on composite key
-- --------------------------------------------------------------------------
-- (country_code, holiday_year, holiday_date) is the natural primary key
-- for a holiday calendar. The NOT EXISTS guard on that composite key
-- is what makes this INSERT re-runnable: a replay without a fresh
-- INVOKE is a no-op, not a duplicate-insert. holiday_year is stamped
-- as 2025 from the wave configuration, not pulled from bronze, because
-- Nager.Date's response doesn't carry the year as a separate field
-- (it's implicit in the path we requested). CAST on bronze's inferred
-- columns aligns them with silver's declared types.

INSERT INTO {{zone_name}}.hr_calendar.country_holidays
SELECT
    b.country_code,
    2025                           AS holiday_year,
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
      AND s.holiday_year = 2025
      AND s.holiday_date = CAST(b.holiday_date AS DATE)
);
