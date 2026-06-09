-- ============================================================================
-- Demo: Public Holiday Calendar Sync, Queries
-- ============================================================================
-- This file exercises the script-parameter-driven incremental ingest
-- flow end to end. The target-table-driven next-wave capture, INVOKE
-- USING with runtime path-param overrides, bronze external table
-- creation (done here, after the INVOKE, because CREATE EXTERNAL TABLE
-- validates and detects schema against the landed files), and the
-- idempotent anti-join merge all live here.
--
-- IMPORTANT: this file MUST execute as a SINGLE multi-statement script
-- (one execute_script_stream call), not statement-by-statement. The
-- script param bag ($next_country, $next_year) is cleared between
-- script invocations, so splitting the SELECT INTO / INVOKE / INSERT
-- across separate calls would wipe the values mid-flight.
--
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used. Public
-- holiday counts vary by country and year so exact row counts or specific
-- date values are never asserted.
-- ============================================================================

-- ============================================================================
-- API surface, calling the endpoint from SQL
-- ============================================================================

-- Inspect the URL-template endpoint catalog row before invoking.
DESCRIBE API ENDPOINT {{zone_name}}.holiday_calendar.public_holidays;

-- Capture next-wave params from the silver target. MAX() is aggregate,
-- so the query always returns exactly one row (safe for INTO grammar).
-- COALESCE bootstraps at 2024 when silver has no Norway rows yet.
SELECT
    'NO'                                      AS next_country,
    COALESCE(MAX(holiday_year), 2024) + 1     AS next_year
FROM {{zone_name}}.holiday_calendar.country_holidays
WHERE country_code = 'NO'
INTO $next_country, $next_year;

-- INVOKE with runtime-resolved path params via USING (...). The engine
-- resolves $next_year / $next_country against the script param bag
-- and assembles the URL:
--     https://date.nager.at/api/v3/PublicHolidays/<year>/<country>
INVOKE API ENDPOINT {{zone_name}}.holiday_calendar.public_holidays
    USING (
        path_param.year         = $next_year,
        path_param.country_code = $next_country
    );

-- Per-run audit row.
SHOW API ENDPOINT RUNS {{zone_name}}.holiday_calendar.public_holidays LIMIT 5;

-- Bronze external table over the landed JSON. Created here, after the
-- INVOKE, because CREATE EXTERNAL TABLE validates and detects schema
-- against the landed files, so the landing folder must exist first.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.holiday_calendar.public_holidays_bronze
USING JSON
LOCATION 'public-holiday-calendar-sync/holiday_calendar/public_holidays'
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

-- Resolve the bronze schema from the freshly written JSON page.
DETECT SCHEMA FOR TABLE {{zone_name}}.holiday_calendar.public_holidays_bronze;

-- Anti-join merge from bronze to silver. $next_year is stamped onto
-- every merged row. NOT EXISTS on (country_code, holiday_year, holiday_date)
-- keeps replays idempotent.
INSERT INTO {{zone_name}}.holiday_calendar.country_holidays
SELECT
    b.country_code,
    $next_year                   AS holiday_year,
    CAST(b.holiday_date AS DATE) AS holiday_date,
    b.local_name,
    b.english_name,
    CAST(b.is_fixed  AS BOOLEAN) AS is_fixed,
    CAST(b.is_global AS BOOLEAN) AS is_global,
    'nager_api'                  AS source_batch
FROM {{zone_name}}.holiday_calendar.public_holidays_bronze b
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.holiday_calendar.country_holidays s
    WHERE s.country_code = b.country_code
      AND s.holiday_year = $next_year
      AND s.holiday_date = CAST(b.holiday_date AS DATE)
);

-- ============================================================================
-- Query 1: Bronze landing
-- ============================================================================
-- ROW_COUNT > 0 confirms the INVOKE with path params wrote data.

ASSERT ROW_COUNT > 0
SELECT
    holiday_date,
    local_name,
    english_name,
    is_global
FROM {{zone_name}}.holiday_calendar.public_holidays_bronze
ORDER BY holiday_date;

-- ============================================================================
-- Query 2: Silver after merge
-- ============================================================================
-- Show all rows in silver partitioned by source batch. The launch_seed
-- rows come from setup.sql; the nager_api rows are from this run.

ASSERT ROW_COUNT > 0
SELECT
    country_code,
    holiday_year,
    source_batch,
    COUNT(*) AS holiday_count
FROM {{zone_name}}.holiday_calendar.country_holidays
GROUP BY country_code, holiday_year, source_batch
ORDER BY country_code, holiday_year, source_batch;

-- ============================================================================
-- Query 3: Incremental watermark check
-- ============================================================================
-- This is the anti-join shape a scheduled pipeline runs BEFORE every
-- INVOKE to find which (country, year) combos still need fetching.
-- After the merge above, the result should be empty (wave completed).

SELECT COUNT(*) AS missing_combos
FROM (VALUES ('NO', 2025)) AS wanted(country_code, holiday_year)
WHERE NOT EXISTS (
    SELECT 1 FROM {{zone_name}}.holiday_calendar.country_holidays t
    WHERE t.country_code = wanted.country_code
      AND t.holiday_year = wanted.holiday_year
);

-- ============================================================================
-- Query 4: Silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.holiday_calendar.country_holidays;
