-- ============================================================================
-- Demo: Planetarium APOD Archive, api_key_query Auth
-- Feature: CREATE CREDENTIAL + auth_mode = 'api_key_query'
-- ============================================================================
--
-- Real-world story: a science-center planetarium caches NASA's
-- Astronomy Picture of the Day (APOD) so its daily exhibit pulls the
-- image, caption, and scientist-written explanation from a local
-- catalog rather than hitting NASA's rate-limited public API on every
-- display refresh. The archive is rebuilt weekly, pulling a 7-8 day
-- window at a time.
--
-- This file declares the catalog objects only. The INVOKE that issues
-- the actual HTTPS request, the per-run audit, the schema detection,
-- and the bronze->silver promotion all live in queries.sql so the
-- user can see in one place how a query-string-credentialed REST
-- endpoint is driven from SQL.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Credential, NASA DEMO_KEY, stored in OS Keychain (default backend)
-- --------------------------------------------------------------------------
-- `DEMO_KEY` is NASA's public shared test key, documented at
-- https://api.nasa.gov and intentionally passed around in demos. A
-- real deployment would replace the SECRET value with a team-issued
-- key from https://api.nasa.gov (just change this one line, the
-- downstream pipeline is identical).

CREATE CREDENTIAL IF NOT EXISTS nasa_apod_key
    TYPE = CREDENTIAL
    SECRET 'DEMO_KEY'
    DESCRIPTION 'NASA Astronomy Picture of the Day public demo key';

-- --------------------------------------------------------------------------
-- 2. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.nasa_api
    COMMENT 'Planetarium exhibit catalog, NASA APOD archive';

-- --------------------------------------------------------------------------
-- 3. Connection, auth_mode = 'api_key_query'
-- --------------------------------------------------------------------------
-- `auth_mode = 'api_key_query'` tells the engine to splice
-- `api_key=<secret>` into every request's query string. The default
-- param name is `api_key` (exactly what NASA expects); a future
-- endpoint-level override could change it per API if needed. The
-- `CREDENTIAL = nasa_apod_key` binding is what the engine resolves
-- at session-token build time, the secret material is inner-sealed
-- into the token so the engine never pulls the value out of the
-- keychain on the per-page HTTP path.

CREATE CONNECTION IF NOT EXISTS nasa_api
    TYPE = rest_api
    OPTIONS (
        base_url        = 'https://api.nasa.gov',
        auth_mode       = 'api_key_query',
        auth_query_name = 'api_key',
        storage_zone    = '{{zone_name}}',
        base_path       = 'nasa_api',
        timeout_secs    = '30'
    )
    CREDENTIAL = nasa_apod_key;

-- --------------------------------------------------------------------------
-- 4. API endpoint, fixed date window in the URL
-- --------------------------------------------------------------------------
-- The api_key (secret material) is handled by the connection-level
-- auth_mode and never appears in the URL. start_date/end_date pin the
-- 8-day archive window inline so the response is deterministic.

CREATE API ENDPOINT {{zone_name}}.nasa_api.apod_archive
    URL '/planetary/apod?start_date=2024-12-20&end_date=2024-12-27'
    RESPONSE FORMAT JSON
    OPTIONS (
        rate_limit_rps     = '1',
        retry_max_attempts = '3'
    );

-- --------------------------------------------------------------------------
-- 5. Bronze external table, flatten each APOD entry
-- --------------------------------------------------------------------------
-- With start_date+end_date, NASA returns a JSON array of 8 APOD
-- records. The bare `$` root_path + top-level include_paths match the
-- pattern from the existing public-holiday-calendar-sync demo.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.nasa_api.apod_bronze
USING JSON
LOCATION 'nasa_api/apod_archive'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.date",
            "$.title",
            "$.explanation",
            "$.media_type",
            "$.url",
            "$.hdurl",
            "$.service_version",
            "$.copyright"
        ],
        "column_mappings": {
            "$.date":             "apod_date",
            "$.title":            "title",
            "$.explanation":      "explanation",
            "$.media_type":       "media_type",
            "$.url":              "media_url",
            "$.hdurl":             "hd_url",
            "$.service_version":  "service_version",
            "$.copyright":        "copyright_holder"
        },
        "max_depth": 2,
        "separator": "_",
        "infer_types": true
    }'
);

-- --------------------------------------------------------------------------
-- 6. Silver Delta table, schema-only declaration
-- --------------------------------------------------------------------------
-- APOD's date field is ISO-8601 `YYYY-MM-DD`. Casting to DATE at
-- promotion (in queries.sql) lets exhibit queries do
-- `WHERE apod_date = DATE '2024-12-25'` natively without string
-- comparison.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.nasa_api.apod_silver (
    apod_date         DATE,
    title             STRING,
    explanation       STRING,
    media_type        STRING,
    media_url         STRING,
    hd_url            STRING,
    service_version   STRING,
    copyright_holder  STRING
)
LOCATION 'silver/apod_archive';
