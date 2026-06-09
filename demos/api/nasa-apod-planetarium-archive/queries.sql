-- ============================================================================
-- Demo: Planetarium APOD Archive, Queries
-- ============================================================================
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used.
--
-- Block ordering note: INVOKE is isolated in its own block. The bronze
-- external table apod_bronze is created here (Block 4), after the INVOKE
-- has written files, not in setup.sql: CREATE EXTERNAL TABLE validates
-- and detects schema against the landed files, so the landing folder
-- must exist first. The planner pre-registers external tables across the
-- whole script and JSON registration fails on empty directories, so the
-- CREATE and any block referencing apod_bronze must run after the INVOKE.
-- ============================================================================

-- ============================================================================
-- Block 1: describe the endpoint
-- ============================================================================

DESCRIBE API ENDPOINT {{zone_name}}.nasa_api.apod_archive;

-- ============================================================================
-- Block 2: INVOKE the endpoint (isolated)
-- ============================================================================
-- Single HTTPS GET against api.nasa.gov; the URL's start_date/end_date
-- window returns a JSON array in one response.

INVOKE API ENDPOINT {{zone_name}}.nasa_api.apod_archive;

-- ============================================================================
-- Block 3: per-run audit
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.nasa_api.apod_archive LIMIT 5;

-- ============================================================================
-- Block 4: detect bronze schema
-- ============================================================================
-- The bronze external table is created here, after the INVOKE above has
-- landed its files, because CREATE EXTERNAL TABLE validates and detects
-- schema against the landed files, so the landing folder must exist first.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.nasa_api.apod_bronze
USING JSON
LOCATION 'nasa-apod-planetarium-archive/nasa_api/apod_archive'
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

DETECT SCHEMA FOR TABLE {{zone_name}}.nasa_api.apod_bronze;

-- ============================================================================
-- Block 5: bronze feed landed
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    apod_date,
    title,
    media_type,
    media_url
FROM {{zone_name}}.nasa_api.apod_bronze
ORDER BY apod_date;

-- ============================================================================
-- Block 6: bronze -> silver promotion
-- ============================================================================

INSERT INTO {{zone_name}}.nasa_api.apod_silver
SELECT
    CAST(apod_date AS DATE) AS apod_date,
    title,
    explanation,
    media_type,
    media_url,
    hd_url,
    service_version,
    copyright_holder
FROM {{zone_name}}.nasa_api.apod_bronze;

-- ============================================================================
-- Block 7: silver curated records
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    apod_date,
    title,
    media_type,
    media_url
FROM {{zone_name}}.nasa_api.apod_silver
ORDER BY apod_date;

-- ============================================================================
-- Block 8: media type distribution
-- ============================================================================

SELECT
    media_type,
    COUNT(*) AS entry_count
FROM {{zone_name}}.nasa_api.apod_silver
GROUP BY media_type
ORDER BY media_type;

-- ============================================================================
-- Block 9: silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.nasa_api.apod_silver;
