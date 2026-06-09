-- ============================================================================
-- Demo: Farm Weather Pulse, Queries
-- ============================================================================
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used.
--
-- Block ordering note: INVOKE is isolated in its own block. The planner
-- pre-registers external tables across the whole script and JSON
-- registration fails on empty directories, so the bronze external table
-- is created here, not in setup.sql, and only after the three INVOKEs
-- have written files. CREATE EXTERNAL TABLE validates and detects schema
-- against the landed files, so any block creating or referencing
-- weather_bronze must run after the three INVOKEs have written files.
-- ============================================================================

-- ============================================================================
-- Block 1: registry inspection
-- ============================================================================

SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.openmeteo_api;

-- ============================================================================
-- Block 2: INVOKE all three endpoints (isolated)
-- ============================================================================
-- One INVOKE per farm. Each writes a distinct JSON page into its
-- endpoint's per-run folder; the bronze table picks them all up via
-- recursive scan.

INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.observation_oslo;
INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.observation_hamburg;
INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.observation_dublin;

-- ============================================================================
-- Block 3: per-endpoint run audit (oslo)
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.openmeteo_api.observation_oslo LIMIT 5;

-- ============================================================================
-- Block 4: per-endpoint run audit (hamburg)
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.openmeteo_api.observation_hamburg LIMIT 5;

-- ============================================================================
-- Block 5: per-endpoint run audit (dublin)
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.openmeteo_api.observation_dublin LIMIT 5;

-- ============================================================================
-- Block 6: create bronze external table, then detect its schema
-- ============================================================================
-- The bronze external table is created here, after the three INVOKE
-- calls, because CREATE EXTERNAL TABLE validates and detects schema
-- against the landed files. The landing folder must exist (the INVOKEs
-- must have written their JSON pages) before this runs.
--
-- Open-Meteo wraps current observations in a `$.current` sub-object
-- sibling to the top-level coordinates. The flatten's column_mappings
-- descend into that sub-object to produce a wide, flat one-row-per-
-- farm shape.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.openmeteo_api.weather_bronze
USING JSON
LOCATION 'openmeteo-farm-weather-pulse/openmeteo_api'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.latitude",
            "$.longitude",
            "$.elevation",
            "$.timezone",
            "$.current.time",
            "$.current.temperature_2m",
            "$.current.wind_speed_10m",
            "$.current.relative_humidity_2m",
            "$.current.precipitation"
        ],
        "column_mappings": {
            "$.latitude":                    "latitude",
            "$.longitude":                   "longitude",
            "$.elevation":                   "elevation_m",
            "$.timezone":                    "timezone",
            "$.current.time":                "observation_time",
            "$.current.temperature_2m":      "temperature_c",
            "$.current.wind_speed_10m":      "wind_speed_kmh",
            "$.current.relative_humidity_2m":"humidity_pct",
            "$.current.precipitation":       "precipitation_mm"
        },
        "max_depth": 3,
        "separator": "_",
        "infer_types": true
    }'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.openmeteo_api.weather_bronze;

-- ============================================================================
-- Block 7: bronze feed landed
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    latitude,
    longitude,
    observation_time,
    temperature_c,
    humidity_pct
FROM {{zone_name}}.openmeteo_api.weather_bronze;

-- ============================================================================
-- Block 8: bronze -> silver promotion
-- ============================================================================

INSERT INTO {{zone_name}}.openmeteo_api.weather_silver
SELECT
    CASE
        WHEN CAST(longitude AS DOUBLE) BETWEEN 10.5 AND 11  THEN 'oslo'
        WHEN CAST(longitude AS DOUBLE) BETWEEN 9.5 AND 10.5 THEN 'hamburg'
        WHEN CAST(longitude AS DOUBLE) BETWEEN -7  AND -5   THEN 'dublin'
        ELSE 'unknown'
    END                              AS farm_name,
    CAST(latitude        AS DOUBLE)  AS latitude,
    CAST(longitude       AS DOUBLE)  AS longitude,
    CAST(elevation_m     AS DOUBLE)  AS elevation_m,
    observation_time,
    CAST(temperature_c   AS DOUBLE)  AS temperature_c,
    CAST(wind_speed_kmh  AS DOUBLE)  AS wind_speed_kmh,
    CAST(humidity_pct    AS DOUBLE)  AS humidity_pct,
    CAST(precipitation_mm AS DOUBLE) AS precipitation_mm
FROM {{zone_name}}.openmeteo_api.weather_bronze;

-- ============================================================================
-- Block 9: silver per-farm observations
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    farm_name,
    latitude,
    longitude,
    observation_time,
    temperature_c,
    wind_speed_kmh,
    humidity_pct,
    precipitation_mm
FROM {{zone_name}}.openmeteo_api.weather_silver
ORDER BY farm_name;

-- ============================================================================
-- Block 10: silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.openmeteo_api.weather_silver;
