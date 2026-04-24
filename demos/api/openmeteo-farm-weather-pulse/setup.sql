-- ============================================================================
-- Demo: Farm Weather Pulse — Query-Param Overrides via USING
-- Feature: INVOKE ... USING (query_param.* = $x) + SET $name = <expr>
--          standalone script params. Same endpoint hit three times with
--          three sets of coordinates.
-- ============================================================================
--
-- Real-world story: an agritech platform runs a weather-monitoring
-- service across 3 partner farms in Northern Europe. The agronomy team
-- reads the shared bronze table every morning to correlate yields with
-- overnight temperature, humidity, and precipitation.
--
-- ONE endpoint. THREE farms. THREE INVOKE lines.
--
-- The endpoint has NO hardcoded coordinates — only the shared knobs
-- (current= fields, timezone= UTC). Each INVOKE supplies
-- `query_param.latitude` + `query_param.longitude` via USING, pulled
-- from script-scoped `$lat_X` / `$lon_X` parameters. Adding a fourth
-- farm is two SETs + one INVOKE line added to this file — no ALTER
-- API ENDPOINT needed.
--
-- IMPORTANT: this demo's statements share script-scoped parameters. The
-- demo harness MUST execute this file as a SINGLE multi-statement script
-- (one `execute_script_stream` call), not statement-by-statement. The
-- script param bag is cleared between script invocations, so splitting
-- the SETs and INVOKEs across separate HTTP calls would wipe $lat_X /
-- $lon_X between statements.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.agri_telemetry
    COMMENT 'Agronomy weather telemetry for partner farms';

-- --------------------------------------------------------------------------
-- 2. REST API connection — Open-Meteo public forecast API
-- --------------------------------------------------------------------------

CREATE CONNECTION IF NOT EXISTS openmeteo_api
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://api.open-meteo.com',
        auth_mode    = 'none',
        storage_zone = '{{zone_name}}',
        base_path    = 'openmeteo_api',
        timeout_secs = '30'
    );

-- --------------------------------------------------------------------------
-- 3. Three endpoints — one per farm, coordinates baked into the URL
-- --------------------------------------------------------------------------
-- Each endpoint targets a single farm's coordinates. Shared knobs
-- (current= fields, timezone= UTC) live directly in each URL.

CREATE API ENDPOINT {{zone_name}}.openmeteo_api.observation_oslo
    URL '/v1/forecast?latitude=59.91&longitude=10.75&current=temperature_2m,wind_speed_10m,relative_humidity_2m,precipitation&timezone=UTC'
    RESPONSE FORMAT JSON
    OPTIONS (rate_limit_rps = '2');

CREATE API ENDPOINT {{zone_name}}.openmeteo_api.observation_hamburg
    URL '/v1/forecast?latitude=53.55&longitude=9.99&current=temperature_2m,wind_speed_10m,relative_humidity_2m,precipitation&timezone=UTC'
    RESPONSE FORMAT JSON
    OPTIONS (rate_limit_rps = '2');

CREATE API ENDPOINT {{zone_name}}.openmeteo_api.observation_dublin
    URL '/v1/forecast?latitude=53.35&longitude=-6.26&current=temperature_2m,wind_speed_10m,relative_humidity_2m,precipitation&timezone=UTC'
    RESPONSE FORMAT JSON
    OPTIONS (rate_limit_rps = '2');

-- --------------------------------------------------------------------------
-- 4. Three INVOKEs — one per farm
-- --------------------------------------------------------------------------
-- Each INVOKE writes a distinct JSON page into its endpoint's per-run folder.

INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.observation_oslo;

INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.observation_hamburg;

INVOKE API ENDPOINT {{zone_name}}.openmeteo_api.observation_dublin;

-- --------------------------------------------------------------------------
-- 6. External table — flatten the nested `$.current` block
-- --------------------------------------------------------------------------
-- Open-Meteo wraps current observations in a `$.current` sub-object
-- sibling to the top-level coordinates. The flatten's column_mappings
-- descend into that sub-object to produce a wide, flat one-row-per-
-- farm shape.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.agri_telemetry.weather_bronze
USING JSON
LOCATION 'openmeteo_api'
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

DETECT SCHEMA FOR TABLE {{zone_name}}.agri_telemetry.weather_bronze;

-- --------------------------------------------------------------------------
-- 7. Silver Delta table — typed promotion with farm_name lookup
-- --------------------------------------------------------------------------
-- The agronomy team's dashboards want a farm_name column they can
-- group by — matching each lat/lon to the canonical name via a CASE
-- expression at promotion. Typed columns (DOUBLE for temperature, etc.)
-- let downstream predicates work without casting.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.agri_telemetry.weather_silver (
    farm_name         STRING,
    latitude          DOUBLE,
    longitude         DOUBLE,
    elevation_m       DOUBLE,
    observation_time  STRING,
    temperature_c     DOUBLE,
    wind_speed_kmh    DOUBLE,
    humidity_pct      DOUBLE,
    precipitation_mm  DOUBLE
)
LOCATION 'silver/farm_weather';

INSERT INTO {{zone_name}}.agri_telemetry.weather_silver
SELECT
    CASE
        WHEN CAST(longitude AS DOUBLE) BETWEEN 10.5 AND 11 THEN 'oslo'
        WHEN CAST(longitude AS DOUBLE) BETWEEN 9.5 AND 10.5 THEN 'hamburg'
        WHEN CAST(longitude AS DOUBLE) BETWEEN -7 AND -5 THEN 'dublin'
        ELSE 'unknown'
    END                                 AS farm_name,
    CAST(latitude AS DOUBLE)            AS latitude,
    CAST(longitude AS DOUBLE)           AS longitude,
    CAST(elevation_m AS DOUBLE)         AS elevation_m,
    observation_time,
    CAST(temperature_c AS DOUBLE)       AS temperature_c,
    CAST(wind_speed_kmh AS DOUBLE)      AS wind_speed_kmh,
    CAST(humidity_pct AS DOUBLE)        AS humidity_pct,
    CAST(precipitation_mm AS DOUBLE)    AS precipitation_mm
FROM {{zone_name}}.agri_telemetry.weather_bronze;

