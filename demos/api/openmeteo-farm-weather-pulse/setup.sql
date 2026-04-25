-- ============================================================================
-- Demo: Farm Weather Pulse, One Endpoint Per Farm
-- Feature: Multiple endpoints under one connection, each with the
--          farm's coordinates baked into the URL.
-- ============================================================================
--
-- Real-world story: an agritech platform runs a weather-monitoring
-- service across 3 partner farms in Northern Europe. The agronomy team
-- reads the shared bronze table every morning to correlate yields with
-- overnight temperature, humidity, and precipitation.
--
-- ONE connection. THREE endpoints (one per farm). THREE INVOKE lines
-- in queries.sql. Each endpoint targets a single farm's coordinates;
-- shared knobs (current= fields, timezone= UTC) live directly in each
-- URL.
--
-- This file declares the catalog objects only. The three INVOKE calls,
-- the per-endpoint run audits, the schema detection, and the
-- bronze->silver promotion all live in queries.sql so the user can
-- see the multi-endpoint fan-out from a single file.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.openmeteo_api
    COMMENT 'Agronomy weather telemetry for partner farms';

-- --------------------------------------------------------------------------
-- 2. REST API connection, Open-Meteo public forecast API
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
-- 3. Three endpoints, one per farm, coordinates baked into the URL
-- --------------------------------------------------------------------------

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
-- 4. Bronze external table, flatten the nested `$.current` block
-- --------------------------------------------------------------------------
-- Open-Meteo wraps current observations in a `$.current` sub-object
-- sibling to the top-level coordinates. The flatten's column_mappings
-- descend into that sub-object to produce a wide, flat one-row-per-
-- farm shape.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.openmeteo_api.weather_bronze
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

-- --------------------------------------------------------------------------
-- 5. Silver Delta table, schema-only declaration
-- --------------------------------------------------------------------------
-- The agronomy team's dashboards want a farm_name column they can
-- group by. The bronze->silver INSERT in queries.sql matches each
-- lat/lon to the canonical name via a CASE expression at promotion.
-- Typed columns (DOUBLE for temperature, etc.) let downstream
-- predicates work without casting.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.openmeteo_api.weather_silver (
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
