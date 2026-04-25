-- ============================================================================
-- Demo: Pokedex First-Generation Reference, Offset Pagination + CALL Preview
-- Feature: pagination = 'offset' (offset_param, limit_param, limit, max_pages),
--          JSON flatten with nested root_path = "$.results"
-- ============================================================================
--
-- Real-world story: a mobile game studio maintains an internal Pokedex
-- reference catalog to power battle-balance analytics and in-game
-- creature search. The battle team pulls the first 100 National Dex
-- entries (Kanto-gen, Bulbasaur #1 through Voltorb #100) from PokeAPI,
-- lands them as JSON pages, projects them into a flat table, then
-- promotes to a typed silver Delta table with the integer dex_id
-- extracted from each entry's detail URL.
--
-- This file declares the catalog objects only. The CALL preview, the
-- INVOKE that drives the offset crawl, the run audit, the schema
-- detection, and the bronze->silver promotion all live in queries.sql
-- so the user can see in one place how an offset-paginated REST
-- endpoint is driven from SQL.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.pokedex_api
    COMMENT 'Game reference catalogs (Pokedex, moves, items)';

-- --------------------------------------------------------------------------
-- 2. REST API connection (public, no auth)
-- --------------------------------------------------------------------------
-- PokeAPI is a free community-maintained service. No auth required.

CREATE CONNECTION IF NOT EXISTS pokedex_api
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://pokeapi.co',
        auth_mode    = 'none',
        storage_zone = '{{zone_name}}',
        base_path    = 'pokedex_api',
        timeout_secs = '30'
    );

-- --------------------------------------------------------------------------
-- 3. API endpoint, OFFSET pagination
-- --------------------------------------------------------------------------
-- Offset pagination pairs two query-string params: the engine increments
-- the offset by `limit` each page until `max_pages` is reached. PokeAPI
-- uses `?offset=N&limit=K` so offset_param/limit_param are plain
-- `offset`/`limit`. rate_limit_rps = 4 is the polite throttle
-- (community service, no enforced limit but don't be that noisy client).

CREATE API ENDPOINT {{zone_name}}.pokedex_api.first_generation
    URL '/api/v2/pokemon'
    RESPONSE FORMAT JSON
    OPTIONS (
        pagination     = 'offset',
        offset_param   = 'offset',
        limit_param    = 'limit',
        limit          = '20',
        max_pages      = '5',
        rate_limit_rps = '4'
    );

-- --------------------------------------------------------------------------
-- 4. Bronze external table, root_path = "$.results" for wrapped arrays
-- --------------------------------------------------------------------------
-- PokeAPI wraps pages as {count, next, previous, results: [...]}. The
-- flatten root_path dives into $.results so every entry in the array
-- becomes one table row. include_paths and column_mappings are then
-- relative to each results[i], `$.name`, `$.url`.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.pokedex_api.pokedex_bronze
USING JSON
LOCATION 'pokedex_api/first_generation'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$.results",
        "include_paths": [
            "$.name",
            "$.url"
        ],
        "column_mappings": {
            "$.name": "pokemon_name",
            "$.url":  "detail_url"
        },
        "max_depth": 2,
        "separator": "_",
        "infer_types": true
    }'
);

-- --------------------------------------------------------------------------
-- 5. Silver Delta table, schema-only declaration
-- --------------------------------------------------------------------------
-- The bronze->silver INSERT in queries.sql parses dex_id out of the
-- detail_url with REGEXP_REPLACE + CAST so the battle team can JOIN
-- on dex_id without string parsing every query.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.pokedex_api.pokedex_silver (
    dex_id        BIGINT,
    pokemon_name  STRING,
    detail_url    STRING
)
LOCATION 'silver/pokedex_silver';
