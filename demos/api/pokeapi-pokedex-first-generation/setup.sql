-- ============================================================================
-- Demo: Pokédex First-Generation Reference — Offset Pagination + CALL Preview
-- Feature: pagination = 'offset' (offset_param, limit_param, limit, max_pages),
--          CALL API ENDPOINT ... LIMIT N PAGE, JSON flatten with nested
--          root_path = "$.results"
-- ============================================================================
--
-- Real-world story: a mobile game studio maintains an internal Pokédex
-- reference catalog to power battle-balance analytics and in-game
-- creature search. The battle team pulls the first 100 National Dex
-- entries (Kanto-gen, Bulbasaur #1 through Voltorb #100) from PokeAPI,
-- lands them as JSON pages, projects them into a flat table, then
-- promotes to a typed silver Delta table with the integer dex_id
-- extracted from each entry's detail URL.
--
-- Pipeline:
--   1. Zone + schema    — bronze landing + game_ref schema
--   2. REST connection  — PokeAPI is public, auth_mode = 'none'
--   3. API endpoint     — URL '/api/v2/pokemon' with OFFSET pagination:
--                           pagination    = 'offset'
--                           offset_param  = 'offset'
--                           limit_param   = 'limit'
--                           limit         = '20'  (20 rows per page)
--                           max_pages     = '5'   (→ 5 × 20 = 100 rows)
--   4. CALL preview     — LIMIT 1 PAGE fetches one response body into an
--                           in-memory buffer and returns it as
--                           (_page_index, _raw_body). No file write, no
--                           run-log update — the authoring-loop
--                           affordance for previewing the wire format.
--   5. INVOKE           — engine walks offsets 0, 20, 40, 60, 80 and
--                           lands 5 JSON envelope files.
--   6. External table   — JSON flatten with root_path = "$.results" to
--                           pull each envelope's results[] into rows.
--   7. Silver Delta     — typed promotion; dex_id parsed from detail_url.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.game_ref
    COMMENT 'Game reference catalogs (Pokédex, moves, items)';

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
-- 3. API endpoint — OFFSET pagination
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
-- 4. CALL API ENDPOINT — preview the wire format, no disk write
-- --------------------------------------------------------------------------
-- Raw one-page preview. Returns (_page_index INT, _raw_body STRING) with
-- no flatten, no parse, no run-log update. Useful before authoring the
-- json_flatten_config so you know what shape you're flattening. When
-- LIMIT N PAGE is present the pagination engine stays active up to N
-- pages; this demo uses LIMIT 1 PAGE for the first-page-only sanity
-- check.

CALL API ENDPOINT {{zone_name}}.pokedex_api.first_generation LIMIT 1 PAGE;

-- --------------------------------------------------------------------------
-- 5. INVOKE — actual HTTPS fetch across 5 pages
-- --------------------------------------------------------------------------
-- offsets 0, 20, 40, 60, 80 — each page writes one envelope file.

INVOKE API ENDPOINT {{zone_name}}.pokedex_api.first_generation;

-- --------------------------------------------------------------------------
-- 6. External table — root_path = "$.results" for wrapped arrays
-- --------------------------------------------------------------------------
-- PokeAPI wraps pages as {count, next, previous, results: [...]}. The
-- flatten root_path dives into $.results so every entry in the array
-- becomes one table row. include_paths and column_mappings are then
-- relative to each results[i] — `$.name`, `$.url`.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.game_ref.pokedex_bronze
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

DETECT SCHEMA FOR TABLE {{zone_name}}.game_ref.pokedex_bronze;
GRANT ADMIN ON TABLE {{zone_name}}.game_ref.pokedex_bronze TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 7. Silver Delta table — typed promotion with parsed dex_id
-- --------------------------------------------------------------------------
-- The detail_url ends in `/pokemon/<n>/` — REGEXP_REPLACE strips the
-- prefix/suffix and CAST promotes the remaining digits to BIGINT. Now
-- the battle team can JOIN on dex_id without string parsing every query.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.game_ref.pokedex_silver (
    dex_id        BIGINT,
    pokemon_name  STRING,
    detail_url    STRING
)
LOCATION 'silver/pokedex_silver';

INSERT INTO {{zone_name}}.game_ref.pokedex_silver
SELECT
    CAST(
        REGEXP_REPLACE(detail_url, '^.*/pokemon/([0-9]+)/$', '\1')
        AS BIGINT
    )                AS dex_id,
    pokemon_name,
    detail_url
FROM {{zone_name}}.game_ref.pokedex_bronze;

GRANT ADMIN ON TABLE {{zone_name}}.game_ref.pokedex_silver TO USER {{current_user}};
