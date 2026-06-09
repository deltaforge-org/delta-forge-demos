-- ============================================================================
-- Demo: Pokedex First-Generation Reference, Queries
-- ============================================================================
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used.
--
-- Block ordering note: INVOKE is isolated in its own block. The bronze
-- external table is created here in Block 5, after the INVOKE, not in
-- setup.sql, because CREATE EXTERNAL TABLE validates and detects schema
-- against the landed files, so the landing folder must exist first. The
-- planner pre-registers external tables across the whole script and JSON
-- registration fails on empty directories, so the bronze CREATE and any
-- block referencing pokedex_bronze must run after the INVOKE has written
-- files. The bronze CREATE is kept out of the INVOKE block itself.
-- ============================================================================

-- ============================================================================
-- Block 1: describe the endpoint
-- ============================================================================

DESCRIBE API ENDPOINT {{zone_name}}.pokedex_api.first_generation;

-- ============================================================================
-- Block 2: CALL preview (no flatten, no disk write)
-- ============================================================================
-- Returns (_page_index INT, _raw_body STRING) for the first page only,
-- useful for inspecting the wire shape before authoring json_flatten_config.

CALL API ENDPOINT {{zone_name}}.pokedex_api.first_generation LIMIT 1 PAGE;

-- ============================================================================
-- Block 3: INVOKE the endpoint (isolated)
-- ============================================================================
-- Drives the offset crawl across 5 pages: offsets 0, 20, 40, 60, 80.

INVOKE API ENDPOINT {{zone_name}}.pokedex_api.first_generation;

-- ============================================================================
-- Block 4: per-run audit
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.pokedex_api.first_generation LIMIT 5;

-- ============================================================================
-- Block 5: detect bronze schema
-- ============================================================================
-- The bronze external table is created here, after the INVOKE, because
-- CREATE EXTERNAL TABLE validates and detects schema against the landed
-- files, so the landing folder must already exist. PokeAPI wraps pages as
-- {count, next, previous, results: [...]}. The flatten root_path dives into
-- $.results so every entry in the array becomes one table row. include_paths
-- and column_mappings are then relative to each results[i], `$.name`, `$.url`.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.pokedex_api.pokedex_bronze
USING JSON
LOCATION 'pokeapi-pokedex-first-generation/pokedex_api/first_generation'
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

DETECT SCHEMA FOR TABLE {{zone_name}}.pokedex_api.pokedex_bronze;

-- ============================================================================
-- Block 6: bronze feed landed
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    pokemon_name,
    detail_url
FROM {{zone_name}}.pokedex_api.pokedex_bronze
LIMIT 10;

-- ============================================================================
-- Block 7: bronze -> silver promotion
-- ============================================================================

INSERT INTO {{zone_name}}.pokedex_api.pokedex_silver
SELECT
    CAST(
        REGEXP_REPLACE(detail_url, '^.*/pokemon/([0-9]+)/$', '\1')
        AS BIGINT
    )           AS dex_id,
    pokemon_name,
    detail_url
FROM {{zone_name}}.pokedex_api.pokedex_bronze;

-- ============================================================================
-- Block 8: silver with parsed dex_id
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    dex_id,
    pokemon_name,
    detail_url
FROM {{zone_name}}.pokedex_api.pokedex_silver
ORDER BY dex_id
LIMIT 10;

-- ============================================================================
-- Block 9: dex_id range overview
-- ============================================================================

SELECT
    MIN(dex_id)            AS min_dex,
    MAX(dex_id)            AS max_dex,
    COUNT(*)               AS total_rows,
    COUNT(DISTINCT dex_id) AS distinct_ids
FROM {{zone_name}}.pokedex_api.pokedex_silver;

-- ============================================================================
-- Block 10: silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.pokedex_api.pokedex_silver;
