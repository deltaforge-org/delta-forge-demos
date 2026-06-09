-- ============================================================================
-- Cleanup: Pokédex First-Generation Reference
-- ============================================================================
-- Reverse order of creation. Zone is left in place — sibling API demos
-- share `bronze`. WITH FILES on both tables also removes their on-disk
-- artefacts (Delta log + parquet for silver, raw JSON envelopes for
-- bronze).
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.pokedex_api.pokedex_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.pokedex_api.pokedex_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.pokedex_api.first_generation;

DROP CONNECTION IF EXISTS pokedex_api;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'pokeapi-pokedex-first-generation' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.pokedex_api;
