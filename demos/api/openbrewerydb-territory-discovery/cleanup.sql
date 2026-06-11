-- ============================================================================
-- Cleanup: Brewery Territory Prospect Catalog
-- ============================================================================
-- breweries_bronze was registered by DISCOVER (not by a hand-written
-- CREATE EXTERNAL TABLE), but it is an ordinary external table in the
-- catalog and drops the same way. IF EXISTS keeps this harmless when a
-- prior run failed before the DISCOVER EXECUTE block.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.openbrewerydb_api.brewery_prospects_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.openbrewerydb_api.breweries_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.openbrewerydb_api.california_breweries;

DROP CONNECTION IF EXISTS openbrewerydb_api;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'openbrewerydb-territory-discovery' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.openbrewerydb_api;
