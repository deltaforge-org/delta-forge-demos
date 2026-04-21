-- ============================================================================
-- Cleanup: European Travel Reference Catalog
-- ============================================================================
-- Reverse order of creation: silver delta table → bronze external table →
-- API ingest → connection → vault entry → schema → zone. WITH FILES on
-- both tables also removes their on-disk artefacts (Delta log + parquet
-- for silver, raw JSON pages for bronze). The OS keychain backend is the
-- always-on default and is never dropped.
-- ============================================================================

-- 1. Silver Delta table (drops its log + parquet files)
DROP DELTA TABLE IF EXISTS {{zone_name}}.travel_geo.european_countries_silver WITH FILES;

-- 2. Bronze external table (also removes the JSON files INVOKE wrote)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.travel_geo.european_countries WITH FILES;

-- 3. API ingest definition (cascades its run history)
DROP API INGEST IF EXISTS {{zone_name}}.rest_countries.europe;

-- 4. REST API connection (data source)
DROP CONNECTION IF EXISTS rest_countries;

-- 5. Vault credential entry
DROP CREDENTIAL IF EXISTS travel_api_token;

-- 6. Schema then zone (zone last — schemas live under it)
DROP SCHEMA IF EXISTS {{zone_name}}.travel_geo;
-- Zone left in place by default — many demos may share `bronze`. Uncomment
-- if this demo runs in an isolated environment where the zone should go too.
-- DROP ZONE IF EXISTS {{zone_name}};
