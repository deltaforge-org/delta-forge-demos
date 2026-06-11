-- ============================================================================
-- Demo: Brewery Territory Prospect Catalog, DISCOVER Auto-Registration
-- Feature: DISCOVER (PRINT + EXECUTE modes, RECURSIVE + FILE_METADATA
--          options) replacing hand-written CREATE EXTERNAL TABLE DDL
-- ============================================================================
--
-- Real-world story: a craft-beverage distributor's sales-ops team is
-- expanding into California. They pull the state's brewery directory
-- from Open Brewery DB (public, no auth, no API key) to build an
-- account-prospect catalog, then split it into north/south sales
-- territories for rep assignment.
--
-- Every other API demo hand-writes a CREATE EXTERNAL TABLE with an
-- explicit json_flatten_config over the landed files. This demo is the
-- DISCOVER walkthrough: after INVOKE lands the raw API response, a
-- single DISCOVER statement reads the actual bytes, detects the format,
-- synthesizes the flatten options, and registers the bronze external
-- table on its own. PRINT mode is shown first so the generated DDL can
-- be reviewed before EXECUTE commits the registration.
--
-- This file declares the catalog objects only: the zone, schema,
-- connection, the endpoint, and the silver Delta table. The bronze
-- external table is NOT declared here, and unlike the sibling demos it
-- is not hand-created in queries.sql either: DISCOVER registers it
-- after INVOKE has landed files. Discovery reads the file's actual
-- bytes, so the landing folder must exist first; the INVOKE, the run
-- audit, both DISCOVER modes, and the bronze->silver promotion all
-- live in queries.sql.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.openbrewerydb_api
    COMMENT 'Craft brewery prospect catalog for distributor territory planning';

-- --------------------------------------------------------------------------
-- 2. Connection, Open Brewery DB public directory service
-- --------------------------------------------------------------------------
-- No auth, no API key, no enforced rate limit; the endpoint still sets
-- a polite rate_limit_rps below.

CREATE CONNECTION IF NOT EXISTS openbrewerydb_api
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://api.openbrewerydb.org',
        auth_mode    = 'none',
        storage_zone = '{{zone_name}}',
        base_path    = 'openbrewerydb-territory-discovery/openbrewerydb_api',
        timeout_secs = '30'
    );

-- --------------------------------------------------------------------------
-- 3. Endpoint, one page of California breweries
-- --------------------------------------------------------------------------
-- The state filter and page size ride as query_param.* options rather
-- than being baked into the URL, so repointing the catalog at another
-- state later is a one-line ALTER (or an INVOKE ... USING override).
-- One page of 50 keeps the fetch to a single polite GET.

CREATE API ENDPOINT {{zone_name}}.openbrewerydb_api.california_breweries
    URL '/v1/breweries'
    RESPONSE FORMAT JSON
    OPTIONS (
        'query_param.by_state' = 'california',
        'query_param.per_page' = '50',
        rate_limit_rps = '2'
    );

-- --------------------------------------------------------------------------
-- 4. Silver Delta table, schema-only declaration
-- --------------------------------------------------------------------------
-- The territory dashboards group by sales_territory and brewery_type.
-- Typed DOUBLE coordinates let the latitude split predicate work
-- without casting downstream. The bronze->silver INSERT in queries.sql
-- derives sales_territory from latitude at promotion time.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.openbrewerydb_api.brewery_prospects_silver (
    brewery_id      STRING,
    brewery_name    STRING,
    brewery_type    STRING,
    city            STRING,
    postal_code     STRING,
    latitude        DOUBLE,
    longitude       DOUBLE,
    phone           STRING,
    website_url     STRING,
    sales_territory STRING
)
LOCATION 'openbrewerydb-territory-discovery/silver/brewery_prospects';
