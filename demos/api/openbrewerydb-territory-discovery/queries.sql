-- ============================================================================
-- Demo: Brewery Territory Prospect Catalog, Queries
-- ============================================================================
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used, because
-- the brewery directory is live data and its contents shift over time.
--
-- Block ordering note: INVOKE is isolated in its own block. DISCOVER
-- reads the actual bytes of the landed files to detect the format, so
-- both DISCOVER blocks (and every block referencing breweries_bronze)
-- must run after the INVOKE has written the JSON page.
-- ============================================================================

-- ============================================================================
-- Block 1: registry inspection
-- ============================================================================

SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.openbrewerydb_api;

-- ============================================================================
-- Block 2: INVOKE the California brewery directory fetch (isolated)
-- ============================================================================
-- One GET against /v1/breweries?by_state=california&per_page=50. The
-- response body (a flat JSON array of brewery records) lands as one
-- file in a timestamped per-run folder under the connection's
-- base_path.

INVOKE API ENDPOINT {{zone_name}}.openbrewerydb_api.california_breweries;

-- ============================================================================
-- Block 3: per-endpoint run audit
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.openbrewerydb_api.california_breweries LIMIT 5;

-- ============================================================================
-- Block 4: DISCOVER in PRINT mode, review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER
-- would run, without registering anything: format detection reads the
-- landed file's actual bytes (not its extension), classifies the text
-- shape as JSON, and synthesizes the flatten options. RECURSIVE
-- descends through the timestamped per-run folder INVOKE created;
-- FILE_METADATA adds the df_file_name / df_row_number provenance
-- columns to the generated table. Nothing exists in the catalog after
-- this block: that is the point of PRINT.

DISCOVER {{zone_name}}.openbrewerydb_api.breweries_bronze
    PATH 'openbrewerydb-territory-discovery/openbrewerydb_api'
    WITH (RECURSIVE = true, FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Block 5: DISCOVER in EXECUTE mode, auto-register the bronze table
-- ============================================================================
-- Same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence).
-- This single statement replaces the hand-written CREATE EXTERNAL
-- TABLE + json_flatten_config block that every sibling API demo
-- carries: the format and its OPTIONS are derived from the data.

DISCOVER {{zone_name}}.openbrewerydb_api.breweries_bronze
    PATH 'openbrewerydb-territory-discovery/openbrewerydb_api'
    WITH (RECURSIVE = true, FILE_METADATA = true);

-- ============================================================================
-- Block 6: bronze feed landed, with file provenance
-- ============================================================================
-- df_file_name comes from the FILE_METADATA = true discovery option and
-- points every row back to the landed page that produced it.

ASSERT ROW_COUNT > 0
SELECT
    name,
    brewery_type,
    city,
    latitude,
    longitude,
    df_file_name
FROM {{zone_name}}.openbrewerydb_api.breweries_bronze;

-- ============================================================================
-- Block 7: bronze -> silver promotion with territory assignment
-- ============================================================================
-- The distributor splits California at the 36th parallel: at or above
-- is the northern rep's territory, below is the southern rep's.
-- Directory entries without coordinates can't be routed to a rep and
-- are excluded from the prospect catalog.

INSERT INTO {{zone_name}}.openbrewerydb_api.brewery_prospects_silver
SELECT
    id                                  AS brewery_id,
    name                                AS brewery_name,
    brewery_type,
    city,
    postal_code,
    CAST(latitude  AS DOUBLE)           AS latitude,
    CAST(longitude AS DOUBLE)           AS longitude,
    phone,
    website_url,
    CASE
        WHEN CAST(latitude AS DOUBLE) >= 36.0 THEN 'norcal'
        ELSE 'socal'
    END                                 AS sales_territory
FROM {{zone_name}}.openbrewerydb_api.breweries_bronze
WHERE latitude IS NOT NULL;

-- ============================================================================
-- Block 8: territory planning rollup
-- ============================================================================
-- The view the sales-ops team actually works from: prospect counts per
-- territory and brewery type, biggest segments first.

ASSERT ROW_COUNT > 0
SELECT
    sales_territory,
    brewery_type,
    COUNT(*) AS prospect_count
FROM {{zone_name}}.openbrewerydb_api.brewery_prospects_silver
GROUP BY sales_territory, brewery_type
ORDER BY sales_territory, prospect_count DESC;

-- ============================================================================
-- Block 9: silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.openbrewerydb_api.brewery_prospects_silver;
