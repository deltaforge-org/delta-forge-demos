-- ============================================================================
-- Cleanup: Rust Release Catalog
-- ============================================================================
-- Reverse order of creation: silver delta table → bronze external table →
-- API endpoint → connection → schema → zone. WITH FILES on both tables
-- also removes their on-disk artefacts (Delta log + parquet for silver,
-- raw JSON pages for bronze). No credential was created by this demo
-- (the endpoint is public and uses auth_mode = 'none'), so nothing to
-- drop there.
-- ============================================================================

-- 1. Silver Delta table (drops its log + parquet files)
DROP DELTA TABLE IF EXISTS {{zone_name}}.release_intel.rust_releases_silver WITH FILES;

-- 2. Bronze external table (also removes the JSON files INVOKE wrote)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.release_intel.rust_releases WITH FILES;

-- 3. API endpoint definition (cascades its run history)
DROP API ENDPOINT IF EXISTS {{zone_name}}.github_releases.rust_releases;

-- 4. REST API connection (data source)
DROP CONNECTION IF EXISTS github_releases;

-- 5. Vault credential entry — defensive drop for any leftover entry from
--    an earlier run (the current setup uses auth_mode = 'none' and does
--    not create one, but IF EXISTS keeps this harmless either way).
DROP CREDENTIAL IF EXISTS github_api_token;

-- 6. Schema then zone (zone last — schemas live under it)
DROP SCHEMA IF EXISTS {{zone_name}}.release_intel;
-- Zone left in place by default — many demos may share this zone. Uncomment
-- if this demo runs in an isolated environment where the zone should go too.
-- DROP ZONE IF EXISTS {{zone_name}};
