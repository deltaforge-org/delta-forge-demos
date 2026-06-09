-- Cleanup: Library Catalog — Index Lifecycle, Staleness, and Rebuild

DROP INDEX IF EXISTS idx_isbn ON TABLE {{zone_name}}.delta_demos.library_catalog;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.library_catalog WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-row-index-catalog-rebuild' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
