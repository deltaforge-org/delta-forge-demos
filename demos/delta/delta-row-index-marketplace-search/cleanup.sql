-- Cleanup: Multi-Vendor Marketplace — Multiple Indexes on the Same Table

DROP INDEX IF EXISTS idx_sku            ON TABLE {{zone_name}}.delta_demos.marketplace_listings;
DROP INDEX IF EXISTS idx_brand          ON TABLE {{zone_name}}.delta_demos.marketplace_listings;
DROP INDEX IF EXISTS idx_category_price ON TABLE {{zone_name}}.delta_demos.marketplace_listings;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.marketplace_listings WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-row-index-marketplace-search' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
