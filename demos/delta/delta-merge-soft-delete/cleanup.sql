-- ============================================================================
-- Delta MERGE — Soft Delete with BY SOURCE — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.vendor_feed WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.vendors WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-merge-soft-delete' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
