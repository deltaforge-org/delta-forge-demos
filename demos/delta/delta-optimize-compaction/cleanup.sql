-- ============================================================================
-- Delta OPTIMIZE — Manual File Compaction & TARGET SIZE — Cleanup
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.daily_orders WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-optimize-compaction' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
