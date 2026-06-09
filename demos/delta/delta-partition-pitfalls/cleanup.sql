-- ============================================================================
-- Delta Partition Pitfalls — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.events_by_month WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.events_by_customer WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-partition-pitfalls' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
