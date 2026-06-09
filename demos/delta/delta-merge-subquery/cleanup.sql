-- ============================================================================
-- Delta MERGE Subquery — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.mergesub_order_events WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.daily_revenue WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-merge-subquery' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
