-- ============================================================================
-- Delta MERGE — Computed Columns & CASE Logic — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.subscription_changes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.computed_subscriptions WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-merge-computed-columns' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
