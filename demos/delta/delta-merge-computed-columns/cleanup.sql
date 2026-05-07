-- ============================================================================
-- Delta MERGE — Computed Columns & CASE Logic — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.subscription_changes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.computed_subscriptions WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
