-- ============================================================================
-- Delta MERGE Subquery — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.mergesub_order_events WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.daily_revenue WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
