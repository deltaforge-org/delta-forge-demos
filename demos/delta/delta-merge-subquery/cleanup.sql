-- ============================================================================
-- Delta MERGE — Subquery & CTE Source Patterns — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.order_events WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.daily_revenue WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
