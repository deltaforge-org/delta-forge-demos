-- ============================================================================
-- Delta Partition Pitfalls — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.events_by_month WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.events_by_customer WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
