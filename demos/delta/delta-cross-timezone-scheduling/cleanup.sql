-- ============================================================================
-- CLEANUP: Delta Cross-Timezone Scheduling — Global Conference Planner
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.conference_schedule WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;

DROP ZONE IF EXISTS {{zone_name}};
