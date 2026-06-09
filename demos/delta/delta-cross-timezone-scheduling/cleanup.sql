-- ============================================================================
-- CLEANUP: Delta Cross-Timezone Scheduling — Global Conference Planner
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.conference_schedule WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-cross-timezone-scheduling' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;

DROP ZONE IF EXISTS {{zone_name}};
