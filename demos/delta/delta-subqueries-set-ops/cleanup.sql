-- ============================================================================
-- University Course Enrollment Analytics — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.enrollments WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.students WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
