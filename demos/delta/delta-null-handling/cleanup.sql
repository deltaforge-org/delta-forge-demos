-- ============================================================================
-- Customer Survey Data Cleansing — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.survey_responses WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-null-handling' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
