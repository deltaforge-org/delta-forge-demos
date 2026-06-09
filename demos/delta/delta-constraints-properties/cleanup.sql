-- ============================================================================
-- Delta Constraints & Table Properties — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.event_log WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.invoices WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-constraints-properties' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
