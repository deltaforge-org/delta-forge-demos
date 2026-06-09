-- ============================================================================
-- CLEANUP: Hospital Shift Handover — Timestamp NTZ Demo
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.shift_handover WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-timestamp-ntz' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;

DROP ZONE IF EXISTS {{zone_name}};
