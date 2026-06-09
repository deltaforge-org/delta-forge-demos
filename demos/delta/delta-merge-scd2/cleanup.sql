-- ============================================================================
-- Delta MERGE SCD2 — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.policy_changes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.policy_dim WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-merge-scd2' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
