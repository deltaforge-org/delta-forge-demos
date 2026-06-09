-- ============================================================================
-- Delta MERGE Multi-Source — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.return_updates WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.payment_updates WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.shipping_updates WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.order_status WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-merge-multi-source' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
