-- ============================================================================
-- Delta MERGE — CDC Upsert with BY SOURCE — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.product_feed WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.upsert_products WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
