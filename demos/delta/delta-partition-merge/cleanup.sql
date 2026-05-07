-- ============================================================================
-- Delta Partition MERGE — Cleanup Script
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.partitioned_supplier_feed WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.partitioned_product_catalog WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
DROP ZONE IF EXISTS {{zone_name}};
