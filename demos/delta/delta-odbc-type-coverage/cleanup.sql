-- ============================================================================
-- Demo: Global Logistics Shipment Telemetry: Full Type Coverage for ODBC
-- Cleanup script: drops the table and shared schema/zone if no other demos
-- still need them.
-- ============================================================================

DROP VIEW IF EXISTS {{zone_name}}.bi_demos.v_shipments_full_types;

DROP DELTA TABLE IF EXISTS {{zone_name}}.bi_demos.shipments_full_types WITH FILES;

-- Shared resources (safe: warns if other demos still reference them)
DROP SCHEMA IF EXISTS {{zone_name}}.bi_demos;
DROP ZONE IF EXISTS {{zone_name}};
