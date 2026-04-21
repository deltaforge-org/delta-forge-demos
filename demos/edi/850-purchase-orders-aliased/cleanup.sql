-- ============================================================================
-- EDI 850 Purchase Orders — Aliased Columns + To-JSON Line Items
-- Cleanup Script
-- ============================================================================
-- Removes the bronze external table, schema, and zone created by setup.sql.
-- DROP EXTERNAL TABLE ... WITH FILES also clears any cached metadata.
-- The schema and zone are shared across demos; DROP SCHEMA / DROP ZONE will
-- succeed silently if they are empty.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.commerce.purchase_orders WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.commerce;

DROP ZONE IF EXISTS {{zone_name}};
