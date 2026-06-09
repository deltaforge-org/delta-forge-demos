-- ============================================================================
-- XML Subtree Capture — Cleanup Script
-- ============================================================================
-- Drops both external tables, then the xml schema, then the zone.
-- Note: {{zone_name}}.xml schema and {{zone_name}} zone may be shared with
-- other XML demos — drop order is correct (tables → schema → zone) and all
-- commands use IF EXISTS so shared-resource conflicts are safe at runtime.
-- ============================================================================

-- STEP 1: Drop tables (WITH FILES removes the underlying data files too)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml_demos.products_json WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.xml_demos.products_xml WITH FILES;

-- STEP 2: Drop schema
DROP SCHEMA IF EXISTS {{zone_name}}.xml_demos;

-- STEP 3: Drop zone
DROP ZONE IF EXISTS {{zone_name}};
