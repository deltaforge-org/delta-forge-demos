-- ============================================================================
-- Protobuf Address Book Contacts — Cleanup Script
-- ============================================================================
-- DROP TABLE commands automatically clean up catalog metadata (columns, etc.).
-- ============================================================================

-- Drop external tables
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.protobuf_demos.contacts WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.protobuf_demos.contact_phones WITH FILES;

-- Shared resources (safe — will warn if other demos still use them)
DROP SCHEMA IF EXISTS {{zone_name}}.protobuf_demos;
DROP ZONE IF EXISTS {{zone_name}};
