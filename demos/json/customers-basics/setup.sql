-- ============================================================================
-- JSON Customers Basics — Setup Script
-- ============================================================================
-- Creates one external table from a JSON array file containing 200 CRM
-- customer records.
--
-- Demonstrates:
--   - JSON array format (single file, multi-line)
--   - include_paths (selective field extraction)
--   - column_mappings (optional rename — here we use auto-detected names)
--   - infer_types (automatic type detection)
--   - file_metadata (df_file_name, df_row_number)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.json_demos
    COMMENT 'JSON-backed external tables';

-- ============================================================================
-- TABLE: customers — 200 CRM customer records
-- ============================================================================
-- A CRM system exported its customer database as a JSON array. Each element
-- is a flat object with id, email, first name, last name, company, signup
-- date, and country. Auto-detected column names follow delta-forge convention
-- (strip $., split camelCase, lowercase). Type inference handles automatic
-- timestamp detection.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json_demos.customers
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.id",
            "$.email",
            "$.first",
            "$.last",
            "$.company",
            "$.created_at",
            "$.country"
        ],
        "max_depth": 1,
        "separator": "_",
        "default_array_handling": "to_json",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
