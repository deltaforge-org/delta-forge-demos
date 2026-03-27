-- ============================================================================
-- Iceberg Native Schema Evolution — Setup
-- ============================================================================
-- Creates an external table backed by a native Apache Iceberg v2 table
-- that has undergone schema evolution across 4 snapshots:
--
--   Snapshot 1: Initial schema (emp_id, full_name, dept, salary, hire_date)
--               300 employees, 60 per department
--   Snapshot 2: ADD COLUMN title + INSERT 60 more employees with titles
--   Snapshot 3: ADD COLUMN location + UPDATE 60 employees with locations
--   Snapshot 4: RENAME COLUMN dept → department (field-id stable)
--
-- Iceberg uses field-id stability for schema evolution: renaming a column
-- updates the metadata but the underlying Parquet files retain the original
-- column name. Delta Forge must resolve the current schema from the latest
-- metadata.json and map columns by field-id, not by name.
--
-- Dataset: 360 employees across 5 departments (Engineering, Sales,
-- Marketing, Finance, HR), 72 per department.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg
    COMMENT 'Apache Iceberg native table demos';

-- STEP 2: Register the Iceberg v2 table with evolved schema
-- The LOCATION points to the Iceberg table root (containing metadata/ and data/).
-- Delta Forge parses the latest metadata.json (v7) which contains the final
-- schema with 7 columns including the renamed 'department' column.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg.employee_directory
USING ICEBERG
LOCATION '{{data_path}}/employee_directory';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg.employee_directory TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg.employee_directory;
