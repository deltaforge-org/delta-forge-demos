-- ============================================================================
-- Excel Multi-Sheet Reporting — Setup Script
-- ============================================================================
-- Creates three external tables from 2 regional XLSX files (East + West).
-- Each table targets a different sheet within the same workbooks:
--   1. all_sales   — sheet "Sales"   (17 + 16 = 33 rows)
--   2. all_returns  — sheet "Returns" (4 + 3 = 7 rows)
--   3. all_staff    — sheet "Staff"   (4 + 3 = 7 rows)
--
-- Demonstrates:
--   - Multi-file reading: 2 XLSX files from one directory
--   - sheet_name: select different sheets from the same workbooks
--   - file_metadata: df_file_name for region identification
--   - Cross-sheet analysis via separate tables per sheet
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.excel_demos
    COMMENT 'Excel-backed external tables';

-- ============================================================================
-- TABLE 1: all_sales — Sales sheet from both regions (33 rows)
-- ============================================================================
-- Reads the "Sales" sheet from both regional workbooks. Enables file metadata
-- so queries can identify which region each row came from.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_demos.all_sales
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Sales',
    has_header = 'true',
    infer_schema_rows = '100',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

-- ============================================================================
-- TABLE 2: all_returns — Returns sheet from both regions (7 rows)
-- ============================================================================
-- Reads the "Returns" sheet from both regional workbooks. Each return
-- references an order_id from the Sales sheet, enabling cross-sheet JOINs.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_demos.all_returns
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Returns',
    has_header = 'true',
    infer_schema_rows = '100',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

-- ============================================================================
-- TABLE 3: all_staff — Staff sheet from both regions (7 rows)
-- ============================================================================
-- Reads the "Staff" sheet from both regional workbooks. Contains employee
-- roster with role and hire date for headcount analysis.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.excel_demos.all_staff
USING EXCEL
LOCATION '{{data_path}}'
OPTIONS (
    sheet_name = 'Staff',
    has_header = 'true',
    infer_schema_rows = '100',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
