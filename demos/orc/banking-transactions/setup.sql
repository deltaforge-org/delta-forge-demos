-- ============================================================================
-- ORC Banking Transactions — Setup Script
-- ============================================================================
-- Creates two external tables from 2 branch transaction ORC files:
--   1. all_transactions  — Both branches (100 rows)
--   2. downtown_only     — Single branch file (50 rows)
--
-- Demonstrates:
--   - Multi-file reading: 2 ORC files in one table
--   - ORC self-describing schema with automatic type detection
--   - file_metadata: df_file_name + df_row_number system columns
--   - Single-file LOCATION for branch-level isolation
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.orc_bank
    COMMENT 'ORC-backed banking transaction tables';

-- ============================================================================
-- TABLE 1: all_transactions — Both branch files (100 rows)
-- ============================================================================
-- Reads all ORC files from the data directory. Combines downtown and suburban
-- branch transactions with file_metadata for traceability.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc_bank.all_transactions
USING ORC
LOCATION '{{data_path}}'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: downtown_only — Single branch file (50 rows)
-- ============================================================================
-- Reads only the downtown branch ORC file for branch-level analysis.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc_bank.downtown_only
USING ORC
LOCATION 'banking-transactions/branch_downtown.orc'
OPTIONS (
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
