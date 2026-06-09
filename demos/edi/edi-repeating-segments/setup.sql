-- ============================================================================
-- EDI Repeating Segments — Setup Script
-- ============================================================================
-- Creates three external tables over the same 14 X12 EDI files, each using
-- a different repeating-segment strategy:
--
--   1. repeating_indexed  — Indexed mode: each occurrence gets its own columns
--                           (n1_1_1, n1_1_2, n1_2_1, n1_2_2, ... n1_6_1, n1_6_2)
--   2. repeating_concat   — Concatenate mode: all occurrences pipe-delimited
--                           (n1_2 = 'Aaron Copeland|XYZ Bank|Philadelphia|...')
--   3. repeating_json     — ToJson mode: all occurrences as JSON arrays
--                           (n1_2 = '["Aaron Copeland","XYZ Bank",...]')
--
-- All three tables materialize N1 (party) and PO1 (line item) segments with
-- max_repeating_segments=6 to handle files with up to 6 occurrences.
--
-- Materialized paths: n1_1, n1_2, po1_1, po1_2, po1_3, po1_4
--   N1_1 = Entity Identifier Code (SO, RI, SF, ST, BY, etc.)
--   N1_2 = Party Name
--   PO1_1 = Line Item Number
--   PO1_2 = Quantity Ordered
--   PO1_3 = Unit of Measure (EA, YD)
--   PO1_4 = Unit Price
--
-- Variables (auto-injected by DeltaForge):
--   data_path     — Local or cloud path where demo data files were downloaded
--   current_user  — Username of the current logged-in user
--   zone_name     — Target zone name (defaults to 'external')
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.edi_demos
    COMMENT 'EDI transaction-backed external tables';


-- ============================================================================
-- TABLE 1: repeating_indexed — Indexed mode (14 transactions)
-- ============================================================================
-- Each occurrence of a repeating segment gets its own set of columns:
--   n1_1_1 = 1st N1, element 1 (entity code)
--   n1_1_2 = 1st N1, element 2 (party name)
--   n1_2_1 = 2nd N1, element 1
--   n1_2_2 = 2nd N1, element 2
--   ... up to n1_6_1, n1_6_2
--
-- Same pattern for PO1: po1_1_1 through po1_6_4
--
-- Column naming: {segment}_{occurrence}_{element} (all 1-based)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.repeating_indexed
USING EDI
LOCATION 'edi-repeating-segments/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "n1_1", "n1_2",
            "po1_1", "po1_2", "po1_3", "po1_4"
        ],
        "max_repeating_segments": 6,
        "repeating_segment_mode": "indexed"
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


-- ============================================================================
-- TABLE 2: repeating_concat — Concatenate mode (14 transactions)
-- ============================================================================
-- All occurrences of a repeating segment are pipe-delimited into the default
-- column names (n1_1, n1_2, po1_1, po1_2, po1_3, po1_4):
--   n1_2 = 'Aaron Copeland|XYZ Bank|Philadelphia|Music Insurance Co. - San Fran|...'
--   po1_1 = '000100001|000200001|000200002'
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.repeating_concat
USING EDI
LOCATION 'edi-repeating-segments/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "n1_1", "n1_2",
            "po1_1", "po1_2", "po1_3", "po1_4"
        ],
        "max_repeating_segments": 6,
        "repeating_segment_mode": "concatenate"
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


-- ============================================================================
-- TABLE 3: repeating_json — ToJson mode (14 transactions)
-- ============================================================================
-- All occurrences of a repeating segment are encoded as JSON arrays in the
-- default column names:
--   n1_2 = '["Aaron Copeland","XYZ Bank","Philadelphia",...]'
--   po1_4 = '["2.53","3.41","3.41"]'
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.edi_demos.repeating_json
USING EDI
LOCATION 'edi-repeating-segments/*.edi'
OPTIONS (
    edi_config = '{
        "ediFormat": "x12",
        "materialized_paths": [
            "n1_1", "n1_2",
            "po1_1", "po1_2", "po1_3", "po1_4"
        ],
        "max_repeating_segments": 6,
        "repeating_segment_mode": "to_json"
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


