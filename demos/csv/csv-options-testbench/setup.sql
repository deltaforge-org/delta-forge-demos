-- ============================================================================
-- CSV Advanced Options Testbench — Setup Script
-- ============================================================================
-- Each table exercises a specific CSV option with data designed so that
-- incorrect parsing produces obviously wrong results.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.csv_demos
    COMMENT 'CSV-backed external tables';

-- ============================================================================
-- TABLE 1: opt_delimiter — Tests delimiter='|'
-- ============================================================================
-- Data uses pipe as separator. If delimiter is not wired, the entire
-- line becomes a single column instead of 4 separate columns.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.opt_delimiter
USING CSV
LOCATION 'csv-options-testbench/01_pipe_delimited.csv'
OPTIONS (
    has_header = 'true',
    delimiter = '|'
);

-- ============================================================================
-- TABLE 2: opt_null_value — Tests null_value='N/A'
-- ============================================================================
-- Rows 2 and 4 have "N/A" in the score column. If null_value is wired,
-- those become SQL NULL. If not, they remain the literal string "N/A".
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.opt_null_value
USING CSV
LOCATION 'csv-options-testbench/02_null_markers.csv'
OPTIONS (
    has_header = 'true',
    null_value = 'N/A'
);

-- ============================================================================
-- TABLE 3: opt_comment — Tests comment_char='#'
-- ============================================================================
-- File has 5 comment lines starting with #. If comment_char is wired,
-- only 3 data rows are returned. If not, the parser errors or returns
-- garbage rows from the comment lines.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.opt_comment
USING CSV
LOCATION 'csv-options-testbench/03_comment_lines.csv'
OPTIONS (
    has_header = 'true',
    comment_char = '#'
);

-- ============================================================================
-- TABLE 4: opt_skip_rows — Tests skip_starting_rows=3
-- ============================================================================
-- First 3 lines are report metadata (not CSV). If skip_starting_rows
-- is wired, line 4 becomes the header. If not, "Report: Quarterly..."
-- becomes the first column name.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.opt_skip_rows
USING CSV
LOCATION 'csv-options-testbench/04_skip_metadata.csv'
OPTIONS (
    has_header = 'true',
    skip_starting_rows = '3'
);

-- ============================================================================
-- TABLE 5: opt_max_rows — Tests max_rows=5
-- ============================================================================
-- File has 10 data rows. If max_rows is wired, only 5 rows are returned.
-- If not, all 10 rows appear.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.opt_max_rows
USING CSV
LOCATION 'csv-options-testbench/05_max_rows.csv'
OPTIONS (
    has_header = 'true',
    max_rows = '5'
);

-- ============================================================================
-- TABLE 6: opt_trim — Tests trim_whitespace='true'
-- ============================================================================
-- Values have leading/trailing spaces. If trim_whitespace is wired,
-- name='Alice' (length 5). If not, name='  Alice  ' (length 9).
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.opt_trim
USING CSV
LOCATION 'csv-options-testbench/06_whitespace.csv'
OPTIONS (
    has_header = 'true',
    trim_whitespace = 'true'
);

-- ============================================================================
-- TABLE 7: opt_quoted — Tests delimiter=';' with quoted fields
-- ============================================================================
-- Semicolon delimiter with descriptions containing literal semicolons
-- inside quotes. If quoting is not handled, columns split incorrectly.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.opt_quoted
USING CSV
LOCATION 'csv-options-testbench/07_semicolon_quoted.csv'
OPTIONS (
    has_header = 'true',
    delimiter = ';',
    quote = '"'
);

-- ============================================================================
-- TABLE 8: opt_combined — Tests multiple options together
-- ============================================================================
-- Pipe delimiter + comment lines + null markers + whitespace trimming.
-- All options must work simultaneously for correct results.
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.csv_demos.opt_combined
USING CSV
LOCATION 'csv-options-testbench/08_combined.csv'
OPTIONS (
    has_header = 'true',
    delimiter = '|',
    comment_char = '#',
    null_value = 'N/A',
    trim_whitespace = 'true'
);
