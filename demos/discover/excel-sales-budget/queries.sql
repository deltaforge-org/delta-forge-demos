-- ============================================================================
-- Demo: Regional Sales-Budget Auto-Onboarding, Queries
-- ============================================================================
-- The landing folder holds one Excel workbook:
--   sales_budget_2026.xlsx: a single sheet, a header row then 24 budget rows
-- DISCOVER detects Excel from the OOXML/ZIP magic header (PK\x03\x04, not the
-- .xlsx extension), reads the first sheet's header row to name the columns,
-- and registers ONE external table. Every assertion value below was
-- precomputed from the workbook rows, not the engine.
-- ============================================================================

-- ============================================================================
-- Query 1: DISCOVER in PRINT mode, review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the landed file,
-- matches the OOXML/ZIP magic number (PK\x03\x04) to the Excel handler, opens
-- the first sheet, and treats the first row as the header. FILE_METADATA adds
-- the df_file_name / df_row_number provenance columns to the generated table.

DISCOVER {{zone_name}}.discover_demos.sales_budget
    PATH 'excel-sales-budget'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode, auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). The
-- evidence column records the magic-number verdict that routed the file to the
-- Excel handler. This single statement replaces a hand-written CREATE EXTERNAL
-- TABLE USING EXCEL with sheet_name and has_header options.

DISCOVER {{zone_name}}.discover_demos.sales_budget
    PATH 'excel-sales-budget'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: The budget rows, all 24 rows from the first sheet
-- ============================================================================
-- One table over the single workbook sheet. The discovered columns are the
-- sanitized header names: region, product_line, quarter, budget_amount,
-- actual_amount, variance_pct. df_file_name confirms the single source file.

ASSERT ROW_COUNT = 24
ASSERT VALUE distinct_source_files = 1
SELECT
    region,
    product_line,
    quarter,
    budget_amount,
    actual_amount,
    COUNT(DISTINCT df_file_name) OVER () AS distinct_source_files
FROM {{zone_name}}.discover_demos.sales_budget;

-- ============================================================================
-- Query 4: Coverage by region, exact row counts per region
-- ============================================================================
-- Proves the region column decoded correctly off the header row. Counts are
-- exact and precomputed from the workbook.

ASSERT ROW_COUNT = 4
ASSERT VALUE budget_rows = 6 WHERE region = 'north'
ASSERT VALUE budget_rows = 6 WHERE region = 'south'
ASSERT VALUE budget_rows = 6 WHERE region = 'east'
ASSERT VALUE budget_rows = 6 WHERE region = 'west'
SELECT
    region,
    COUNT(*) AS budget_rows
FROM {{zone_name}}.discover_demos.sales_budget
GROUP BY region
ORDER BY region;

-- ============================================================================
-- Query 5: Coverage by product line, exact row counts per line
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE budget_rows = 8 WHERE product_line = 'hardware'
ASSERT VALUE budget_rows = 8 WHERE product_line = 'software'
ASSERT VALUE budget_rows = 8 WHERE product_line = 'services'
SELECT
    product_line,
    COUNT(*) AS budget_rows
FROM {{zone_name}}.discover_demos.sales_budget
GROUP BY product_line
ORDER BY product_line;

-- ============================================================================
-- Query 6: Budget attainment, rows that hit or beat their budget
-- ============================================================================
-- An integer aggregate over the inferred numeric columns: how many of the 24
-- budget rows had actual_amount at or above budget_amount. Exact count,
-- precomputed in Python from the workbook values.

ASSERT ROW_COUNT = 1
ASSERT VALUE rows_at_or_over_budget = 13
SELECT
    COUNT(*) FILTER (WHERE actual_amount >= budget_amount)
        AS rows_at_or_over_budget
FROM {{zone_name}}.discover_demos.sales_budget;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total budget-row volume, the
-- distinct dimension cardinalities (4 regions, 3 product lines, 4 quarters),
-- and the single source workbook.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 24
ASSERT VALUE distinct_regions = 4
ASSERT VALUE distinct_product_lines = 3
ASSERT VALUE distinct_quarters = 4
ASSERT VALUE distinct_source_files = 1
SELECT
    COUNT(*)                          AS total_rows,
    COUNT(DISTINCT region)            AS distinct_regions,
    COUNT(DISTINCT product_line)      AS distinct_product_lines,
    COUNT(DISTINCT quarter)           AS distinct_quarters,
    COUNT(DISTINCT df_file_name)      AS distinct_source_files
FROM {{zone_name}}.discover_demos.sales_budget;
