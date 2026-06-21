-- ============================================================================
-- Demo: EDI X12 Auto-Onboarding : Queries
-- ============================================================================
-- The onboarding folder holds 6 X12 EDI documents from RIVERBEND
-- DISTRIBUTION's trading partners, one transaction set per file:
--   3 purchase orders (transaction set 850, outbound to suppliers)
--   3 invoices        (transaction set 810, inbound from suppliers)
-- DISCOVER registers ONE external table over all six files. It reads the
-- bytes, recognises the ISA/GS/ST envelope, classifies the feed as EDI, and
-- auto-detects the X12 dialect. Every assertion value below was precomputed
-- from the data files in Python, not from the engine.
--
-- Column reference (X12 default output):
--   st_1                = Transaction Set ID code ('850' = PO, '810' = Invoice)
--   gs_1                = Functional Identifier Code ('PO' or 'IN')
--   df_transaction_json = Full parsed transaction as a JSON object
--   df_file_name        = Source .edi file (from FILE_METADATA = true)
-- ============================================================================

-- ============================================================================
-- Query 1: DISCOVER in PRINT mode : review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the landed .edi
-- files, recognises the ISA/GS/ST interchange envelope, classifies the feed
-- as EDI (USING EDI), and auto-detects the X12 dialect with no edi_config.
-- FILE_METADATA adds the df_file_name / df_row_number provenance columns to
-- the generated table.

DISCOVER {{zone_name}}.discover_demos.edi_documents
    PATH 'edi-x12-onboarding'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode : auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). The
-- evidence column records the EDI envelope detection across the six .edi
-- files. This single statement replaces a hand-written CREATE EXTERNAL TABLE
-- USING EDI + edi_config block.

DISCOVER {{zone_name}}.discover_demos.edi_documents
    PATH 'edi-x12-onboarding'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: The unified feed : all 6 transactions, one row per file
-- ============================================================================
-- One table spans every .edi document in the folder. st_1 is the X12
-- transaction set code that DISCOVER exposes from the ST segment, proving the
-- EDI envelope parsed correctly under auto-detection.

ASSERT ROW_COUNT = 6
ASSERT VALUE txn_type = '850' WHERE df_file_name = 'po_acme_85001.edi'
ASSERT VALUE txn_type = '810' WHERE df_file_name = 'inv_acme_81001.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    gs_1 AS group_code
FROM {{zone_name}}.discover_demos.edi_documents
ORDER BY df_file_name;

-- ============================================================================
-- Query 4: Transaction type distribution : purchase orders vs invoices
-- ============================================================================
-- Groups the feed by its X12 transaction set ID (ST-01). The onboarding
-- folder carries exactly two document types: 3 purchase orders (850) and
-- 3 invoices (810). Counts are exact and precomputed from the data.

ASSERT ROW_COUNT = 2
ASSERT VALUE txn_count = 3 WHERE txn_type = '850'
ASSERT VALUE txn_count = 3 WHERE txn_type = '810'
SELECT
    st_1 AS txn_type,
    CASE st_1
        WHEN '850' THEN 'Purchase Order'
        WHEN '810' THEN 'Invoice'
        ELSE 'Other'
    END AS txn_name,
    COUNT(*) AS txn_count
FROM {{zone_name}}.discover_demos.edi_documents
GROUP BY st_1
ORDER BY st_1;

-- ============================================================================
-- Query 5: Functional group distribution : PO vs IN envelopes
-- ============================================================================
-- The GS-01 functional identifier code classifies each functional group:
-- 'PO' wraps the purchase orders and 'IN' wraps the invoices. This mirrors
-- the st_1 split and confirms the GS envelope parsed alongside the ST.

ASSERT ROW_COUNT = 2
ASSERT VALUE txn_count = 3 WHERE group_code = 'PO'
ASSERT VALUE txn_count = 3 WHERE group_code = 'IN'
SELECT
    gs_1 AS group_code,
    CASE gs_1
        WHEN 'PO' THEN 'Purchase Order'
        WHEN 'IN' THEN 'Invoice / General'
        ELSE gs_1
    END AS group_name,
    COUNT(*) AS txn_count
FROM {{zone_name}}.discover_demos.edi_documents
GROUP BY gs_1
ORDER BY gs_1;

-- ============================================================================
-- Query 6: Provenance : one source file per transaction
-- ============================================================================
-- df_file_name comes from the FILE_METADATA = true discovery option. Each of
-- the six .edi files contributes exactly one transaction row, so the count of
-- distinct source files equals the row count.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_files = 6
ASSERT VALUE total_rows = 6
SELECT
    COUNT(DISTINCT df_file_name) AS distinct_files,
    COUNT(*)                     AS total_rows
FROM {{zone_name}}.discover_demos.edi_documents;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total transaction volume, the
-- distinct source-file count, the count of distinct transaction set codes
-- (exactly two: 850 and 810), and that the full message JSON is populated for
-- every row.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_transactions = 6
ASSERT VALUE distinct_source_files = 6
ASSERT VALUE distinct_txn_types = 2
ASSERT VALUE json_populated = 6
SELECT
    COUNT(*)                            AS total_transactions,
    COUNT(DISTINCT df_file_name)        AS distinct_source_files,
    COUNT(DISTINCT st_1)                AS distinct_txn_types,
    COUNT(*) FILTER (WHERE df_transaction_json IS NOT NULL) AS json_populated
FROM {{zone_name}}.discover_demos.edi_documents;
