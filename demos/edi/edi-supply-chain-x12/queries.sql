-- ============================================================================
-- EDI Supply Chain X12 — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge unifies X12 EDI transactions from
-- multiple trading partners and transaction types into queryable tables.
--
-- Two tables are available:
--   supply_chain_messages      — Compact view: ISA/GS/ST headers + full JSON
--   supply_chain_materialized  — Enriched view: headers + key business fields
--
-- Column reference (always available — ISA envelope fields):
--   ISA_6  = Interchange Sender ID    ISA_8  = Interchange Receiver ID
--   ISA_9  = Interchange Date         ISA_12 = Interchange Control Version
--   ISA_13 = Interchange Control No.  ISA_15 = Usage Indicator (T=Test, P=Prod)
--
-- Column reference (always available — GS functional group fields):
--   GS_1   = Functional Identifier Code (PO, IN, FA)
--   GS_2   = Application Sender Code    GS_3   = Application Receiver Code
--   GS_4   = Group Date                 GS_8   = Version / Release Code
--
-- Column reference (always available — ST transaction set):
--   ST_1   = Transaction Set ID (850, 810, 856, etc.)
--   ST_2   = Transaction Set Control Number
--
-- Materialized columns (supply_chain_materialized table only):
--   BEG_1  = Purpose Code   BEG_3  = PO Number       BEG_5  = PO Date
--   BIG_1  = Invoice Date   BIG_2  = Invoice Number
--   BSN_2  = Shipment ID    BSN_3  = Shipment Date
--   N1_1   = Entity ID Code N1_2   = Party Name
--   CTT_1  = Total Line Items
-- ============================================================================


-- ============================================================================
-- 1. All Transactions — Header Overview
-- ============================================================================
-- This query reads from the compact table (supply_chain_messages) to show
-- the ISA/GS/ST header of every X12 transaction. Each row is one EDI
-- transaction set parsed from the 14 source files.
--
-- What you'll see:
--   - df_file_name:  The source .edi file this row came from
--   - sender_id:     The interchange sender (ISA-06): 000123456, SENDER1,
--                    ABCMUSICSUPPLY, or TO — identifies the trading partner
--   - receiver_id:   The interchange receiver (ISA-08): PARTNERID, TEST2,
--                    ARIBAEDI, RECEIVER1, or FROM
--   - txn_type:      X12 transaction set ID (ST-01): 850, 810, 855, etc.
--   - x12_version:   Version from GS-08 (004010 for all files)

ASSERT ROW_COUNT = 14
ASSERT VALUE sender_id = 'ABCMUSICSUPPLY' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE receiver_id = 'ARIBAEDI' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE txn_type = '850' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE txn_type = '997' WHERE df_file_name = 'x12_997_functional_acknowledgment.edi'
ASSERT VALUE x12_version = '004010' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE sender_id = 'TO' WHERE df_file_name = 'x12_997_functional_acknowledgment.edi'
SELECT
    df_file_name,
    isa_6 AS sender_id,
    isa_8 AS receiver_id,
    st_1 AS txn_type,
    gs_8 AS x12_version
FROM {{zone_name}}.edi_demos.supply_chain_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Transaction Type Distribution
-- ============================================================================
-- Groups transactions by their X12 transaction set ID (ST-01) to show the
-- mix of document types in this supply chain feed. Each transaction type
-- represents a different step in the order-to-cash cycle.
--
-- What you'll see:
--   - txn_type:       X12 transaction set identifier code
--   - txn_name:       Human-readable name for each transaction type:
--                     850=Purchase Order, 810=Invoice, 855=PO Ack,
--                     856=Ship Notice, 857=Ship/Bill Notice,
--                     861=Receiving Advice, 997=Functional Ack,
--                     824=Application Advice
--   - txn_count:      Number of transactions of each type
--
-- 8 distinct types — 850(3), 810(5), 855(1), 856(1), 857(1),
-- 861(1), 997(1), 824(1)

ASSERT ROW_COUNT = 8
ASSERT VALUE txn_count = 5 WHERE txn_type = '810'
ASSERT VALUE txn_count = 3 WHERE txn_type = '850'
ASSERT VALUE txn_count = 1 WHERE txn_type = '997'
ASSERT VALUE txn_count = 1 WHERE txn_type = '824'
SELECT
    st_1 AS txn_type,
    CASE st_1
        WHEN '850' THEN 'Purchase Order'
        WHEN '810' THEN 'Invoice'
        WHEN '855' THEN 'PO Acknowledgment'
        WHEN '856' THEN 'Ship Notice'
        WHEN '857' THEN 'Shipment & Billing Notice'
        WHEN '861' THEN 'Receiving Advice'
        WHEN '997' THEN 'Functional Acknowledgment'
        WHEN '824' THEN 'Application Advice'
        ELSE 'Other'
    END AS txn_name,
    COUNT(*) AS txn_count
FROM {{zone_name}}.edi_demos.supply_chain_messages
GROUP BY st_1
ORDER BY txn_count DESC, st_1;


-- ============================================================================
-- 3. Order Details — Materialized View
-- ============================================================================
-- This query reads from the materialized table to show purchase order
-- details extracted from BEG segments. The BEG segment only appears in
-- 850 (Purchase Order) transactions, so BEG_3 will be NULL for all other
-- transaction types.
--
-- What you'll see:
--   - df_file_name:  Source file name
--   - txn_type:      Transaction set ID (should be 850 for all rows)
--   - po_number:     Purchase order number from BEG-03 (e.g. "1000012",
--                    "4600000406", "XX-1234")
--   - po_date:       Purchase order date from BEG-05 (YYYYMMDD format)
--   - party_name:    First trading partner name from N1-02 (e.g. "John Doe",
--                    "Transplace Laredo", "ABC AEROSPACE")
--   - line_items:    Number of line items from CTT-01

ASSERT ROW_COUNT = 3
ASSERT VALUE po_number = '1000012' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po_date = '20090827' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_name = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE line_items = '1' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po_number = '4600000406' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE party_name = 'Transplace Laredo' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po_number = 'XX-1234' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE party_name = 'ABC AEROSPACE' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    beg_3 AS po_number,
    beg_5 AS po_date,
    n1_2 AS party_name,
    ctt_1 AS line_items
FROM {{zone_name}}.edi_demos.supply_chain_materialized
WHERE beg_3 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 4. Invoice Details — Materialized View
-- ============================================================================
-- Extracts invoice information from BIG segments in 810 (Invoice)
-- transactions. The BIG segment contains the invoice date and number.
-- Only 810 transactions have BIG segments populated.
--
-- What you'll see:
--   - df_file_name:   Source file name
--   - invoice_date:   Invoice date from BIG-01 (YYYYMMDD format, e.g. "20030310",
--                     "20000513")
--   - invoice_number: Invoice identifier from BIG-02 (e.g. "DO091003TESTINV01",
--                     "SG427254")
--   - party_name:     First trading partner from N1-02 (e.g. "Aaron Copeland",
--                     "ABC AEROSPACE CORPORATION")
--   - line_items:     Number of line items from CTT-01

ASSERT ROW_COUNT = 5
ASSERT VALUE invoice_date = '20030310' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE invoice_number = 'DO091003TESTINV01' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE party_name = 'Aaron Copeland' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE line_items = '2' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE invoice_number = 'SG427254' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE invoice_date = '20000513' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE party_name = 'ABC AEROSPACE CORPORATION' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
SELECT
    df_file_name,
    big_1 AS invoice_date,
    big_2 AS invoice_number,
    n1_2 AS party_name,
    ctt_1 AS line_items
FROM {{zone_name}}.edi_demos.supply_chain_materialized
WHERE big_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 5. Trading Partner Analysis
-- ============================================================================
-- Groups transactions by their ISA sender/receiver pairs to identify all
-- trading partner relationships in the feed. ISA-06 is the sender and
-- ISA-08 is the receiver of the interchange envelope.
--
-- What you'll see:
--   - sender_id:      Interchange sender from ISA-06 (padded to 15 chars)
--   - receiver_id:    Interchange receiver from ISA-08 (padded to 15 chars)
--   - txn_count:      Number of transactions between this pair
--
-- 5 distinct sender/receiver pairs:
--   000123456 -> PARTNERID (1 txn: 850)
--   000123456 -> TEST2 (1 txn: 850)
--   ABCMUSICSUPPLY -> ARIBAEDI (4 txns: 810s)
--   SENDER1 -> RECEIVER1 (7 txns: mixed)
--   TO -> FROM (1 txn: 997)

ASSERT ROW_COUNT = 5
ASSERT VALUE txn_count = 7 WHERE sender_id = 'SENDER1'
ASSERT VALUE txn_count = 4 WHERE sender_id = 'ABCMUSICSUPPLY'
ASSERT VALUE txn_count = 1 WHERE sender_id = 'TO'
SELECT
    isa_6 AS sender_id,
    isa_8 AS receiver_id,
    COUNT(*) AS txn_count
FROM {{zone_name}}.edi_demos.supply_chain_messages
GROUP BY isa_6, isa_8
ORDER BY txn_count DESC;


-- ============================================================================
-- 6. X12 Version Distribution
-- ============================================================================
-- Shows how many transactions came from each X12 interchange control
-- version. ISA-12 identifies the version of the ISA envelope standard.
-- This demonstrates DeltaForge parsing multiple X12 versions in a
-- single table without any version-specific configuration.
--
-- What you'll see:
--   - isa_version:    The interchange control version from ISA-12
--   - txn_count:      How many transactions use that version
--
-- 2 distinct versions:
--   00401 — X12 version 4010 (7 transactions: the PO/invoice files)
--   00204 — X12 version 2040 (7 transactions: the SENDER1 files)

ASSERT ROW_COUNT = 2
ASSERT VALUE txn_count = 7 WHERE isa_version = '00204'
ASSERT VALUE txn_count = 7 WHERE isa_version = '00401'
SELECT
    isa_12 AS isa_version,
    COUNT(*) AS txn_count
FROM {{zone_name}}.edi_demos.supply_chain_messages
GROUP BY isa_12
ORDER BY isa_12;


-- ============================================================================
-- 7. Functional Group Codes
-- ============================================================================
-- Groups transactions by their GS-01 functional identifier code. This
-- code classifies the business purpose of each functional group:
--   PO = Purchase Order (two 850 files)
--   IN = Invoice and general (810s, 850_edifabric, 855, 856, 857, 861, 824)
--   FA = Functional Acknowledgment (997)
--
-- What you'll see:
--   - group_code:     GS-01 functional identifier (PO, IN, or FA)
--   - group_name:     Human-readable group description
--   - txn_count:      Number of transactions in each group

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 11 WHERE group_code = 'IN'
ASSERT VALUE txn_count = 1 WHERE group_code = 'FA'
ASSERT VALUE txn_count = 2 WHERE group_code = 'PO'
SELECT
    gs_1 AS group_code,
    CASE gs_1
        WHEN 'PO' THEN 'Purchase Order'
        WHEN 'IN' THEN 'Invoice / General'
        WHEN 'FA' THEN 'Functional Acknowledgment'
        ELSE gs_1
    END AS group_name,
    COUNT(*) AS txn_count
FROM {{zone_name}}.edi_demos.supply_chain_messages
GROUP BY gs_1
ORDER BY txn_count DESC;


-- ============================================================================
-- 8. Full Transaction JSON — Deep Access via df_transaction_json
-- ============================================================================
-- Every row includes df_transaction_json containing the complete parsed
-- X12 transaction as a JSON object. This enables access to ANY segment
-- and element — including segments not materialized (PO1 line items,
-- IT1 invoice items, HL hierarchical levels, SAC charges, TXI tax info).
--
-- What you'll see:
--   - df_file_name:         Source file name
--   - txn_type:             Transaction set ID from ST-01
--   - df_transaction_json:  Full transaction as JSON (click to expand in UI)
--
-- Tip: The JSON contains every segment with its elements as arrays. Use
-- JSON functions for deep access without needing materialized_paths.

ASSERT ROW_COUNT = 3
ASSERT VALUE df_transaction_json IS NOT NULL WHERE df_file_name = 'x12_810_invoice_a.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    df_transaction_json
FROM {{zone_name}}.edi_demos.supply_chain_messages
ORDER BY df_file_name
LIMIT 3;


-- ============================================================================
-- 9. SUMMARY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly.
-- All checks should return PASS.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'beg_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'materialized_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'source_files_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'transaction_count_14'
ASSERT VALUE result = 'PASS' WHERE check_name = 'transaction_types'
SELECT check_name, result FROM (

    -- Check 1: Total transaction count = 14 (one per .edi file)
    SELECT 'transaction_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.supply_chain_messages) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 14 distinct source files in df_file_name
    SELECT 'source_files_14' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi_demos.supply_chain_messages) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: At least 5 distinct transaction types (actual: 8)
    SELECT 'transaction_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT st_1) FROM {{zone_name}}.edi_demos.supply_chain_messages) >= 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Materialized table also has 14 rows (same files, different columns)
    SELECT 'materialized_count_14' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.supply_chain_materialized) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: BEG_3 (PO number) is populated for at least some rows (the 850s)
    SELECT 'beg_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.supply_chain_materialized
                       WHERE beg_3 IS NOT NULL AND beg_3 <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: df_transaction_json is populated for all 14 transactions
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.supply_chain_messages
                       WHERE df_transaction_json IS NOT NULL) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
