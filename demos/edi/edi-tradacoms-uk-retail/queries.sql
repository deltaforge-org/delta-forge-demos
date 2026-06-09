-- ============================================================================
-- EDI TRADACOMS UK Retail — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge parses TRADACOMS — the UK retail EDI
-- standard — into queryable tables. Four files cover purchase orders,
-- product planning forecasts, and utility bills (including escape handling).
--
-- Two tables are available:
--   tradacoms_messages      — Compact view: STX/MHD header fields + full JSON
--   tradacoms_materialized  — Enriched view: STX/MHD headers + TYP/SDT/CDT columns
--
-- IMPORTANT: Each TRADACOMS file contains multiple MHD (message header)
-- segments. The executor produces one row per MHD message, so:
--   tradacoms_order.edi              → 4 rows (ORDHDR, ORDERS x2, ORDTLR)
--   tradacoms_product_planning.edi   → 3 rows (PPRHDR, PPRDET, PPRTLR)
--   tradacoms_utility_bill.edi       → 4 rows (UTLHDR, UTLBIL, UVATLR, UTLTLR)
--   tradacoms_utility_bill_escape.edi→ 4 rows (same as above, with escapes)
--   Total: 15 rows across 4 files
--
-- Column reference (always available — STX envelope fields):
--   STX_1  = Syntax identifier (e.g. "ANA:1")
--   STX_2  = Sender identification
--   STX_3  = Recipient identification
--   STX_4  = Transmission date/time
--   STX_5  = Transmission reference
--   MHD_1  = Message reference number
--   MHD_2  = Message type:version (e.g. "ORDERS:9")
--
-- Materialized columns (tradacoms_materialized table only):
--   TYP_1  = Transaction type code     TYP_2  = Sub-type/version
--   SDT_1  = Supplier code             SDT_2  = Supplier name
--   CDT_1  = Customer code             CDT_2  = Customer name
-- ============================================================================


-- ============================================================================
-- 1. All Messages — Header Overview
-- ============================================================================
-- This query reads from the compact table (tradacoms_messages) to show the
-- STX envelope and MHD type of every message. Each row is one MHD segment
-- from within a TRADACOMS file.
--
-- What you'll see:
--   - df_file_name:  The source .edi file this row came from
--   - syntax_id:     Always "ANA:1" for TRADACOMS (Article Numbering Assoc.)
--   - sender:        Trading partner who sent the transmission
--   - tx_date:       Transmission date (YYMMDD or YYMMDD:HHMMSS)
--   - msg_ref:       MHD_1 — sequential message number within the file
--   - msg_type:      MHD_2 — message type and version (e.g. "ORDERS:9")

ASSERT ROW_COUNT = 15
ASSERT VALUE syntax_id = 'ANA:1' WHERE df_file_name = 'tradacoms_order.edi' AND msg_ref = '1'
ASSERT VALUE msg_type = 'ORDHDR:9' WHERE df_file_name = 'tradacoms_order.edi' AND msg_ref = '1'
ASSERT VALUE msg_type = 'ORDERS:9' WHERE df_file_name = 'tradacoms_order.edi' AND msg_ref = '2'
ASSERT VALUE msg_type = 'PPRHDR:2' WHERE df_file_name = 'tradacoms_product_planning.edi' AND msg_ref = '1'
ASSERT VALUE msg_type = 'UTLHDR:3' WHERE df_file_name = 'tradacoms_utility_bill.edi' AND msg_ref = '1'
ASSERT VALUE msg_type = 'UTLBIL:3' WHERE df_file_name = 'tradacoms_utility_bill.edi' AND msg_ref = '2'
SELECT
    df_file_name,
    stx_1 AS syntax_id,
    stx_2 AS sender,
    stx_4 AS tx_date,
    mhd_1 AS msg_ref,
    mhd_2 AS msg_type
FROM {{zone_name}}.edi_demos.tradacoms_messages
ORDER BY df_file_name, mhd_1;


-- ============================================================================
-- 2. Message Type Distribution
-- ============================================================================
-- Groups messages by their MHD_2 type to show the variety of TRADACOMS
-- message types across all files. Types follow the pattern NAME:VERSION.
--
-- What you'll see:
--   - message_type:   TRADACOMS message type (ORDERS:9, UTLBIL:3, etc.)
--   - message_count:  How many messages of that type across all files
--
-- 10 distinct types — ORDHDR:9, ORDERS:9, ORDTLR:9, PPRHDR:2,
-- PPRDET:2, PPRTLR:2, UTLHDR:3, UTLBIL:3, UVATLR:3, UTLTLR:3

ASSERT ROW_COUNT = 10
ASSERT VALUE message_count = 2 WHERE message_type = 'ORDERS:9'
ASSERT VALUE message_count = 1 WHERE message_type = 'ORDHDR:9'
ASSERT VALUE message_count = 1 WHERE message_type = 'ORDTLR:9'
ASSERT VALUE message_count = 2 WHERE message_type = 'UTLBIL:3'
ASSERT VALUE message_count = 2 WHERE message_type = 'UTLHDR:3'
ASSERT VALUE message_count = 1 WHERE message_type = 'PPRDET:2'
SELECT
    mhd_2 AS message_type,
    COUNT(*) AS message_count
FROM {{zone_name}}.edi_demos.tradacoms_messages
GROUP BY mhd_2
ORDER BY mhd_2;


-- ============================================================================
-- 3. Source File Distribution
-- ============================================================================
-- Shows how many MHD messages each TRADACOMS file contains. This
-- demonstrates that a single TRADACOMS file is a multi-message envelope.
--
-- What you'll see:
--   - source_file:    The .edi file name
--   - messages:       Number of MHD segments (rows) from that file
--
-- 4 files — order(4), planning(3), utility_bill(4), escape(4)

ASSERT ROW_COUNT = 4
ASSERT VALUE messages = 4 WHERE source_file = 'tradacoms_order.edi'
ASSERT VALUE messages = 3 WHERE source_file = 'tradacoms_product_planning.edi'
ASSERT VALUE messages = 4 WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE messages = 4 WHERE source_file = 'tradacoms_utility_bill_escape.edi'
SELECT
    df_file_name AS source_file,
    COUNT(*) AS messages
FROM {{zone_name}}.edi_demos.tradacoms_messages
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. Trading Partners
-- ============================================================================
-- Extracts unique sender/receiver pairs from the STX envelope. In
-- TRADACOMS, STX_2 identifies the sender and STX_3 the recipient.
-- Some include EAN codes (e.g. "5017416000006"), others use names.
--
-- What you'll see:
--   - sender:     STX_2 — originator of the transmission
--   - receiver:   STX_3 — intended recipient
--   - tx_count:   Number of messages from this partner pair

ASSERT ROW_COUNT = 3
ASSERT VALUE tx_count = 8 WHERE sender = '1011101000000:SOME ELECTRIC COMPANY PLC'
ASSERT VALUE tx_count = 4 WHERE sender = ':ANY SHOP PLC'
ASSERT VALUE tx_count = 3 WHERE sender = '5017416000006'
SELECT
    stx_2 AS sender,
    stx_3 AS receiver,
    COUNT(*) AS tx_count
FROM {{zone_name}}.edi_demos.tradacoms_messages
GROUP BY stx_2, stx_3
ORDER BY tx_count DESC;


-- ============================================================================
-- 5. Document Types — Materialized View
-- ============================================================================
-- Reads from the materialized table where TYP and SDT/CDT fields have been
-- extracted as first-class SQL columns. TYP appears in message bodies
-- (not all messages have it — header/trailer messages may be NULL).
--
-- What you'll see:
--   - source_file:    Which .edi file this came from
--   - msg_type:       MHD_2 message type
--   - typ_code:       TYP_1 — transaction type code (e.g. "0430")
--   - typ_version:    TYP_2 — sub-type or version indicator
--   - supplier_name:  SDT_2 — supplier trading name

ASSERT ROW_COUNT = 15
ASSERT VALUE typ_code = '0430' WHERE source_file = 'tradacoms_order.edi' AND msg_type = 'ORDHDR:9'
ASSERT VALUE typ_version = 'NEW-ORDERS' WHERE source_file = 'tradacoms_order.edi' AND msg_type = 'ORDHDR:9'
ASSERT VALUE supplier_name = 'XYZ MANUFACTURING PLC' WHERE source_file = 'tradacoms_order.edi' AND msg_type = 'ORDHDR:9'
ASSERT VALUE typ_code = '2300' WHERE source_file = 'tradacoms_product_planning.edi' AND msg_type = 'PPRHDR:2'
ASSERT VALUE typ_code = '0715' WHERE source_file = 'tradacoms_utility_bill.edi' AND msg_type = 'UTLHDR:3'
SELECT
    df_file_name AS source_file,
    mhd_2 AS msg_type,
    typ_1 AS typ_code,
    typ_2 AS typ_version,
    sdt_2 AS supplier_name
FROM {{zone_name}}.edi_demos.tradacoms_materialized
ORDER BY df_file_name, mhd_1;


-- ============================================================================
-- 6. TRADACOMS Date Range
-- ============================================================================
-- Shows the transmission dates from the STX_4 field. TRADACOMS date format
-- varies: older files use YYMMDD (e.g. "940321"), newer ones include time
-- as YYMMDD:HHMMSS (e.g. "180513:025446").
--
-- What you'll see:
--   - source_file:    The .edi file
--   - tx_date:        STX_4 — date (and possibly time) of transmission
--   - tx_reference:   STX_5 — unique transmission reference

ASSERT ROW_COUNT = 4
ASSERT VALUE tx_date = '940321' WHERE source_file = 'tradacoms_order.edi'
ASSERT VALUE tx_reference = 'REFS' WHERE source_file = 'tradacoms_order.edi'
ASSERT VALUE tx_date = '180513:025446' WHERE source_file = 'tradacoms_product_planning.edi'
ASSERT VALUE tx_reference = '11488' WHERE source_file = 'tradacoms_product_planning.edi'
SELECT DISTINCT
    df_file_name AS source_file,
    stx_4 AS tx_date,
    stx_5 AS tx_reference
FROM {{zone_name}}.edi_demos.tradacoms_messages
ORDER BY stx_4;


-- ============================================================================
-- 7. Escape Character Test
-- ============================================================================
-- TRADACOMS uses ? as an escape character. The escape test file contains
-- names like "GEORGE?'S FRIED CHIKEN ?+ SONS" where ?' escapes the
-- apostrophe (segment delimiter) and ?+ escapes the plus sign (data
-- element separator). DeltaForge decodes these automatically.
--
-- What you'll see:
--   - msg_type:       Message type from the escape test file
--   - customer_name:  CDT_2 — should show decoded text with apostrophe and +
--   - supplier_name:  SDT_2 — supplier from the same file

ASSERT ROW_COUNT = 4
ASSERT VALUE supplier_name = 'SITE 1' WHERE msg_type = 'UTLHDR:3'
ASSERT VALUE customer_name = 'GEORGE''S FRIED CHIKEN + SONS. Could be the best chicken yet?' WHERE msg_type = 'UTLHDR:3'
SELECT
    mhd_2 AS msg_type,
    cdt_2 AS customer_name,
    sdt_2 AS supplier_name
FROM {{zone_name}}.edi_demos.tradacoms_materialized
WHERE df_file_name LIKE '%escape%'
ORDER BY mhd_1;


-- ============================================================================
-- 8. Full Transaction JSON
-- ============================================================================
-- Every row includes df_transaction_json containing the complete parsed
-- TRADACOMS message as a JSON structure. This enables access to ANY
-- segment/field — including segments not materialized (ORD, OLD, DIN,
-- IL, SFR, PDN, PLO, BCD, CCD, VAT, etc.).
--
-- What you'll see:
--   - df_file_name:         Source file name
--   - msg_type:             MHD_2 message type
--   - df_transaction_json:  Full message as JSON (click to expand in the UI)
--
-- Tip: The JSON contains every segment with its fields. Use JSON functions
-- for deep access without needing materialized_paths.

ASSERT ROW_COUNT = 3
ASSERT VALUE df_transaction_json IS NOT NULL WHERE df_file_name = 'tradacoms_order.edi' AND mhd_1 = '1'
ASSERT VALUE df_transaction_json IS NOT NULL WHERE df_file_name = 'tradacoms_order.edi' AND mhd_1 = '2'
ASSERT VALUE msg_type = 'ORDHDR:9' WHERE df_file_name = 'tradacoms_order.edi' AND mhd_1 = '1'
SELECT
    df_file_name,
    mhd_1,
    mhd_2 AS msg_type,
    df_transaction_json
FROM {{zone_name}}.edi_demos.tradacoms_messages
ORDER BY df_file_name, mhd_1
LIMIT 3;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, distinct types, and key invariants.
-- 4 files → 15 rows total (ORDHDR+ORDERS×2+ORDTLR=4, PPRHDR+PPRDET+PPRTLR=3,
-- UTLHDR+UTLBIL+UVATLR+UTLTLR=4 per utility file × 2 = 8), 10 distinct types.

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'source_files_4'
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_types'
ASSERT VALUE result = 'PASS' WHERE check_name = 'materialized_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
SELECT check_name, result FROM (

    -- Check 1: Exact total message count = 15 (4+3+4+4 rows across 4 files)
    SELECT 'message_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_messages) = 15
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 4 distinct source files in df_file_name
    SELECT 'source_files_4' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi_demos.tradacoms_messages) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Exactly 10 distinct message types in MHD_2
    SELECT 'message_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT mhd_2) FROM {{zone_name}}.edi_demos.tradacoms_messages) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Materialized table has same 15 rows (same files, additional columns)
    SELECT 'materialized_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_materialized) = 15
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: df_transaction_json is populated for all 15 messages
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_messages
                       WHERE df_transaction_json IS NOT NULL) = 15
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
