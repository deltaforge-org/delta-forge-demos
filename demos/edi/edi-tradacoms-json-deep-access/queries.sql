-- ============================================================================
-- EDI TRADACOMS — JSON Deep Segment Extraction — Demo Queries
-- ============================================================================
-- Showcases SQL JSON functions for on-demand exploration and extraction of
-- deeply nested TRADACOMS segments from df_transaction_json — without needing
-- materialized_paths.
--
-- JSON functions demonstrated:
--   json_array_length()        — Count body segments per message
--   json_extract_path_text()   — Extract text value at a JSON path
--   LENGTH / REPLACE           — String-based segment type counting
--
-- df_transaction_json structure (TRADACOMS):
--   A JSON array of segment objects (body segments only — no MHD/MTR):
--   [
--     {"segment":"TYP","elements":[{"name":"Type Code","value":"0430"},...]},
--     {"segment":"SDT","elements":[{"name":"Supplier Code","value":"..."},...]},
--     ...
--   ]
--
-- Column reference (always available — STX envelope fields):
--   STX_1  = Syntax identifier (e.g. "ANA:1")
--   STX_2  = Sender identification
--   STX_3  = Recipient identification
--   STX_4  = Transmission date/time
--   STX_5  = Transmission reference
--   MHD_1  = Message reference number
--   MHD_2  = Message type:version (e.g. "ORDERS:9", "PPRDET:2")
-- ============================================================================


-- ============================================================================
-- 1. Message Overview with JSON Segment Counts
-- ============================================================================
-- Uses json_array_length to count how many body segments (between MHD and MTR)
-- each message contains. This reveals message complexity at a glance —
-- PPRDET:2 has 32 segments while PPRTLR:2 has just 1.
--
-- What you'll see:
--   - source_file:    The .edi file this message came from
--   - msg_type:       TRADACOMS message type (e.g. ORDERS:9, PPRDET:2)
--   - msg_ref:        Sequential message number within the file
--   - segment_count:  Number of body segments in df_transaction_json

ASSERT ROW_COUNT = 7
ASSERT VALUE segment_count = 32 WHERE source_file = 'tradacoms_product_planning.edi' AND msg_type = 'PPRDET:2'
ASSERT VALUE segment_count = 1 WHERE source_file = 'tradacoms_product_planning.edi' AND msg_type = 'PPRTLR:2'
ASSERT VALUE segment_count = 4 WHERE source_file = 'tradacoms_product_planning.edi' AND msg_type = 'PPRHDR:2'
ASSERT VALUE segment_count = 4 WHERE source_file = 'tradacoms_order.edi' AND msg_type = 'ORDHDR:9'
ASSERT VALUE segment_count = 1 WHERE source_file = 'tradacoms_order.edi' AND msg_type = 'ORDTLR:9'
ASSERT VALUE segment_count = 7 WHERE source_file = 'tradacoms_order.edi' AND msg_type = 'ORDERS:9' AND msg_ref = '2'
ASSERT VALUE segment_count = 5 WHERE source_file = 'tradacoms_order.edi' AND msg_type = 'ORDERS:9' AND msg_ref = '3'
SELECT
    df_file_name AS source_file,
    mhd_2 AS msg_type,
    mhd_1 AS msg_ref,
    json_array_length(df_transaction_json) AS segment_count
FROM {{zone_name}}.edi_demos.tradacoms_json_messages
ORDER BY df_file_name, mhd_1;


-- ============================================================================
-- 2. First Segment Tag per Message — JSON Array Access
-- ============================================================================
-- Extracts the segment tag of the first body segment in each message using
-- json_extract_path_text at array index 0. This reveals what type of segment
-- each message starts with after MHD:
--   PPRHDR → TYP, PPRDET → SFR, PPRTLR → PPT
--   ORDHDR → TYP, ORDERS → CLO, ORDTLR → OFT
--
-- What you'll see:
--   - msg_type:        TRADACOMS message type
--   - first_segment:   Tag of the first body segment (e.g. TYP, SFR, CLO)

ASSERT ROW_COUNT = 7
ASSERT VALUE first_segment = 'TYP' WHERE msg_type = 'PPRHDR:2'
ASSERT VALUE first_segment = 'SFR' WHERE msg_type = 'PPRDET:2'
ASSERT VALUE first_segment = 'PPT' WHERE msg_type = 'PPRTLR:2'
ASSERT VALUE first_segment = 'OFT' WHERE msg_type = 'ORDTLR:9'
ASSERT VALUE first_segment = 'TYP' WHERE msg_type = 'ORDHDR:9'
ASSERT VALUE first_segment = 'CLO' WHERE msg_type = 'ORDERS:9' AND msg_ref = '2'
ASSERT VALUE first_segment = 'CLO' WHERE msg_type = 'ORDERS:9' AND msg_ref = '3'
SELECT
    mhd_2 AS msg_type,
    mhd_1 AS msg_ref,
    json_extract_path_text(df_transaction_json, '0', 'segment') AS first_segment
FROM {{zone_name}}.edi_demos.tradacoms_json_messages
ORDER BY df_file_name, mhd_1;


-- ============================================================================
-- 3. Product Codes from Planning Data — Deep JSON Path Navigation
-- ============================================================================
-- Navigates into the PPRDET:2 message to extract product codes from the PDN
-- (Product Definition) segment. PDN is the second segment (index 1) in the
-- planning detail. Its elements array contains the product identifier at
-- index 1 (element 2 in TRADACOMS numbering).
--
-- This demonstrates the core value proposition: extracting deeply nested
-- segment values without pre-configuring materialized_paths.
--
-- What you'll see:
--   - segment_tag:   Should be 'PDN' (Product Definition Number)
--   - product_code:  The TRADACOMS product identifier (e.g. ":C13T08954010")

ASSERT ROW_COUNT = 1
ASSERT VALUE segment_tag = 'PDN'
ASSERT VALUE product_code = ':C13T08954010'
SELECT
    json_extract_path_text(df_transaction_json, '1', 'segment') AS segment_tag,
    json_extract_path_text(df_transaction_json, '1', 'elements', '1', 'value') AS product_code
FROM {{zone_name}}.edi_demos.tradacoms_json_messages
WHERE mhd_2 = 'PPRDET:2';


-- ============================================================================
-- 4. Stock Forecast Totals — SFX Segment Extraction
-- ============================================================================
-- Extracts stock forecast summary values from SFX (Stock Forecast Extension)
-- segments in the PPRDET message. The first SFX segment appears at index 16
-- (0-based) in the planning detail, containing the forecast type (STK = stock)
-- and the aggregated forecast quantity.
--
-- What you'll see:
--   - sfx_segment:     Should be 'SFX'
--   - forecast_type:   'STK' (stock) or 'SAL' (sales)
--   - forecast_value:  Aggregated quantity (e.g. 339 units for stock)

ASSERT ROW_COUNT = 1
ASSERT VALUE sfx_segment = 'SFX'
ASSERT VALUE forecast_type = 'STK'
ASSERT VALUE forecast_value = '339'
SELECT
    json_extract_path_text(df_transaction_json, '16', 'segment') AS sfx_segment,
    json_extract_path_text(df_transaction_json, '16', 'elements', '3', 'value') AS forecast_type,
    json_extract_path_text(df_transaction_json, '16', 'elements', '4', 'value') AS forecast_value
FROM {{zone_name}}.edi_demos.tradacoms_json_messages
WHERE mhd_2 = 'PPRDET:2';


-- ============================================================================
-- 5. Order Line Descriptions — OLD Segment via JSON
-- ============================================================================
-- Extracts product descriptions from OLD (Order Line Detail) segments in the
-- first ORDERS:9 message (msg_ref=2). OLD segments carry the product name at
-- element index 9 (the 10th element in TRADACOMS numbering).
--
-- The first ORDERS message has 3 OLD segments at indices 3, 4, 5 (after
-- CLO=0, ORD=1, DIN=2). This query pulls all three product descriptions
-- in a single row using positional JSON access.
--
-- What you'll see:
--   - product_a:  Description from OLD at index 3 ("PRODUCT A")
--   - product_b:  Description from OLD at index 4 ("PRODUCT B")
--   - product_c:  Description from OLD at index 5 ("PRODUCT C")

ASSERT ROW_COUNT = 1
ASSERT VALUE product_a = 'PRODUCT A'
ASSERT VALUE product_b = 'PRODUCT B'
ASSERT VALUE product_c = 'PRODUCT C'
SELECT
    json_extract_path_text(df_transaction_json, '3', 'elements', '9', 'value') AS product_a,
    json_extract_path_text(df_transaction_json, '4', 'elements', '9', 'value') AS product_b,
    json_extract_path_text(df_transaction_json, '5', 'elements', '9', 'value') AS product_c
FROM {{zone_name}}.edi_demos.tradacoms_json_messages
WHERE mhd_2 = 'ORDERS:9' AND mhd_1 = '2';


-- ============================================================================
-- 6. Delivery Instructions — DIN Segment Extraction
-- ============================================================================
-- Extracts delivery date and special instructions from the DIN (Delivery
-- Instructions) segment in the first ORDERS message. DIN is at index 2
-- (after CLO and ORD). Element 0 is the delivery date, element 3 is the
-- free-text instruction.
--
-- What you'll see:
--   - delivery_date:  Delivery date in YYMMDD format (e.g. "940328")
--   - instruction:    Free-text delivery instruction

ASSERT ROW_COUNT = 1
ASSERT VALUE delivery_date = '940328'
ASSERT VALUE instruction = 'RING BEFORE DELIVERY'
SELECT
    json_extract_path_text(df_transaction_json, '2', 'elements', '0', 'value') AS delivery_date,
    json_extract_path_text(df_transaction_json, '2', 'elements', '3', 'value') AS instruction
FROM {{zone_name}}.edi_demos.tradacoms_json_messages
WHERE mhd_2 = 'ORDERS:9' AND mhd_1 = '2';


-- ============================================================================
-- 7. Segment Type Frequency in Planning Detail — String Counting
-- ============================================================================
-- Counts how many times each key segment type appears in the PPRDET:2 message
-- using a string-based counting technique. This is useful when you need to
-- understand the structure of a complex message before writing targeted
-- extraction queries.
--
-- The PPRDET message contains variable-length groups:
--   PDN (Product Definition) — 2 products
--   PLO (Planning Location)  — 7 locations across both products
--   SFS (Stock Forecast)     — 8 forecast line items
--   SFX (Forecast Extension) — 4 summary totals
--
-- What you'll see:
--   - pdn_count:  Number of PDN segments (product groups)
--   - plo_count:  Number of PLO segments (locations)
--   - sfs_count:  Number of SFS segments (forecast lines)
--   - sfx_count:  Number of SFX segments (summaries)

ASSERT ROW_COUNT = 1
ASSERT VALUE pdn_count = 2
ASSERT VALUE plo_count = 7
ASSERT VALUE sfs_count = 8
ASSERT VALUE sfx_count = 4
SELECT
    (LENGTH(df_transaction_json) - LENGTH(REPLACE(df_transaction_json, '"segment":"PDN"', '')))
        / LENGTH('"segment":"PDN"') AS pdn_count,
    (LENGTH(df_transaction_json) - LENGTH(REPLACE(df_transaction_json, '"segment":"PLO"', '')))
        / LENGTH('"segment":"PLO"') AS plo_count,
    (LENGTH(df_transaction_json) - LENGTH(REPLACE(df_transaction_json, '"segment":"SFS"', '')))
        / LENGTH('"segment":"SFS"') AS sfs_count,
    (LENGTH(df_transaction_json) - LENGTH(REPLACE(df_transaction_json, '"segment":"SFX"', '')))
        / LENGTH('"segment":"SFX"') AS sfx_count
FROM {{zone_name}}.edi_demos.tradacoms_json_messages
WHERE mhd_2 = 'PPRDET:2';


-- ============================================================================
-- 8. Cross-File Message Totals
-- ============================================================================
-- Summarizes message counts per source file. The order file has 4 messages
-- (ORDHDR + 2x ORDERS + ORDTLR) and the planning file has 3 (PPRHDR +
-- PPRDET + PPRTLR).
--
-- What you'll see:
--   - source_file:      The .edi file name
--   - total_messages:   Number of MHD messages in that file

ASSERT ROW_COUNT = 2
ASSERT VALUE total_messages = 4 WHERE source_file = 'tradacoms_order.edi'
ASSERT VALUE total_messages = 3 WHERE source_file = 'tradacoms_product_planning.edi'
SELECT
    df_file_name AS source_file,
    COUNT(*) AS total_messages
FROM {{zone_name}}.edi_demos.tradacoms_json_messages
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly and JSON
-- functions work as expected against TRADACOMS data. All checks should
-- return PASS.

ASSERT ROW_COUNT = 4
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count_7'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'planning_first_segment_sfr'
ASSERT VALUE result = 'PASS' WHERE check_name = 'order_messages_4'
SELECT check_name, result FROM (

    -- Check 1: Total message count = 7 (3 planning + 4 order)
    SELECT 'message_count_7' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_json_messages) = 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: df_transaction_json is populated for all 7 messages
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_json_messages
                       WHERE df_transaction_json IS NOT NULL) = 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: First body segment of PPRDET is SFR
    SELECT 'planning_first_segment_sfr' AS check_name,
           CASE WHEN (SELECT json_extract_path_text(df_transaction_json, '0', 'segment')
                      FROM {{zone_name}}.edi_demos.tradacoms_json_messages
                      WHERE mhd_2 = 'PPRDET:2') = 'SFR'
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Order file produces exactly 4 messages
    SELECT 'order_messages_4' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_json_messages
                       WHERE df_file_name = 'tradacoms_order.edi') = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
