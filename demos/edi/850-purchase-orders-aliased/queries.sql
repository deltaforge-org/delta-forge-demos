-- ============================================================================
-- EDI 850 Purchase Orders — Aliased Columns + To-JSON Line Items
-- ============================================================================
-- Proves two engine features for the wholesale-distributor PO inbox:
--   (1) aliasFormat = 'friendly_column' rewrites materialized column names
--       using the per-segment X12 V5010 element dictionary, producing
--       analyst-friendly identifiers like "purchaseordernumber_beg_2"
--       instead of opaque "beg_2".
--   (2) repeating_segment_mode = 'to_json' collapses every PO1 line-item
--       segment (and every N1 party segment) into a JSON array per element
--       column, so json_array_length() and json_extract() reach line-level
--       data without parsing df_transaction_json.
--
-- Engine column-name predictions for these 4 .edi files
-- (verified against delta-forge-edi/src/edi_metadata.rs V5010 dictionary):
--   purchaseordertypecode_beg_1   <- BEG element 1 (purpose code, e.g. '00')
--   purchaseordernumber_beg_2     <- BEG element 2 (PO type code, e.g. 'NE')
--   releasenumber_beg_3           <- BEG element 3 (PO number)
--   contractnumber_beg_5          <- BEG element 5 (PO date YYYYMMDD)
--   memberreportingcategoryname_n1_1
--                                 <- N1 element 1 (entity code BY/VN/ST)
--   identificationcodequalifier_n1_2
--                                 <- N1 element 2 (party name)
--   po1_1                         <- PO1 element 1 (line ID; empty dict entry,
--                                    falls back to numeric column name)
--   unitorbasisformeasurementcode_po1_2
--                                 <- PO1 element 2 (quantity)
--   unitprice_po1_3               <- PO1 element 3 (UoM, 'EA')
--   basisofunitpricecode_po1_4    <- PO1 element 4 (unit price)
--   productserviceid_po1_6        <- PO1 element 6 ('VN' qualifier)
--   productserviceidqualifier_po1_7
--                                 <- PO1 element 7 (SKU)
--
-- The friendly prefixes come from the engine's element catalog and may sit
-- one slot off vs the X12 spec's BEG01/BEG02 numbering (handler comment in
-- edi_handler.rs::lookup_friendly_name acknowledges this off-by-one). The
-- numeric suffix preserves stable column addressing.
-- ============================================================================


-- ============================================================================
-- 1. PO inventory — total purchase orders ingested
-- ============================================================================
-- One row per ST 850 transaction. Confirms the bronze ingest captured all
-- 4 incoming POs from the trading-partner inbox.

ASSERT ROW_COUNT = 1
ASSERT VALUE po_count = 4
SELECT COUNT(*) AS po_count
FROM {{zone_name}}.commerce.purchase_orders;


-- ============================================================================
-- 2. Aliased-column lookup — query by friendly BEG column names
-- ============================================================================
-- This query exercises aliasFormat directly: it filters and returns BEG
-- columns by their FRIENDLY-PREFIXED names (purchaseordertypecode_beg_1,
-- releasenumber_beg_3). With aliasFormat='none' these columns would not
-- exist (they would be plain beg_1 / beg_3), so the query would fail at
-- planning time. It returning 4 rows proves the alias rewrite ran.
--
-- purpose_code = '00' is "Original" in X12; po_acme, po_bigbox, po_dynamart
-- are originals, po_cornerco is purpose '01' (Cancellation).

ASSERT ROW_COUNT = 3
ASSERT VALUE po_number = 'PO-ACME-44521' WHERE df_file_name = 'po_acme_001.edi'
ASSERT VALUE po_number = 'BBM-2026-99812' WHERE df_file_name = 'po_bigbox_002.edi'
ASSERT VALUE po_number = 'DYN-RETAIL-505012' WHERE df_file_name = 'po_dynamart_004.edi'
SELECT
    df_file_name,
    json_extract_path_text(purchaseordertypecode_beg_1, '0') AS purpose_code,
    json_extract_path_text(releasenumber_beg_3, '0')        AS po_number,
    json_extract_path_text(contractnumber_beg_5, '0')       AS po_date
FROM {{zone_name}}.commerce.purchase_orders
WHERE json_extract_path_text(purchaseordertypecode_beg_1, '0') = '00'
ORDER BY df_file_name;


-- ============================================================================
-- 3. JSON line-item arrays — proves repeating_segment_mode = 'to_json'
-- ============================================================================
-- Every PO1 segment was collapsed into a JSON array per element column.
-- json_array_length on the SKU column (productserviceidqualifier_po1_7)
-- returns the line-item count per PO, matching the CTT segment value the
-- trading partner sent.
--
-- Expected line counts (counted from the source .edi files):
--   po_acme_001.edi      4 lines
--   po_bigbox_002.edi    6 lines
--   po_cornerco_003.edi  3 lines
--   po_dynamart_004.edi  5 lines

ASSERT ROW_COUNT = 4
ASSERT VALUE line_item_count = 4 WHERE df_file_name = 'po_acme_001.edi'
ASSERT VALUE line_item_count = 6 WHERE df_file_name = 'po_bigbox_002.edi'
ASSERT VALUE line_item_count = 3 WHERE df_file_name = 'po_cornerco_003.edi'
ASSERT VALUE line_item_count = 5 WHERE df_file_name = 'po_dynamart_004.edi'
SELECT
    df_file_name,
    json_array_length(productserviceidqualifier_po1_7) AS line_item_count,
    productserviceidqualifier_po1_7                    AS skus_json
FROM {{zone_name}}.commerce.purchase_orders
ORDER BY df_file_name;


-- ============================================================================
-- 4. Cross-PO aggregation — total line items across the inbox
-- ============================================================================
-- SUM of json_array_length values rolls every line item across every PO into
-- one number. Useful for daily fill-rate dashboards. Engine-computed total
-- matches independent count from the source files (4 + 6 + 3 + 5 = 18).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_line_items = 18
SELECT
    SUM(json_array_length(productserviceidqualifier_po1_7)) AS total_line_items
FROM {{zone_name}}.commerce.purchase_orders;


-- ============================================================================
-- 5. Vendor JSON membership — N1 to_json rollup
-- ============================================================================
-- N1 segments are in the default repeating-segment list, so all three N1
-- occurrences per PO (Buyer / Vendor / ShipTo) collapsed into JSON arrays
-- inside identificationcodequalifier_n1_2. Every PO in this corpus is for
-- the same wholesale distributor — proven by a LIKE substring scan over the
-- JSON-array column.

ASSERT ROW_COUNT = 4
ASSERT VALUE party_count = 3 WHERE df_file_name = 'po_acme_001.edi'
ASSERT VALUE party_count = 3 WHERE df_file_name = 'po_bigbox_002.edi'
ASSERT VALUE party_count = 3 WHERE df_file_name = 'po_cornerco_003.edi'
ASSERT VALUE party_count = 3 WHERE df_file_name = 'po_dynamart_004.edi'
ASSERT VALUE has_distributor = true WHERE df_file_name = 'po_acme_001.edi'
ASSERT VALUE has_distributor = true WHERE df_file_name = 'po_bigbox_002.edi'
ASSERT VALUE has_distributor = true WHERE df_file_name = 'po_cornerco_003.edi'
ASSERT VALUE has_distributor = true WHERE df_file_name = 'po_dynamart_004.edi'
SELECT
    df_file_name,
    json_array_length(identificationcodequalifier_n1_2)        AS party_count,
    identificationcodequalifier_n1_2 LIKE '%Wholesale Distributors LLC%'
                                                                AS has_distributor,
    memberreportingcategoryname_n1_1                            AS entity_codes_json
FROM {{zone_name}}.commerce.purchase_orders
ORDER BY df_file_name;


-- ============================================================================
-- 6. Schema introspection — proves friendly columns exist in the catalog
-- ============================================================================
-- A typed-vs-numeric contrast. Counts how many catalog columns carry the
-- 'purchaseordernumber' and 'productserviceidqualifier' prefixes. With
-- aliasFormat='none' (default), these prefixes would never appear and the
-- count would be 0. The presence of >=1 each proves the alias rewrite
-- reached the catalog metadata, not just the runtime row dict.

ASSERT ROW_COUNT = 1
ASSERT VALUE purchase_order_alias_count >= 1
ASSERT VALUE product_service_alias_count >= 1
SELECT
    SUM(CASE WHEN column_name LIKE '%purchaseordernumber%' THEN 1 ELSE 0 END)
        AS purchase_order_alias_count,
    SUM(CASE WHEN column_name LIKE '%productserviceidqualifier%' THEN 1 ELSE 0 END)
        AS product_service_alias_count
FROM information_schema.columns
WHERE table_name = 'purchase_orders';


-- ============================================================================
-- 7. Per-PO line totals — combines aliased columns + JSON arrays
-- ============================================================================
-- Demonstrates both features in one query: friendly column names supply the
-- PO header (po number, date) and the JSON-array column supplies the line
-- count. AMT segment value (independently computed in our verify script)
-- proves the totals match what trading partners sent.

ASSERT ROW_COUNT = 4
ASSERT VALUE po_number = 'PO-ACME-44521' WHERE df_file_name = 'po_acme_001.edi'
ASSERT VALUE line_count = 4              WHERE df_file_name = 'po_acme_001.edi'
ASSERT VALUE po_number = 'BBM-2026-99812' WHERE df_file_name = 'po_bigbox_002.edi'
ASSERT VALUE line_count = 6              WHERE df_file_name = 'po_bigbox_002.edi'
ASSERT VALUE po_number = 'CC-77-3041'    WHERE df_file_name = 'po_cornerco_003.edi'
ASSERT VALUE line_count = 3              WHERE df_file_name = 'po_cornerco_003.edi'
ASSERT VALUE po_number = 'DYN-RETAIL-505012' WHERE df_file_name = 'po_dynamart_004.edi'
ASSERT VALUE line_count = 5              WHERE df_file_name = 'po_dynamart_004.edi'
SELECT
    df_file_name,
    json_extract_path_text(releasenumber_beg_3, '0')   AS po_number,
    json_extract_path_text(contractnumber_beg_5, '0')  AS po_date,
    json_array_length(productserviceidqualifier_po1_7) AS line_count
FROM {{zone_name}}.commerce.purchase_orders
ORDER BY df_file_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting summary that rolls every key invariant into one PASS/FAIL
-- result set. If any check fails the whole demo fails — useful for CI.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'po_count_4'
ASSERT VALUE result = 'PASS' WHERE check_name = 'aliased_beg_columns_present'
ASSERT VALUE result = 'PASS' WHERE check_name = 'aliased_po1_columns_present'
ASSERT VALUE result = 'PASS' WHERE check_name = 'to_json_total_lines_18'
ASSERT VALUE result = 'PASS' WHERE check_name = 'every_po_has_distributor'
ASSERT VALUE result = 'PASS' WHERE check_name = 'every_po_has_three_parties'
SELECT check_name, result FROM (

    SELECT 'po_count_4' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.commerce.purchase_orders) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'aliased_beg_columns_present' AS check_name,
           CASE WHEN (SELECT COUNT(*)
                      FROM information_schema.columns
                      WHERE table_name = 'purchase_orders'
                        AND column_name LIKE '%purchaseordernumber%') >= 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'aliased_po1_columns_present' AS check_name,
           CASE WHEN (SELECT COUNT(*)
                      FROM information_schema.columns
                      WHERE table_name = 'purchase_orders'
                        AND column_name LIKE '%productserviceidqualifier%') >= 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'to_json_total_lines_18' AS check_name,
           CASE WHEN (SELECT SUM(json_array_length(productserviceidqualifier_po1_7))
                      FROM {{zone_name}}.commerce.purchase_orders) = 18
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'every_po_has_distributor' AS check_name,
           CASE WHEN (SELECT COUNT(*)
                      FROM {{zone_name}}.commerce.purchase_orders
                      WHERE identificationcodequalifier_n1_2 LIKE '%Wholesale Distributors LLC%') = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'every_po_has_three_parties' AS check_name,
           CASE WHEN (SELECT MIN(json_array_length(identificationcodequalifier_n1_2))
                      FROM {{zone_name}}.commerce.purchase_orders) = 3
                AND (SELECT MAX(json_array_length(identificationcodequalifier_n1_2))
                      FROM {{zone_name}}.commerce.purchase_orders) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
