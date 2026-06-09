-- ============================================================================
-- EDIFACT Invoice Reconciliation — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge reconciles EDIFACT procurement documents
-- across the order-to-invoice cycle: purchase orders (ORDERS), order responses
-- (ORDRSP), and invoices (INVOIC) from multiple trading partners and EDIFACT
-- directory versions.
--
-- Two tables are available:
--   commerce_messages      — Compact view: UNB/UNH headers + full JSON
--   commerce_materialized  — Enriched view: headers + BGM/NAD/LIN/DTM/MOA/TAX
--
-- Column reference (always available — UNB envelope fields):
--   UNB_1 = Syntax identifier (UNOB, UNOC)
--   UNB_2 = Interchange sender
--   UNB_3 = Interchange recipient
--   UNB_4 = Date/time of preparation
--   UNB_5 = Interchange control reference
--
-- Column reference (always available — UNH message header):
--   UNH_1 = Message reference number
--   UNH_2 = Message type (ORDERS, ORDRSP, INVOIC)
--
-- Materialized columns (commerce_materialized table only):
--   BGM_1 = Document name code    BGM_2 = Document number
--   NAD_1 = Party qualifier       NAD_2 = Party identification (full composite)
--   LIN_1 = Line item number      LIN_3 = Item number (full composite)
--   DTM_1 = Date/time composite (qualifier:value:format, e.g. '137:20251008:102')
--   MOA_1 = Monetary amount composite (qualifier:amount, e.g. '203:699.84')
--   TAX_1 = Tax type qualifier
-- ============================================================================


-- ============================================================================
-- 1. Document Overview
-- ============================================================================
-- Shows the UNB envelope header of every EDIFACT message. Each row is one
-- message parsed from the 4 source files. The syntax identifier (UNB_1)
-- and sender/recipient fields reveal the interchange partners.
--
-- What you'll see:
--   - df_file_name:  The source .edi file this row came from
--   - syntax_id:     UNB_1 — syntax identifier (UNOB or UNOC)
--   - sender:        UNB_2 — interchange sender ID
--   - recipient:     UNB_3 — interchange recipient ID
--   - msg_type:      UNH_2 — message type (ORDERS, ORDRSP, INVOIC)

ASSERT ROW_COUNT = 4
ASSERT VALUE sender = 'SENDER1' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE syntax_id = 'UNOC' WHERE df_file_name = 'edifact_ORDRSP_order_response.edi'
SELECT
    df_file_name,
    unb_1 AS syntax_id,
    unb_2 AS sender,
    unb_3 AS recipient,
    unh_2 AS msg_type
FROM {{zone_name}}.edi_demos.commerce_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Document Classification (BGM)
-- ============================================================================
-- The BGM (Beginning of Message) segment identifies each document's purpose.
-- BGM_1 is the document name code — a numeric or alphanumeric classifier:
--   220  = Purchase Order
--   380  = Commercial Invoice
--   393  = Factored Invoice
--   Z12  = Order Response
--
-- What you'll see:
--   - df_file_name:  Source file
--   - bgm_code:      BGM_1 — document name code
--   - bgm_decoded:   Human-readable document type
--   - doc_number:    BGM_2 — document/message number (PO or invoice number)

ASSERT ROW_COUNT = 4
ASSERT VALUE doc_number = '128576' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE bgm_code = '380' WHERE df_file_name = 'edifact_INVOIC_invoice_edifabric.edi'
SELECT
    df_file_name,
    bgm_1 AS bgm_code,
    CASE bgm_1
        WHEN '220' THEN 'Purchase Order'
        WHEN '380' THEN 'Commercial Invoice'
        WHEN '393' THEN 'Factored Invoice'
        WHEN 'Z12' THEN 'Order Response'
        ELSE bgm_1
    END AS bgm_decoded,
    bgm_2 AS doc_number
FROM {{zone_name}}.edi_demos.commerce_materialized
ORDER BY df_file_name;


-- ============================================================================
-- 3. Order vs Invoice Classification
-- ============================================================================
-- Groups documents into Order, Invoice, or Response categories using BGM_1.
-- This is the reconciliation pivot — matching orders against invoices.
--
-- What you'll see:
--   - doc_type:   Classified category (Order, Invoice, or Response)
--   - doc_count:  Number of documents in each category
--
-- 3 categories: Order=1 (ORDERS), Invoice=2 (both INVOIC), Response=1 (ORDRSP)

ASSERT ROW_COUNT = 3
ASSERT VALUE doc_count = 2 WHERE doc_type = 'Invoice'
SELECT
    CASE
        WHEN bgm_1 = '220' THEN 'Order'
        WHEN bgm_1 IN ('380', '393') THEN 'Invoice'
        ELSE 'Response'
    END AS doc_type,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi_demos.commerce_materialized
GROUP BY
    CASE
        WHEN bgm_1 = '220' THEN 'Order'
        WHEN bgm_1 IN ('380', '393') THEN 'Invoice'
        ELSE 'Response'
    END
ORDER BY doc_count DESC;


-- ============================================================================
-- 4. Trading Partners (NAD)
-- ============================================================================
-- The NAD (Name and Address) segment identifies trading partners. NAD_1
-- is the party qualifier and NAD_2 is the party identification composite
-- (EAN/GLN with qualifier, e.g. '4012345500004::9'). Last NAD wins.
-- The D01B INVOIC has no NAD segments, so it is excluded.
--
-- Party qualifiers:
--   SU = Supplier
--   BY = Buyer
--   DP = Delivery Party
--
-- What you'll see:
--   - df_file_name:  Source file
--   - party_role:    NAD_1 decoded to human-readable role
--   - party_code:    NAD_1 — raw qualifier code
--   - party_id:      NAD_2 — party identification composite (EAN/GLN)

ASSERT ROW_COUNT = 3
ASSERT VALUE party_id = '4012345500004::9' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE party_code = 'DP' WHERE df_file_name = 'edifact_INVOIC_invoice_edifabric.edi'
SELECT
    df_file_name,
    CASE nad_1
        WHEN 'SU' THEN 'Supplier'
        WHEN 'BY' THEN 'Buyer'
        WHEN 'DP' THEN 'Delivery Party'
        ELSE nad_1
    END AS party_role,
    nad_1 AS party_code,
    nad_2 AS party_id
FROM {{zone_name}}.edi_demos.commerce_materialized
WHERE nad_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 5. Timeline (DTM)
-- ============================================================================
-- The DTM (Date/Time/Period) segment carries date info as a single composite
-- element: qualifier:value:format (e.g. '137:20251008:102'). DTM_1 holds the
-- full composite. Last DTM occurrence per message wins.
--
-- Date qualifiers (first component):
--   2   = Delivery date (requested)
--   137 = Document/message date
--   171 = Reference date/time
--
-- What you'll see:
--   - df_file_name:    Source file
--   - dtm_composite:   DTM_1 — full composite (qualifier:date:format)

ASSERT ROW_COUNT = 4
ASSERT VALUE dtm_composite = '2:20020913:102' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE dtm_composite = '137:20251008:102' WHERE df_file_name = 'edifact_D01B_INVOIC_invoice.edi'
ASSERT VALUE dtm_composite = '171:20020215:102' WHERE df_file_name = 'edifact_INVOIC_invoice_edifabric.edi'
ASSERT VALUE dtm_composite = '171:201510231149:203' WHERE df_file_name = 'edifact_ORDRSP_order_response.edi'
SELECT
    df_file_name,
    dtm_1 AS dtm_composite
FROM {{zone_name}}.edi_demos.commerce_materialized
WHERE dtm_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 6. Monetary Amounts (MOA)
-- ============================================================================
-- The MOA (Monetary Amount) segment carries financial values as a composite
-- field: qualifier:amount (e.g. "203:699.84"). All 4 messages have MOA.
--
-- MOA qualifiers:
--   24  = Payable amount
--   125 = Taxable amount
--   131 = Total charges/allowances
--   203 = Line item amount
--
-- What you'll see:
--   - df_file_name:  Source file
--   - msg_type:      UNH_2 — message type
--   - moa_composite: MOA_1 — full composite (qualifier:amount)

ASSERT ROW_COUNT = 4
ASSERT VALUE moa_composite = '203:699.84' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE moa_composite = '131:120' WHERE df_file_name = 'edifact_INVOIC_invoice_edifabric.edi'
ASSERT VALUE moa_composite = '24:9' WHERE df_file_name = 'edifact_ORDRSP_order_response.edi'
ASSERT VALUE moa_composite = '125:666.660000' WHERE df_file_name = 'edifact_D01B_INVOIC_invoice.edi'
SELECT
    df_file_name,
    unh_2 AS msg_type,
    moa_1 AS moa_composite
FROM {{zone_name}}.edi_demos.commerce_materialized
ORDER BY df_file_name;


-- ============================================================================
-- 7. Tax Analysis (TAX)
-- ============================================================================
-- The TAX (Duty/Tax/Fee Details) segment identifies tax types. TAX_1 is the
-- duty/tax/fee type qualifier. ORDRSP has no TAX segment so it is excluded.
--
-- Tax qualifier:
--   7 = Value Added Tax (VAT)
--
-- What you'll see:
--   - df_file_name:  Source file
--   - msg_type:      UNH_2 — message type
--   - tax_type:      TAX_1 — raw qualifier
--   - tax_decoded:   Human-readable tax type

ASSERT ROW_COUNT = 3
ASSERT VALUE tax_type = '7' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
SELECT
    df_file_name,
    unh_2 AS msg_type,
    tax_1 AS tax_type,
    CASE tax_1
        WHEN '7' THEN 'Value Added Tax (VAT)'
        ELSE tax_1
    END AS tax_decoded
FROM {{zone_name}}.edi_demos.commerce_materialized
WHERE tax_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity checks: total rows, BGM populated, MOA populated,
-- and df_transaction_json non-null for all messages.

ASSERT ROW_COUNT = 4
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'all_have_bgm'
ASSERT VALUE result = 'PASS' WHERE check_name = 'moa_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
SELECT check_name, result FROM (

    -- Check 1: Total message count = 4 (one per .edi file)
    SELECT 'message_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.commerce_messages) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: All 4 messages have BGM_1 populated
    SELECT 'all_have_bgm' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.commerce_materialized
                       WHERE bgm_1 IS NOT NULL) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: All 4 messages have MOA_1 populated
    SELECT 'moa_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.commerce_materialized
                       WHERE moa_1 IS NOT NULL) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: df_transaction_json is populated for all 4 messages
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.commerce_messages
                       WHERE df_transaction_json IS NOT NULL) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
