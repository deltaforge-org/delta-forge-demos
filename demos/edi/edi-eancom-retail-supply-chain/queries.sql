-- ============================================================================
-- EANCOM Retail Supply Chain — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge parses GS1/EANCOM messages from a
-- European retailer's EDI hub into queryable tables. Six files cover the
-- full retail supply chain: despatch advices, invoices, order responses,
-- price catalogues, transport status, and transport instructions.
--
-- Two tables are available:
--   eancom_messages      — Compact view: UNB/UNH header fields + full JSON
--   eancom_materialized  — Enriched view: UNB/UNH + BGM/NAD/LIN/STS/CPS/QTY
--
-- Column reference (always available — UNB envelope fields):
--   UNB_1  = Syntax identifier (UNOA or UNOB)
--   UNB_2  = Interchange sender
--   UNB_3  = Interchange recipient
--   UNB_4  = Date/time of preparation
--   UNB_5  = Interchange control reference
--
-- Column reference (always available — UNH message header):
--   UNH_1  = Message reference number
--   UNH_2  = Message type (DESADV, INVOIC, ORDRSP, PRICAT, IFTSTA, IFTMIN)
--
-- Materialized columns (eancom_materialized table only):
--   BGM_1  = Document name code       BGM_2  = Document number
--   NAD_1  = Party qualifier           NAD_2  = Party identification (GLN)
--   LIN_1  = Line item number          LIN_3  = Item number (GTIN)
--   STS_1  = Status event code
--   CPS_1  = Hierarchical ID (packing) QTY_1  = Quantity detail (composite)
-- ============================================================================


-- ============================================================================
-- 1. Message Overview
-- ============================================================================
-- Shows the UNB/UNH header of every EANCOM message parsed from 6 source
-- files. Each file contains exactly one message, so total rows = 6.

ASSERT ROW_COUNT = 6
ASSERT VALUE sender = 'SENDER1' WHERE df_file_name = 'eancom_DESADV_despatch_advice.edi'
ASSERT VALUE syntax_id = 'UNOA' WHERE df_file_name = 'eancom_PRICAT_price_catalogue.edi'
ASSERT VALUE msg_type = 'DESADV' WHERE df_file_name = 'eancom_DESADV_despatch_advice.edi'
ASSERT VALUE msg_type = 'IFTSTA' WHERE df_file_name = 'eancom_IFTSTA_transport_status.edi'
ASSERT VALUE msg_type = 'INVOIC' WHERE df_file_name = 'eancom_INVOIC_invoice.edi'
ASSERT VALUE msg_type = 'IFTMIN' WHERE df_file_name = 'eancom_instruction.edi'
SELECT
    df_file_name,
    unb_1 AS syntax_id,
    unh_2 AS msg_type,
    unh_1 AS msg_ref,
    unb_2 AS sender,
    unb_3 AS recipient
FROM {{zone_name}}.edi_demos.eancom_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Document Registry (BGM)
-- ============================================================================
-- The BGM (Beginning of Message) segment identifies the document type and
-- number. Every EANCOM message has a BGM segment. BGM_1 is the document
-- name code (e.g. 351=despatch advice, 380=invoice), BGM_2 is the unique
-- document reference number.

ASSERT ROW_COUNT = 6
ASSERT VALUE bgm_1 = '351' WHERE df_file_name = 'eancom_DESADV_despatch_advice.edi'
ASSERT VALUE bgm_1 = '380' WHERE df_file_name = 'eancom_INVOIC_invoice.edi'
ASSERT VALUE bgm_2 = 'PC32458' WHERE df_file_name = 'eancom_PRICAT_price_catalogue.edi'
ASSERT VALUE bgm_2 = 'DES587441' WHERE df_file_name = 'eancom_DESADV_despatch_advice.edi'
ASSERT VALUE bgm_2 = '569952' WHERE df_file_name = 'eancom_instruction.edi'
SELECT
    df_file_name,
    unh_2 AS msg_type,
    bgm_1,
    bgm_2
FROM {{zone_name}}.edi_demos.eancom_materialized
ORDER BY df_file_name;


-- ============================================================================
-- 3. Supplier & Customer Roles (NAD)
-- ============================================================================
-- The NAD (Name and Address) segment identifies trading partners by role.
-- NAD_1 is the party qualifier: BY=buyer, SU=supplier, DP=delivery party,
-- SH=shipper, FW=freight forwarder, CZ=consignor.
-- NAD_2 is the GLN (Global Location Number) / EAN-13 code.
-- NAD is a repeating segment (default mode=First, max=1), so the FIRST NAD
-- of each message is materialized. Groups by role to show how many messages
-- have each party type as their primary NAD.
--
-- First NAD per file (alphabetical):
--   DESADV=SU, IFTSTA=FW, INVOIC=BY, ORDRSP=BY, PRICAT=BY, IFTMIN=CZ
--   → BY=3, SU=1, FW=1, CZ=1 → 4 distinct roles

ASSERT ROW_COUNT = 4
ASSERT VALUE partner_count = 3 WHERE role = 'BY'
ASSERT VALUE partner_count = 1 WHERE role = 'SU'
ASSERT VALUE partner_count = 1 WHERE role = 'FW'
ASSERT VALUE partner_count = 1 WHERE role = 'CZ'
SELECT
    nad_1 AS role,
    COUNT(*) AS partner_count
FROM {{zone_name}}.edi_demos.eancom_materialized
GROUP BY nad_1
ORDER BY partner_count DESC;


-- ============================================================================
-- 4. Product Lines (LIN)
-- ============================================================================
-- The LIN (Line Item) segment identifies products by GTIN/EAN article number.
-- Only 4 of 6 message types contain line items: DESADV, INVOIC, ORDRSP,
-- PRICAT. Transport messages (IFTSTA, IFTMIN) have no LIN segments.
-- LIN_1 = line item number, LIN_3 = GTIN/EAN article number.

ASSERT ROW_COUNT = 4
ASSERT VALUE lin_3 = '5410738377131:SRV' WHERE df_file_name = 'eancom_PRICAT_price_catalogue.edi'
ASSERT VALUE lin_3 = '5410738000152:SRV' WHERE df_file_name = 'eancom_DESADV_despatch_advice.edi'
ASSERT VALUE lin_1 = '1' WHERE df_file_name = 'eancom_PRICAT_price_catalogue.edi'
ASSERT VALUE lin_1 = '1' WHERE df_file_name = 'eancom_INVOIC_invoice.edi'
SELECT
    df_file_name,
    unh_2 AS msg_type,
    lin_1,
    lin_3
FROM {{zone_name}}.edi_demos.eancom_materialized
WHERE lin_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 5. Price Catalogue (PRICAT)
-- ============================================================================
-- PRICAT messages carry product pricing and master data from suppliers.
-- This query filters for the PRICAT message and shows its BGM document
-- reference, line items with GTINs, and the supplier (NAD) identification.

ASSERT ROW_COUNT = 1
ASSERT VALUE bgm_2 = 'PC32458'
ASSERT VALUE lin_1 = '1'
ASSERT VALUE lin_3 = '5410738377131:SRV'
ASSERT VALUE nad_1 = 'BY'
ASSERT VALUE nad_2 = '5412345000020::9'
SELECT
    df_file_name,
    bgm_2,
    lin_1,
    lin_3,
    nad_1,
    nad_2
FROM {{zone_name}}.edi_demos.eancom_materialized
WHERE unh_2 = 'PRICAT'
ORDER BY df_file_name;


-- ============================================================================
-- 6. Despatch Tracking (DESADV)
-- ============================================================================
-- DESADV messages notify the retailer of incoming shipments. They include
-- CPS (Consignment Packing Sequence) for hierarchical packing structure
-- and QTY (Quantity) for item quantities. BGM_2 is the despatch advice
-- number, LIN_3 is the GTIN of shipped products.

ASSERT ROW_COUNT = 1
ASSERT VALUE bgm_2 = 'DES587441'
ASSERT VALUE cps_1 = '3'
ASSERT VALUE nad_1 = 'SU'
ASSERT VALUE lin_1 = '1'
ASSERT VALUE lin_3 = '5410738000152:SRV'
SELECT
    df_file_name,
    bgm_2,
    cps_1,
    qty_1,
    nad_1,
    lin_1,
    lin_3
FROM {{zone_name}}.edi_demos.eancom_materialized
WHERE unh_2 = 'DESADV'
ORDER BY df_file_name;


-- ============================================================================
-- 7. Shipment Status (IFTSTA)
-- ============================================================================
-- IFTSTA messages report transport status events. STS_1 is the status
-- event code ('1' = event reported). This query filters for the IFTSTA
-- message and shows its BGM reference and status code.

ASSERT ROW_COUNT = 1
ASSERT VALUE sts_1 = '1'
ASSERT VALUE bgm_2 = '95-455'
ASSERT VALUE nad_1 = 'FW'
ASSERT VALUE nad_2 = '5422331123459::9'
SELECT
    df_file_name,
    bgm_2,
    sts_1,
    nad_1,
    nad_2
FROM {{zone_name}}.edi_demos.eancom_materialized
WHERE unh_2 = 'IFTSTA'
ORDER BY df_file_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Automated verification that the demo loaded correctly.

ASSERT ROW_COUNT = 4
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'source_files_6'
ASSERT VALUE result = 'PASS' WHERE check_name = 'bgm_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
SELECT check_name, result FROM (

    -- Check 1: Exact total message count = 6 (one per file)
    SELECT 'message_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.eancom_messages) = 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 6 distinct source files in df_file_name
    SELECT 'source_files_6' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi_demos.eancom_messages) = 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: All 6 messages have BGM segments materialized
    SELECT 'bgm_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.eancom_materialized
                       WHERE bgm_1 IS NOT NULL AND bgm_2 IS NOT NULL) = 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: df_transaction_json is populated for all 6 messages
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.eancom_messages
                       WHERE df_transaction_json IS NOT NULL) = 6
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
