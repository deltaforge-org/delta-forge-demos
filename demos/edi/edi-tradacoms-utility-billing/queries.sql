-- ============================================================================
-- EDI TRADACOMS — UK Energy Billing & VAT Reconciliation — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge extracts and reconciles utility billing
-- data from TRADACOMS UTLHDR/UTLBIL/UVATLR/UTLTLR messages.
--
-- Two files are ingested:
--   tradacoms_utility_bill.edi         — Standard utility bill (4 messages)
--   tradacoms_utility_bill_escape.edi  — Same bill, escape chars in customer name
--
-- Two tables are available:
--   tradacoms_bills        — Compact view: STX/MHD headers + TYP/SDT/CDT
--   tradacoms_bill_details — Materialized: BCD, CCD, VAT, BTL, VTS, TTL fields
--
-- Billing field reference:
--   BCD  — Billing Control Detail (date, account, period)
--   CCD  — Charge Calculation Detail (tariff, volume, rates)
--   VAT  — VAT line (rate, net, vat amount, gross)
--   BTL  — Bill Total (charges, vat, total)
--   VTS  — VAT Summary (net, vat, gross per VAT code)
--   TTL  — Transmission Total (net, vat, gross across all bills)
-- ============================================================================


-- ============================================================================
-- 1. Billing Envelope Overview
-- ============================================================================
-- Lists all 8 messages across both files with their message type and sender.
-- Each file contains the same 4-message TRADACOMS structure:
--   UTLHDR:3 — Utility header (trading partner identification)
--   UTLBIL:3 — Utility bill (charges, VAT, totals)
--   UVATLR:3 — VAT trailer (VAT summaries by code)
--   UTLTLR:3 — Utility trailer (transmission totals)
--
-- What you'll see:
--   - source_file:  Which .edi file the message came from
--   - msg_ref:      Message reference number (1-4 within each file)
--   - msg_type:     Message type and version
--   - sender:       STX sender identification

ASSERT ROW_COUNT = 8
ASSERT VALUE msg_type = 'UTLHDR:3' WHERE source_file = 'tradacoms_utility_bill.edi' AND msg_ref = '1'
ASSERT VALUE msg_type = 'UTLBIL:3' WHERE source_file = 'tradacoms_utility_bill.edi' AND msg_ref = '2'
ASSERT VALUE msg_type = 'UVATLR:3' WHERE source_file = 'tradacoms_utility_bill.edi' AND msg_ref = '3'
ASSERT VALUE msg_type = 'UTLTLR:3' WHERE source_file = 'tradacoms_utility_bill.edi' AND msg_ref = '4'
ASSERT VALUE msg_type = 'UTLHDR:3' WHERE source_file = 'tradacoms_utility_bill_escape.edi' AND msg_ref = '1'
ASSERT VALUE msg_type = 'UTLBIL:3' WHERE source_file = 'tradacoms_utility_bill_escape.edi' AND msg_ref = '2'
ASSERT VALUE msg_type = 'UVATLR:3' WHERE source_file = 'tradacoms_utility_bill_escape.edi' AND msg_ref = '3'
ASSERT VALUE msg_type = 'UTLTLR:3' WHERE source_file = 'tradacoms_utility_bill_escape.edi' AND msg_ref = '4'
ASSERT VALUE sender = '1011101000000:SOME ELECTRIC COMPANY PLC' WHERE source_file = 'tradacoms_utility_bill.edi' AND msg_ref = '1'
SELECT df_file_name AS source_file,
       mhd_1 AS msg_ref,
       mhd_2 AS msg_type,
       stx_2 AS sender
FROM {{zone_name}}.edi_demos.tradacoms_bills
ORDER BY df_file_name, mhd_1;


-- ============================================================================
-- 2. Customer Identification & Escape Handling
-- ============================================================================
-- Compares customer names from both files to demonstrate TRADACOMS escape
-- character decoding. The standard file has a plain customer name; the escape
-- file uses ?', ?+, and ?? to encode special characters:
--   ?'  → '  (apostrophe, normally the segment terminator)
--   ?+  → +  (plus sign, normally the element separator)
--   ??  → ?  (literal question mark)
--
-- What you'll see:
--   - Standard file:  "SOME CLIENT"
--   - Escape file:    "GEORGE'S FRIED CHIKEN + SONS. Could be the best chicken yet?"

ASSERT ROW_COUNT = 2
ASSERT VALUE customer_name = 'SOME CLIENT' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE customer_name = 'GEORGE''S FRIED CHIKEN + SONS. Could be the best chicken yet?' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE supplier_name = 'SITE 1' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE supplier_name = 'SITE 1' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
SELECT df_file_name AS source_file,
       sdt_2 AS supplier_name,
       cdt_2 AS customer_name
FROM {{zone_name}}.edi_demos.tradacoms_bills
WHERE mhd_2 = 'UTLHDR:3'
ORDER BY df_file_name;


-- ============================================================================
-- 3. Billing Period & Account Details
-- ============================================================================
-- Extracts BCD (Billing Control Detail) fields from UTLBIL messages. BCD
-- contains the core billing metadata: when the bill was produced, which
-- account it applies to, and the consumption period covered.
--
-- What you'll see:
--   - billing_date:    "141218" (18 Dec 2014 in YYMMDD format)
--   - account_number:  "0614438000016" (customer account reference)
--   - billing_period:  "140905:141204" (5 Sep 2014 to 4 Dec 2014)

ASSERT ROW_COUNT = 2
ASSERT VALUE billing_date = '141218' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE account_number = '0614438000016' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE billing_period = '140905:141204' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE billing_date = '141218' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE account_number = '0614438000016' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE billing_period = '140905:141204' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
SELECT df_file_name AS source_file,
       bcd_1 AS billing_date,
       bcd_3 AS account_number,
       bcd_8 AS billing_period
FROM {{zone_name}}.edi_demos.tradacoms_bill_details
WHERE mhd_2 = 'UTLBIL:3'
ORDER BY df_file_name;


-- ============================================================================
-- 4. Charge Calculation Details (CCD)
-- ============================================================================
-- Extracts tariff and volume information from CCD (Charge Calculation Detail)
-- segments. Each UTLBIL contains 3 CCD lines representing different charge
-- components (metered consumption, standing charges, etc.).
--
-- Note: When multiple CCD segments exist in one message, materialized_paths
-- retains the last non-empty value for each element. Since all three CCDs
-- share the same tariff description and CCD #1 is the only one with volume
-- data in element 12, the values shown reflect the effective extraction.
--
-- What you'll see:
--   - tariff:  "CCCCC:DOMESTIC RESTRICTED" (composite: code + description)
--   - volume:  "1770000:KWH" (composite: quantity + unit, from CCD #1)

ASSERT ROW_COUNT = 2
ASSERT VALUE tariff = 'CCCCC:DOMESTIC RESTRICTED' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE volume = '1770000:KWH' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE tariff = 'CCCCC:DOMESTIC RESTRICTED' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE volume = '1770000:KWH' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
SELECT df_file_name AS source_file,
       ccd_3 AS tariff,
       ccd_12 AS volume
FROM {{zone_name}}.edi_demos.tradacoms_bill_details
WHERE mhd_2 = 'UTLBIL:3'
ORDER BY df_file_name;


-- ============================================================================
-- 5. VAT Summary
-- ============================================================================
-- Extracts VAT segment fields from the UTLBIL message. The VAT segment
-- provides the tax calculation breakdown: rate code, net charges before tax,
-- the VAT amount itself, and the gross total including tax.
--
-- What you'll see:
--   - vat_rate:      "91" (VAT rate code — maps to the applicable rate)
--   - net_amount:    "11398" (net charges in pence: GBP 113.98)
--   - vat_amount:    "569" (VAT in pence: GBP 5.69)
--   - gross_amount:  "11967" (gross total in pence: GBP 119.67)
--
-- Reconciliation check: 11398 + 569 = 11967 (net + vat = gross)

ASSERT ROW_COUNT = 2
ASSERT VALUE vat_rate = '91' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE net_amount = '11398' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE vat_amount = '569' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE gross_amount = '11967' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE vat_rate = '91' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE net_amount = '11398' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE vat_amount = '569' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE gross_amount = '11967' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
SELECT df_file_name AS source_file,
       vat_2 AS vat_rate,
       vat_6 AS net_amount,
       vat_7 AS vat_amount,
       vat_8 AS gross_amount
FROM {{zone_name}}.edi_demos.tradacoms_bill_details
WHERE mhd_2 = 'UTLBIL:3'
ORDER BY df_file_name;


-- ============================================================================
-- 6. Bill Total Reconciliation
-- ============================================================================
-- Extracts BTL (Bill Total) segment fields. The BTL segment summarizes the
-- bill: total charges, total VAT, and the final bill amount. These values
-- should reconcile with the VAT segment: BTL charges = VAT net, BTL vat =
-- VAT amount, BTL total = VAT gross.
--
-- What you'll see:
--   - total_charges:  "11398" (matches VAT net_amount)
--   - total_vat:      "569" (matches VAT vat_amount)
--   - bill_total:     "11967" (matches VAT gross_amount)

ASSERT ROW_COUNT = 2
ASSERT VALUE total_charges = '11398' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE total_vat = '569' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE bill_total = '11967' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE total_charges = '11398' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE total_vat = '569' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE bill_total = '11967' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
SELECT df_file_name AS source_file,
       btl_2 AS total_charges,
       btl_3 AS total_vat,
       btl_5 AS bill_total
FROM {{zone_name}}.edi_demos.tradacoms_bill_details
WHERE mhd_2 = 'UTLBIL:3'
ORDER BY df_file_name;


-- ============================================================================
-- 7. Transmission Totals (UTLTLR)
-- ============================================================================
-- Extracts TTL (Transmission Total) from the UTLTLR trailer message. TTL
-- provides the file-level totals that aggregate all bills within the
-- transmission. In a multi-bill file, these would sum across all UTLBIL
-- messages; here each file contains a single bill.
--
-- What you'll see:
--   - total_net:    "3090963" (total net in pence: GBP 30,909.63)
--   - total_vat:    "154343" (total VAT in pence: GBP 1,543.43)
--   - total_gross:  "3245306" (total gross in pence: GBP 32,453.06)
--
-- Note: TTL values are larger than the single bill because they represent
-- cumulative counters across the supplier's billing run.

ASSERT ROW_COUNT = 2
ASSERT VALUE total_net = '3090963' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE total_vat = '154343' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE total_gross = '3245306' WHERE source_file = 'tradacoms_utility_bill.edi'
ASSERT VALUE total_net = '3090963' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE total_vat = '154343' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
ASSERT VALUE total_gross = '3245306' WHERE source_file = 'tradacoms_utility_bill_escape.edi'
SELECT df_file_name AS source_file,
       ttl_1 AS total_net,
       ttl_2 AS total_vat,
       ttl_5 AS total_gross
FROM {{zone_name}}.edi_demos.tradacoms_bill_details
WHERE mhd_2 = 'UTLTLR:3'
ORDER BY df_file_name;


-- ============================================================================
-- 8. Cross-File Bill Comparison
-- ============================================================================
-- Compares bill totals between the two files. Since the escape-character file
-- contains identical billing data (only the customer name differs), the BTL
-- totals should match across both files. This verifies that escape-character
-- decoding does not affect numeric billing fields.
--
-- What you'll see:
--   - file_count:       2 (both files loaded)
--   - matching_totals:  "YES" (both files have the same BTL_5 value)
--   - bill_total:       "11967" (the common bill total)

ASSERT ROW_COUNT = 1
ASSERT VALUE file_count = 2
ASSERT VALUE matching_totals = 'YES'
SELECT COUNT(DISTINCT df_file_name) AS file_count,
       CASE WHEN COUNT(DISTINCT btl_5) = 1 THEN 'YES' ELSE 'NO' END AS matching_totals,
       MIN(btl_5) AS bill_total
FROM {{zone_name}}.edi_demos.tradacoms_bill_details
WHERE mhd_2 = 'UTLBIL:3';


-- ============================================================================
-- VERIFY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly and
-- billing-specific fields are properly extracted.
-- All checks should return PASS.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count_8'
ASSERT VALUE result = 'PASS' WHERE check_name = 'utlbil_count_2'
ASSERT VALUE result = 'PASS' WHERE check_name = 'vat_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'totals_match'
ASSERT VALUE result = 'PASS' WHERE check_name = 'vat_reconcile'
ASSERT VALUE result = 'PASS' WHERE check_name = 'ttl_reconcile'
SELECT check_name, result FROM (

    -- Check 1: Total message count = 8 (4 per file x 2 files)
    SELECT 'message_count_8' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_bills) = 8
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Exactly 2 UTLBIL messages (one per file)
    SELECT 'utlbil_count_2' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_bill_details
                       WHERE mhd_2 = 'UTLBIL:3') = 2
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: VAT fields are populated for UTLBIL rows
    SELECT 'vat_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_bill_details
                       WHERE mhd_2 = 'UTLBIL:3'
                         AND vat_6 IS NOT NULL AND vat_6 <> ''
                         AND vat_7 IS NOT NULL AND vat_7 <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Bill totals match across both files
    SELECT 'totals_match' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT btl_5)
                       FROM {{zone_name}}.edi_demos.tradacoms_bill_details
                       WHERE mhd_2 = 'UTLBIL:3') = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: VAT reconciliation — net + vat = gross for every UTLBIL
    SELECT 'vat_reconcile' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_bill_details
                       WHERE mhd_2 = 'UTLBIL:3'
                         AND CAST(vat_6 AS BIGINT) + CAST(vat_7 AS BIGINT) = CAST(vat_8 AS BIGINT)) = 2
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: TTL reconciliation — ttl_net + ttl_vat = ttl_gross for every UTLTLR
    SELECT 'ttl_reconcile' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_bill_details
                       WHERE mhd_2 = 'UTLTLR:3'
                         AND CAST(ttl_1 AS BIGINT) + CAST(ttl_2 AS BIGINT) = CAST(ttl_5 AS BIGINT)) = 2
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
