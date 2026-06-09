-- ============================================================================
-- EDI Order Lifecycle Tracking — Demo Queries
-- ============================================================================
-- Cross-document traceability queries that correlate different X12 transaction
-- types within a single unified EDI table. Unlike single-document-type queries,
-- these demonstrate how a finance team can trace purchase orders through the
-- complete order-to-cash lifecycle: PO creation (850) → acknowledgment (855)
-- → shipment (856/857) → invoicing (810) → receipt (861).
--
-- Table: lifecycle_tracking
--   One external table over all 14 EDI files with materialized fields from
--   every lifecycle-relevant segment type.
--
-- Column reference (always available — ISA/GS/ST envelope):
--   ST_1   = Transaction Set ID (850, 810, 855, 856, 857, 861, 824, 997)
--   ISA_6  = Interchange Sender ID
--   ISA_8  = Interchange Receiver ID
--
-- Materialized columns (lifecycle-specific):
--   BEG_1/3/5  = PO purpose, number, date (850 only)
--   BIG_1/2    = Invoice date, number (810 only)
--   BAK_1/3/4  = Ack status code, PO reference, ack date (855 only)
--   BSN_2/3    = Shipment ID, date (856 only)
--   BRA_1      = Receipt ID (861 only)
--   BGN_2      = Advice reference (824 only)
--   N1_1/2     = First party entity code, name (most types)
--   CTT_1      = Line item count (most types)
--   REF_1/2    = First reference qualifier, value (most types)
-- ============================================================================


-- ============================================================================
-- 1. Lifecycle Overview — All Documents by Type
-- ============================================================================
-- Maps every transaction to its lifecycle stage using a CASE on ST_1. The
-- document_id column uses COALESCE to pull the primary identifier from
-- whichever segment is present: BEG_3 for POs, BIG_2 for invoices, BSN_2
-- for ship notices, BAK_3 for PO acks, BRA_1 for receipts, BGN_2 for advice.
--
-- This is the core cross-document query: one SELECT shows the full lifecycle
-- of all 14 transactions with their stage, identifier, and source file.
--
-- What you'll see:
--   - lifecycle_stage:  Human-readable stage name (Order, Invoice, etc.)
--   - document_id:      Primary identifier from each document type
--   - txn_type:         Raw X12 transaction set ID
--   - df_file_name:     Source .edi file

ASSERT ROW_COUNT = 14
ASSERT VALUE lifecycle_stage = 'Order' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE document_id = '1000012' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE lifecycle_stage = 'Invoice' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE document_id = 'SG427254' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE lifecycle_stage = 'Acknowledgment' WHERE df_file_name = 'x12_855_purchase_order_ack.edi'
ASSERT VALUE document_id = '1234567' WHERE df_file_name = 'x12_855_purchase_order_ack.edi'
ASSERT VALUE lifecycle_stage = 'Shipment' WHERE df_file_name = 'x12_856_ship_notice.edi'
ASSERT VALUE lifecycle_stage = 'Receipt' WHERE df_file_name = 'x12_861_receiving_advice.edi'
SELECT
    CASE st_1
        WHEN '850' THEN 'Order'
        WHEN '855' THEN 'Acknowledgment'
        WHEN '856' THEN 'Shipment'
        WHEN '857' THEN 'Shipment & Billing'
        WHEN '810' THEN 'Invoice'
        WHEN '861' THEN 'Receipt'
        WHEN '997' THEN 'Acknowledgment (Func)'
        WHEN '824' THEN 'Application Advice'
        ELSE 'Other'
    END AS lifecycle_stage,
    COALESCE(beg_3, big_2, bsn_2, bak_3, bra_1, bgn_2) AS document_id,
    st_1 AS txn_type,
    df_file_name
FROM {{zone_name}}.edi_demos.lifecycle_tracking
ORDER BY
    CASE st_1
        WHEN '850' THEN 1
        WHEN '855' THEN 2
        WHEN '856' THEN 3
        WHEN '857' THEN 4
        WHEN '810' THEN 5
        WHEN '861' THEN 6
        WHEN '997' THEN 7
        WHEN '824' THEN 8
        ELSE 9
    END,
    df_file_name;


-- ============================================================================
-- 2. Lifecycle Stage Counts
-- ============================================================================
-- Groups transactions by lifecycle stage and counts how many documents exist
-- at each stage. This answers: "How many POs, invoices, shipments, etc. are
-- in the feed?" — a common dashboard metric for order-to-cash monitoring.
--
-- What you'll see:
--   - lifecycle_stage:  Human-readable stage name
--   - stage_count:      Number of transactions at that stage

ASSERT ROW_COUNT = 8
ASSERT VALUE stage_count = 5 WHERE lifecycle_stage = 'Invoice'
ASSERT VALUE stage_count = 3 WHERE lifecycle_stage = 'Order'
ASSERT VALUE stage_count = 1 WHERE lifecycle_stage = 'Acknowledgment'
ASSERT VALUE stage_count = 1 WHERE lifecycle_stage = 'Shipment'
ASSERT VALUE stage_count = 1 WHERE lifecycle_stage = 'Shipment & Billing'
ASSERT VALUE stage_count = 1 WHERE lifecycle_stage = 'Receipt'
ASSERT VALUE stage_count = 1 WHERE lifecycle_stage = 'Application Advice'
ASSERT VALUE stage_count = 1 WHERE lifecycle_stage = 'Acknowledgment (Func)'
SELECT
    CASE st_1
        WHEN '850' THEN 'Order'
        WHEN '855' THEN 'Acknowledgment'
        WHEN '856' THEN 'Shipment'
        WHEN '857' THEN 'Shipment & Billing'
        WHEN '810' THEN 'Invoice'
        WHEN '861' THEN 'Receipt'
        WHEN '997' THEN 'Acknowledgment (Func)'
        WHEN '824' THEN 'Application Advice'
        ELSE 'Other'
    END AS lifecycle_stage,
    COUNT(*) AS stage_count
FROM {{zone_name}}.edi_demos.lifecycle_tracking
GROUP BY st_1
ORDER BY stage_count DESC, st_1;


-- ============================================================================
-- 3. Purchase Order Detail
-- ============================================================================
-- Filters to 850 (Purchase Order) transactions and shows key business fields
-- from the BEG segment: PO number, date, and the first trading partner name.
-- The REF segment provides additional reference data (e.g., SR=Supplier
-- Reference, IA=Internal Account).
--
-- What you'll see:
--   - df_file_name:  Source file
--   - po_number:     Purchase order number from BEG-03
--   - po_date:       Purchase order date from BEG-05 (YYYYMMDD)
--   - party_code:    First N1 entity code (ST=Ship To, BY=Buyer)
--   - party_name:    First N1 party name
--   - ref_type:      First REF qualifier code
--   - ref_value:     First REF value
--   - line_items:    Number of line items from CTT-01

ASSERT ROW_COUNT = 3
ASSERT VALUE po_number = '1000012' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po_date = '20090827' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_code = 'ST' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE party_name = 'John Doe' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE line_items = '1' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE po_number = '4600000406' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po_date = '20140724' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE party_name = 'Transplace Laredo' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE line_items = '3' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE po_number = 'XX-1234' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE po_date = '20170301' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE party_code = 'BY' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE party_name = 'ABC AEROSPACE' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
SELECT
    df_file_name,
    beg_3 AS po_number,
    beg_5 AS po_date,
    n1_1 AS party_code,
    n1_2 AS party_name,
    ref_1 AS ref_type,
    ref_2 AS ref_value,
    ctt_1 AS line_items
FROM {{zone_name}}.edi_demos.lifecycle_tracking
WHERE st_1 = '850'
ORDER BY df_file_name;


-- ============================================================================
-- 4. Invoice Detail
-- ============================================================================
-- Filters to 810 (Invoice) transactions and shows key business fields from
-- the BIG segment: invoice date, invoice number, and first trading partner.
-- The REF segment shows reference qualifiers (VN=Vendor Number, IV=Invoice,
-- AP=Accounts Payable).
--
-- What you'll see:
--   - df_file_name:    Source file
--   - invoice_number:  Invoice identifier from BIG-02
--   - invoice_date:    Invoice date from BIG-01 (YYYYMMDD)
--   - party_name:      First N1 party name
--   - ref_type:        First REF qualifier code
--   - ref_value:       First REF value
--   - line_items:      Number of line items from CTT-01

ASSERT ROW_COUNT = 5
ASSERT VALUE invoice_number = 'DO091003TESTINV01' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE invoice_date = '20030310' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE party_name = 'Aaron Copeland' WHERE df_file_name = 'x12_810_invoice_a.edi'
ASSERT VALUE invoice_number = 'SG427254' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE invoice_date = '20000513' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE party_name = 'ABC AEROSPACE CORPORATION' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
SELECT
    df_file_name,
    big_2 AS invoice_number,
    big_1 AS invoice_date,
    n1_2 AS party_name,
    ref_1 AS ref_type,
    ref_2 AS ref_value,
    ctt_1 AS line_items
FROM {{zone_name}}.edi_demos.lifecycle_tracking
WHERE st_1 = '810'
ORDER BY df_file_name;


-- ============================================================================
-- 5. Shipment & Fulfillment
-- ============================================================================
-- Filters to the fulfillment stage: 856 (Ship Notice), 857 (Shipment &
-- Billing), and 861 (Receiving Advice). Each document type contributes
-- different identifiers — BSN_2 for shipment ID, BRA_1 for receipt ID.
--
-- This query shows how one table unifies the fulfillment chain: shipment
-- notification → combined ship/bill → receiving confirmation.
--
-- What you'll see:
--   - df_file_name:     Source file
--   - txn_type:         856, 857, or 861
--   - lifecycle_stage:  Shipment, Shipment & Billing, or Receipt
--   - shipment_id:      BSN_2 (856 only)
--   - shipment_date:    BSN_3 (856 only)
--   - receipt_id:       BRA_1 (861 only)
--   - party_code:       First N1 entity code
--   - party_name:       First N1 party name

ASSERT ROW_COUNT = 3
ASSERT VALUE lifecycle_stage = 'Shipment' WHERE df_file_name = 'x12_856_ship_notice.edi'
ASSERT VALUE txn_type = '856' WHERE df_file_name = 'x12_856_ship_notice.edi'
ASSERT VALUE shipment_id = '01140824' WHERE df_file_name = 'x12_856_ship_notice.edi'
ASSERT VALUE shipment_date = '20051015' WHERE df_file_name = 'x12_856_ship_notice.edi'
ASSERT VALUE party_name = 'WAL-MART DC 6094J-JIT' WHERE df_file_name = 'x12_856_ship_notice.edi'
ASSERT VALUE lifecycle_stage = 'Shipment & Billing' WHERE df_file_name = 'x12_856_ship_bill_notice.edi'
ASSERT VALUE txn_type = '857' WHERE df_file_name = 'x12_856_ship_bill_notice.edi'
ASSERT VALUE shipment_id IS NULL WHERE df_file_name = 'x12_856_ship_bill_notice.edi'
ASSERT VALUE lifecycle_stage = 'Receipt' WHERE df_file_name = 'x12_861_receiving_advice.edi'
ASSERT VALUE txn_type = '861' WHERE df_file_name = 'x12_861_receiving_advice.edi'
ASSERT VALUE receipt_id = 'C000548241' WHERE df_file_name = 'x12_861_receiving_advice.edi'
SELECT
    df_file_name,
    st_1 AS txn_type,
    CASE st_1
        WHEN '856' THEN 'Shipment'
        WHEN '857' THEN 'Shipment & Billing'
        WHEN '861' THEN 'Receipt'
    END AS lifecycle_stage,
    bsn_2 AS shipment_id,
    bsn_3 AS shipment_date,
    bra_1 AS receipt_id,
    n1_1 AS party_code,
    n1_2 AS party_name
FROM {{zone_name}}.edi_demos.lifecycle_tracking
WHERE st_1 IN ('856', '857', '861')
ORDER BY
    CASE st_1
        WHEN '856' THEN 1
        WHEN '857' THEN 2
        WHEN '861' THEN 3
    END;


-- ============================================================================
-- 6. Document Timeline
-- ============================================================================
-- Shows all documents that have a date field, ordered chronologically. The
-- document date is extracted from whichever segment is present: BEG_5 for POs,
-- BIG_1 for invoices, BSN_3 for ship notices, BAK_4 for PO acks.
--
-- This reveals how the lifecycle progresses over time: orders placed, then
-- acknowledged, shipped, invoiced. Documents without dates (997, 824, 857,
-- 861) are excluded.
--
-- What you'll see:
--   - document_date:    The business date from the relevant segment
--   - lifecycle_stage:  Stage in the order-to-cash cycle
--   - document_id:      Primary document identifier
--   - df_file_name:     Source file

ASSERT ROW_COUNT = 10
ASSERT VALUE document_date = '20090827' WHERE df_file_name = 'x12_850_purchase_order.edi'
ASSERT VALUE document_date = '20140724' WHERE df_file_name = 'x12_850_purchase_order_a.edi'
ASSERT VALUE document_date = '20170301' WHERE df_file_name = 'x12_850_purchase_order_edifabric.edi'
ASSERT VALUE document_date = '20050102' WHERE df_file_name = 'x12_855_purchase_order_ack.edi'
ASSERT VALUE document_date = '20051015' WHERE df_file_name = 'x12_856_ship_notice.edi'
ASSERT VALUE document_date = '20000513' WHERE df_file_name = 'x12_810_invoice_edifabric.edi'
ASSERT VALUE document_date = '20030310' WHERE df_file_name = 'x12_810_invoice_a.edi'
SELECT
    COALESCE(beg_5, big_1, bsn_3, bak_4) AS document_date,
    CASE st_1
        WHEN '850' THEN 'Order'
        WHEN '855' THEN 'Acknowledgment'
        WHEN '856' THEN 'Shipment'
        WHEN '857' THEN 'Shipment & Billing'
        WHEN '810' THEN 'Invoice'
        WHEN '861' THEN 'Receipt'
        WHEN '997' THEN 'Acknowledgment (Func)'
        WHEN '824' THEN 'Application Advice'
        ELSE 'Other'
    END AS lifecycle_stage,
    COALESCE(beg_3, big_2, bsn_2, bak_3, bra_1, bgn_2) AS document_id,
    df_file_name
FROM {{zone_name}}.edi_demos.lifecycle_tracking
WHERE COALESCE(beg_5, big_1, bsn_3, bak_4) IS NOT NULL
ORDER BY COALESCE(beg_5, big_1, bsn_3, bak_4);


-- ============================================================================
-- 7. Trading Partner Activity Across Lifecycle Stages
-- ============================================================================
-- Groups documents by their first party name (N1_2) and shows which lifecycle
-- stages each trading partner participates in. This answers: "Which partners
-- appear across multiple stages of the order-to-cash cycle?"
--
-- Partners appearing in multiple stages indicate deeper trading relationships.
-- Partners in only one stage may be specialized (e.g., shipping only).
--
-- What you'll see:
--   - party_name:       First N1 party name
--   - party_code:       First N1 entity code (ST, BY, SO, SF, SU)
--   - stages:           Comma-separated list of lifecycle stages
--   - document_count:   Total documents for this party

ASSERT ROW_COUNT = 7
ASSERT VALUE document_count = 4 WHERE party_name = 'Aaron Copeland'
ASSERT VALUE party_code = 'SO' WHERE party_name = 'Aaron Copeland'
ASSERT VALUE document_count = 1 WHERE party_name = 'ABC AEROSPACE'
ASSERT VALUE party_code = 'BY' WHERE party_name = 'ABC AEROSPACE'
ASSERT VALUE document_count = 1 WHERE party_name = 'WAL-MART DC 6094J-JIT'
ASSERT VALUE party_code = 'ST' WHERE party_name = 'WAL-MART DC 6094J-JIT'
ASSERT VALUE document_count = 1 WHERE party_name = 'XYZ MANUFACTURING CO'
ASSERT VALUE party_code = 'SF' WHERE party_name = 'XYZ MANUFACTURING CO'
SELECT
    n1_2 AS party_name,
    n1_1 AS party_code,
    string_agg(DISTINCT
        CASE st_1
            WHEN '850' THEN 'Order'
            WHEN '855' THEN 'Acknowledgment'
            WHEN '856' THEN 'Shipment'
            WHEN '857' THEN 'Shipment & Billing'
            WHEN '810' THEN 'Invoice'
            WHEN '861' THEN 'Receipt'
            WHEN '997' THEN 'Acknowledgment (Func)'
            WHEN '824' THEN 'Application Advice'
            ELSE 'Other'
        END, ', '
    ) AS stages,
    COUNT(*) AS document_count
FROM {{zone_name}}.edi_demos.lifecycle_tracking
WHERE n1_2 IS NOT NULL AND n1_2 <> ''
GROUP BY n1_2, n1_1
ORDER BY document_count DESC, party_name;


-- ============================================================================
-- 8. VERIFY — All Checks
-- ============================================================================
-- Automated pass/fail verification that the lifecycle tracking table loaded
-- correctly and cross-document queries produce expected results.

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = 'all_850s_have_beg3'
ASSERT VALUE result = 'PASS' WHERE check_name = 'all_810s_have_big2'
ASSERT VALUE result = 'PASS' WHERE check_name = 'lifecycle_has_14_rows'
ASSERT VALUE result = 'PASS' WHERE check_name = 'eight_txn_types'
ASSERT VALUE result = 'PASS' WHERE check_name = 'fulfillment_has_3_docs'
SELECT check_name, result FROM (

    -- Check 1: All 14 transactions loaded into the unified table
    SELECT 'lifecycle_has_14_rows' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.lifecycle_tracking) = 14
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 8 distinct transaction types present
    SELECT 'eight_txn_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT st_1) FROM {{zone_name}}.edi_demos.lifecycle_tracking) = 8
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: All 850s have BEG_3 (PO number) populated
    SELECT 'all_850s_have_beg3' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.lifecycle_tracking
                       WHERE st_1 = '850' AND (beg_3 IS NULL OR beg_3 = '')) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: All 810s have BIG_2 (invoice number) populated
    SELECT 'all_810s_have_big2' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.lifecycle_tracking
                       WHERE st_1 = '810' AND (big_2 IS NULL OR big_2 = '')) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Fulfillment stage has exactly 3 documents (856 + 857 + 861)
    SELECT 'fulfillment_has_3_docs' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.lifecycle_tracking
                       WHERE st_1 IN ('856', '857', '861')) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
