-- ============================================================================
-- EDI TRADACOMS — Purchase Order Line Items — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge extracts order line items from a
-- TRADACOMS purchase order file. The file contains 4 MHD messages following
-- the Header-Detail-Trailer pattern:
--   ORDHDR:9  — Order header (trading partners, document type)
--   ORDERS:9  — Order detail x2 (line items, delivery, location)
--   ORDTLR:9  — Order trailer (file-level totals)
--
-- Two tables are available:
--   tradacoms_order_compact — Default: STX/MHD headers + full JSON
--   tradacoms_order_lines   — Enriched: OLD, ORD, DIN, OTR fields extracted
--
-- The file represents a UK grocery retailer (ANY SHOP PLC) sending
-- purchase orders to their supplier (XYZ MANUFACTURING PLC) via TRADACOMS.
-- Order 981 has 3 product lines; Order 982 has 2 product lines.
-- NOTE: materialized_paths extracts the last occurrence of repeating segments
-- (e.g., OLD), so only the final line per ORDERS message is visible.
--
-- Column reference (tradacoms_order_lines materialized columns):
--   OLD_1  = Line number        OLD_2  = Product EAN-13 code
--   OLD_3  = Supplier code      OLD_5  = Quantity ordered
--   OLD_6  = Unit price          OLD_10 = Product description
--   ORD_1  = Order reference     CLO_1  = Customer location code
--   DIN_1  = Delivery date       DIN_4  = Delivery instruction
--   OTR_1  = Declared line count OTR_2  = Declared total
--   TYP_1  = Type code           TYP_2  = Type description
--   SDT_2  = Supplier name       CDT_2  = Customer name
-- ============================================================================


-- ============================================================================
-- 1. Order Envelope Overview
-- ============================================================================
-- Shows all 4 MHD messages in the TRADACOMS file, demonstrating the
-- Header-Detail-Trailer pattern. ORDHDR comes first, then one or more
-- ORDERS detail messages, and finally ORDTLR closes the transmission.
--
-- What you'll see:
--   - msg_ref:    MHD_1 — sequential message number (1-4)
--   - msg_type:   MHD_2 — ORDHDR:9, ORDERS:9 (x2), ORDTLR:9
--   - sender:     STX_2 — the retailer sending the order
--   - receiver:   STX_3 — the supplier receiving the order

ASSERT ROW_COUNT = 4
ASSERT VALUE msg_type = 'ORDHDR:9' WHERE msg_ref = '1'
ASSERT VALUE msg_type = 'ORDERS:9' WHERE msg_ref = '2'
ASSERT VALUE msg_type = 'ORDERS:9' WHERE msg_ref = '3'
ASSERT VALUE msg_type = 'ORDTLR:9' WHERE msg_ref = '4'
SELECT
    mhd_1 AS msg_ref,
    mhd_2 AS msg_type,
    stx_2 AS sender,
    stx_3 AS receiver
FROM {{zone_name}}.edi_demos.tradacoms_order_compact
ORDER BY mhd_1;


-- ============================================================================
-- 2. Trading Partner Details
-- ============================================================================
-- Extracts supplier and customer names from the ORDHDR message using the
-- materialized table. The ORDHDR contains TYP (transaction type), SDT
-- (supplier details), and CDT (customer details) segments.
--
-- What you'll see:
--   - typ_code:       TYP_1 — "0430" = purchase order transaction
--   - typ_desc:       TYP_2 — "NEW-ORDERS" transaction sub-type
--   - supplier_name:  SDT_2 — the supplier receiving the order
--   - customer_name:  CDT_2 — the retailer placing the order

ASSERT ROW_COUNT = 1
ASSERT VALUE supplier_name = 'XYZ MANUFACTURING PLC'
ASSERT VALUE customer_name = 'ANY SHOP PLC'
ASSERT VALUE typ_code = '0430'
ASSERT VALUE typ_desc = 'NEW-ORDERS'
SELECT
    typ_1 AS typ_code,
    typ_2 AS typ_desc,
    sdt_2 AS supplier_name,
    cdt_2 AS customer_name
FROM {{zone_name}}.edi_demos.tradacoms_order_lines
WHERE mhd_2 = 'ORDHDR:9';


-- ============================================================================
-- 3. Order Line Items — Last Line Per Order
-- ============================================================================
-- Extracts OLD (Order Line Detail) segments from ORDERS messages. Because
-- materialized_paths extracts the last occurrence of repeating segments,
-- only the final OLD segment per ORDERS message survives. Order 981 shows
-- OLD 3 (PRODUCT C); Order 982 shows OLD 2 (PRODUCT L).
--
-- What you'll see:
--   - order_ref:      ORD_1 — order reference (composite: number::date)
--   - line_num:       OLD_1 — line number (last line in the order)
--   - product_ean:    OLD_2 — EAN-13 product barcode
--   - supplier_code:  OLD_3 — supplier's internal product code
--   - quantity:       OLD_5 — quantity ordered
--   - unit_price:     OLD_6 — price per unit
--   - description:    OLD_10 — product description text

ASSERT ROW_COUNT = 2
ASSERT VALUE product_ean = '5000100154073' WHERE order_ref = '981::940321'
ASSERT VALUE description = 'PRODUCT C' WHERE order_ref = '981::940321'
ASSERT VALUE quantity = '6' WHERE order_ref = '982::940321'
ASSERT VALUE description = 'PRODUCT L' WHERE order_ref = '982::940321'
SELECT
    ord_1 AS order_ref,
    old_1 AS line_num,
    old_2 AS product_ean,
    old_3 AS supplier_code,
    old_5 AS quantity,
    old_6 AS unit_price,
    old_10 AS description
FROM {{zone_name}}.edi_demos.tradacoms_order_lines
WHERE mhd_2 = 'ORDERS:9' AND old_1 IS NOT NULL
ORDER BY ord_1, old_1;


-- ============================================================================
-- 4. Order Value Calculation
-- ============================================================================
-- Calculates the total value of each order from the last OLD segment per
-- ORDERS message (materialized_paths extracts the last occurrence of
-- repeating segments). This demonstrates CAST-based arithmetic on
-- TRADACOMS string fields.
--
-- What you'll see:
--   - order_ref:   ORD_1 — the order reference
--   - order_total: Calculated (quantity * unit_price) for the last line
--
-- Order 981: 4*30 = 120 (last line: OLD 3, PRODUCT C)
-- Order 982: 6*8  = 48  (last line: OLD 2, PRODUCT L)

ASSERT ROW_COUNT = 2
ASSERT VALUE order_total = 120 WHERE order_ref = '981::940321'
ASSERT VALUE order_total = 48 WHERE order_ref = '982::940321'
SELECT
    ord_1 AS order_ref,
    SUM(CAST(old_5 AS INTEGER) * CAST(old_6 AS INTEGER)) AS order_total
FROM {{zone_name}}.edi_demos.tradacoms_order_lines
WHERE mhd_2 = 'ORDERS:9' AND old_1 IS NOT NULL
GROUP BY ord_1
ORDER BY ord_1;


-- ============================================================================
-- 5. Delivery Instructions
-- ============================================================================
-- Extracts DIN (Delivery Instruction) segments from ORDERS messages. The
-- DIN segment contains delivery date and special instructions. Note that
-- only the first order (981) includes a DIN segment with delivery details;
-- the second order (982) has no DIN.
--
-- What you'll see:
--   - order_ref:      ORD_1 — the order reference
--   - delivery_date:  DIN_1 — requested delivery date (YYMMDD)
--   - instruction:    DIN_4 — special delivery instruction text

ASSERT ROW_COUNT = 1
ASSERT VALUE delivery_date = '940328'
ASSERT VALUE instruction = 'RING BEFORE DELIVERY'
SELECT
    ord_1 AS order_ref,
    din_1 AS delivery_date,
    din_4 AS instruction
FROM {{zone_name}}.edi_demos.tradacoms_order_lines
WHERE mhd_2 = 'ORDERS:9' AND din_1 IS NOT NULL
ORDER BY ord_1;


-- ============================================================================
-- 6. Customer Locations
-- ============================================================================
-- Extracts CLO (Customer Location) codes from ORDERS messages. Each order
-- specifies a delivery location using an EAN-coded composite identifier.
-- These codes identify specific retail outlets or warehouses.
--
-- What you'll see:
--   - order_ref:      ORD_1 — the order reference
--   - location_code:  CLO_1 — EAN location composite (EAN::internal code)

ASSERT ROW_COUNT = 2
ASSERT VALUE location_code = '5000600003240::68322' WHERE order_ref = '981::940321'
ASSERT VALUE location_code = '5000600003282::68347' WHERE order_ref = '982::940321'
SELECT
    ord_1 AS order_ref,
    clo_1 AS location_code
FROM {{zone_name}}.edi_demos.tradacoms_order_lines
WHERE mhd_2 = 'ORDERS:9' AND clo_1 IS NOT NULL
ORDER BY ord_1;


-- ============================================================================
-- 7. Order Trailer Reconciliation
-- ============================================================================
-- Extracts OTR (Order Trailer) segments that declare the expected line
-- count and total for each order. Supply chain teams use these for
-- reconciliation — comparing declared totals against actual line counts.
--
-- What you'll see:
--   - order_ref:       ORD_1 — the order reference
--   - declared_lines:  OTR_1 — expected number of OLD line items
--   - declared_total:  OTR_2 — declared order total from the trailer

ASSERT ROW_COUNT = 2
ASSERT VALUE declared_lines = '3' WHERE order_ref = '981::940321'
ASSERT VALUE declared_lines = '2' WHERE order_ref = '982::940321'
ASSERT VALUE declared_total = '6' WHERE order_ref = '981::940321'
ASSERT VALUE declared_total = '13' WHERE order_ref = '982::940321'
SELECT
    ord_1 AS order_ref,
    otr_1 AS declared_lines,
    otr_2 AS declared_total
FROM {{zone_name}}.edi_demos.tradacoms_order_lines
WHERE mhd_2 = 'ORDERS:9' AND otr_1 IS NOT NULL
ORDER BY ord_1;


-- ============================================================================
-- 8. Grand Total — All Orders
-- ============================================================================
-- Aggregates across all order line items to produce file-level totals.
-- This gives a single-row summary of the entire purchase order transmission.
--
-- What you'll see:
--   - total_lines:       Number of ORDERS messages with OLD data (2)
--   - grand_total:       Sum of (quantity * unit_price) for last lines (168)
--   - distinct_products: Count of unique EAN-13 product codes (2)

ASSERT ROW_COUNT = 1
ASSERT VALUE total_lines = 2
ASSERT VALUE grand_total = 168
ASSERT VALUE distinct_products = 2
SELECT
    COUNT(*) AS total_lines,
    SUM(CAST(old_5 AS INTEGER) * CAST(old_6 AS INTEGER)) AS grand_total,
    COUNT(DISTINCT old_2) AS distinct_products
FROM {{zone_name}}.edi_demos.tradacoms_order_lines
WHERE mhd_2 = 'ORDERS:9' AND old_1 IS NOT NULL;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: message count, order count, line count, and
-- grand total. Validates that the file parsed correctly and all segments
-- are accessible through both the compact and materialized tables.

ASSERT ROW_COUNT = 4
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'order_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'line_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'grand_total'
SELECT check_name, result FROM (

    -- Check 1: Exact total message count = 4 (ORDHDR + ORDERS x2 + ORDTLR)
    SELECT 'message_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_order_compact) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 2 distinct orders in ORDERS messages
    SELECT 'order_count' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT ord_1) FROM {{zone_name}}.edi_demos.tradacoms_order_lines
                       WHERE mhd_2 = 'ORDERS:9' AND ord_1 IS NOT NULL) = 2
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 2 line items (last OLD per ORDERS message)
    SELECT 'line_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.tradacoms_order_lines
                       WHERE mhd_2 = 'ORDERS:9' AND old_1 IS NOT NULL) = 2
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Grand total = 168 (120 + 48)
    SELECT 'grand_total' AS check_name,
           CASE WHEN (SELECT SUM(CAST(old_5 AS INTEGER) * CAST(old_6 AS INTEGER))
                       FROM {{zone_name}}.edi_demos.tradacoms_order_lines
                       WHERE mhd_2 = 'ORDERS:9' AND old_1 IS NOT NULL) = 168
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
