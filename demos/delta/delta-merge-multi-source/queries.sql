-- ============================================================================
-- Delta MERGE — Multi-Source Sequential Updates — Educational Queries
-- ============================================================================
-- WHAT: Three independent source systems MERGE into a single order_status
--       table sequentially — shipping, payments, then returns.
-- WHY:  In event-driven architectures and microservice data integration,
--       each system owns its own columns. Sequential MERGEs let each source
--       update only its fields while preserving data written by others.
-- HOW:  Each MERGE matches on order_id and updates only source-specific
--       columns. The return MERGE also updates cross-cutting status fields
--       (shipping_status, payment_status) when business logic requires it.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Orders Before Updates
-- ============================================================================
-- All 20 orders start with shipping/payment/return fields in their initial
-- state: shipping_status='pending', payment_status='authorized',
-- return_status='none', and all detail columns NULL.

ASSERT ROW_COUNT = 20
SELECT order_id, customer_name, order_date, total_amount,
       shipping_status, carrier, tracking_number,
       payment_status, payment_method, transaction_id,
       return_status, return_reason, refund_amount
FROM {{zone_name}}.delta_demos.order_status
ORDER BY order_id;

ASSERT VALUE shipping_status = 'pending' WHERE order_id = 'ORD-5001'
SELECT order_id, shipping_status, payment_status, return_status
FROM {{zone_name}}.delta_demos.order_status
WHERE order_id = 'ORD-5001';


-- ============================================================================
-- MERGE 1: Shipping Updates (14 rows)
-- ============================================================================
-- The shipping system provides tracking numbers, carrier info, and ship dates
-- for orders 1-14. Orders 15-20 have not shipped yet. This MERGE only touches
-- shipping-related columns — payment and return columns are untouched.

ASSERT ROW_COUNT = 14
MERGE INTO {{zone_name}}.delta_demos.order_status AS target
USING {{zone_name}}.delta_demos.shipping_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET
        shipping_status = source.shipping_status,
        carrier         = source.carrier,
        tracking_number = source.tracking_number,
        ship_date       = source.ship_date,
        last_updated    = source.last_updated;


-- ============================================================================
-- EXPLORE: After Shipping Merge
-- ============================================================================
-- After the shipping MERGE, 7 orders are delivered, 7 are shipped (in transit),
-- and 6 remain pending (no shipping event received).

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 7 WHERE shipping_status = 'delivered'
ASSERT VALUE order_count = 7 WHERE shipping_status = 'shipped'
ASSERT VALUE order_count = 6 WHERE shipping_status = 'pending'
SELECT shipping_status,
       COUNT(*) AS order_count
FROM {{zone_name}}.delta_demos.order_status
GROUP BY shipping_status
ORDER BY order_count DESC;

-- Verify specific tracking numbers were written correctly
ASSERT VALUE carrier = 'FedEx' WHERE order_id = 'ORD-5001'
ASSERT VALUE tracking_number = 'FX-78234561' WHERE order_id = 'ORD-5001'
SELECT order_id, shipping_status, carrier, tracking_number, ship_date
FROM {{zone_name}}.delta_demos.order_status
WHERE order_id IN ('ORD-5001', 'ORD-5005', 'ORD-5010', 'ORD-5019')
ORDER BY order_id;


-- ============================================================================
-- MERGE 2: Payment Updates (16 rows)
-- ============================================================================
-- The payment system confirms captures for orders 1-16. Orders 17-20 remain
-- in "authorized" state. This MERGE only touches payment-related columns —
-- shipping data from MERGE 1 is fully preserved.

ASSERT ROW_COUNT = 16
MERGE INTO {{zone_name}}.delta_demos.order_status AS target
USING {{zone_name}}.delta_demos.payment_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET
        payment_status = source.payment_status,
        payment_method = source.payment_method,
        transaction_id = source.transaction_id,
        last_updated   = source.last_updated;


-- ============================================================================
-- EXPLORE: After Payment Merge
-- ============================================================================
-- Payment method distribution across the 16 captured payments.

ASSERT ROW_COUNT = 4
ASSERT VALUE method_count = 7 WHERE payment_method = 'credit_card'
ASSERT VALUE method_count = 3 WHERE payment_method = 'paypal'
ASSERT VALUE method_count = 3 WHERE payment_method = 'debit_card'
ASSERT VALUE method_count = 3 WHERE payment_method = 'apple_pay'
SELECT payment_method,
       COUNT(*) AS method_count
FROM {{zone_name}}.delta_demos.order_status
WHERE payment_method IS NOT NULL
GROUP BY payment_method
ORDER BY method_count DESC;

-- KEY EDUCATIONAL POINT: Shipping data from MERGE 1 is preserved after
-- the payment MERGE. ORD-5001 now has BOTH shipping AND payment data.
ASSERT VALUE shipping_status = 'delivered' WHERE order_id = 'ORD-5001'
ASSERT VALUE carrier = 'FedEx' WHERE order_id = 'ORD-5001'
ASSERT VALUE payment_status = 'captured' WHERE order_id = 'ORD-5001'
ASSERT VALUE payment_method = 'credit_card' WHERE order_id = 'ORD-5001'
ASSERT VALUE transaction_id = 'TXN-CC-90001' WHERE order_id = 'ORD-5001'
SELECT order_id, shipping_status, carrier, tracking_number,
       payment_status, payment_method, transaction_id
FROM {{zone_name}}.delta_demos.order_status
WHERE order_id = 'ORD-5001';


-- ============================================================================
-- MERGE 3: Return Updates (4 rows)
-- ============================================================================
-- The returns system reports 4 return/refund events. Unlike the previous two
-- MERGEs which only touched their own columns, this MERGE also updates
-- shipping_status to 'returned' and payment_status to 'refunded' — because
-- business logic requires cross-cutting status changes when a return occurs.

ASSERT ROW_COUNT = 4
MERGE INTO {{zone_name}}.delta_demos.order_status AS target
USING {{zone_name}}.delta_demos.return_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET
        return_status   = source.return_status,
        return_reason   = source.return_reason,
        refund_amount   = source.refund_amount,
        shipping_status = 'returned',
        payment_status  = 'refunded',
        last_updated    = source.last_updated;


-- ============================================================================
-- EXPLORE: Complete Order Picture
-- ============================================================================
-- All 20 orders now reflect the combined state from all 3 source systems.
-- Each MERGE contributed its own data; the return MERGE also updated
-- cross-cutting status fields for the 4 returned orders.

ASSERT ROW_COUNT = 20
SELECT order_id, customer_name, total_amount,
       shipping_status, carrier, tracking_number,
       payment_status, payment_method, transaction_id,
       return_status, return_reason, refund_amount
FROM {{zone_name}}.delta_demos.order_status
ORDER BY order_id;

-- ORD-5002 was touched by all 3 sources: delivered by UPS, paid via PayPal,
-- then returned (wrong_item) with full refund. The return MERGE overrode
-- shipping_status to 'returned' and payment_status to 'refunded'.
ASSERT VALUE shipping_status = 'returned' WHERE order_id = 'ORD-5002'
ASSERT VALUE payment_status = 'refunded' WHERE order_id = 'ORD-5002'
ASSERT VALUE return_reason = 'wrong_item' WHERE order_id = 'ORD-5002'
SELECT order_id, shipping_status, carrier, payment_status, payment_method,
       return_status, return_reason, refund_amount
FROM {{zone_name}}.delta_demos.order_status
WHERE order_id = 'ORD-5002';

-- ORD-5019 was not updated by any source — all detail columns remain NULL.
ASSERT VALUE shipping_status = 'pending' WHERE order_id = 'ORD-5019'
ASSERT VALUE payment_status = 'authorized' WHERE order_id = 'ORD-5019'
ASSERT VALUE return_status = 'none' WHERE order_id = 'ORD-5019'
SELECT order_id, shipping_status, carrier, tracking_number,
       payment_status, payment_method, transaction_id,
       return_status, return_reason, refund_amount
FROM {{zone_name}}.delta_demos.order_status
WHERE order_id = 'ORD-5019';


-- ============================================================================
-- LEARN: Multi-Source Preservation
-- ============================================================================
-- The central lesson: each MERGE only updated its own columns, preserving
-- data written by prior MERGEs. The exception is the return MERGE, which
-- intentionally updated shipping_status and payment_status to reflect the
-- cross-cutting business impact of a return.
--
-- These 4 orders were touched by all 3 sources (shipping + payment + returns).
-- They demonstrate the full lifecycle: shipped → paid → returned/refunded.

ASSERT ROW_COUNT = 4
SELECT order_id, customer_name, total_amount,
       shipping_status, carrier,
       payment_status, payment_method,
       return_status, return_reason, refund_amount
FROM {{zone_name}}.delta_demos.order_status
WHERE return_status <> 'none'
ORDER BY order_id;


-- ============================================================================
-- EXPLORE: Order Fulfillment Summary
-- ============================================================================
-- Final shipping status breakdown after all 3 MERGEs:
--   delivered=4 (was 7, minus 3 that were returned)
--   shipped=6   (was 7, minus 1 that was returned)
--   pending=6   (unchanged — no shipping event)
--   returned=4  (set by return MERGE)

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 6 WHERE shipping_status = 'pending'
ASSERT VALUE order_count = 6 WHERE shipping_status = 'shipped'
ASSERT VALUE order_count = 4 WHERE shipping_status = 'delivered'
ASSERT VALUE order_count = 4 WHERE shipping_status = 'returned'
SELECT shipping_status,
       COUNT(*)                AS order_count,
       ROUND(SUM(total_amount), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.order_status
GROUP BY shipping_status
ORDER BY order_count DESC;

-- Final payment status breakdown:
--   captured=12, authorized=4 (never captured), refunded=4
ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 12 WHERE payment_status = 'captured'
ASSERT VALUE order_count = 4 WHERE payment_status = 'authorized'
ASSERT VALUE order_count = 4 WHERE payment_status = 'refunded'
SELECT payment_status,
       COUNT(*)                AS order_count,
       ROUND(SUM(total_amount), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.order_status
GROUP BY payment_status
ORDER BY order_count DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows_unchanged: All 3 MERGEs were UPDATE-only, no INSERTs or DELETEs
ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.delta_demos.order_status;

-- Verify delivered_count: 7 originally delivered, 3 returned (ORD-5002, 5005, 5007) → 4
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE shipping_status = 'delivered';

-- Verify shipped_count: 7 originally shipped, 1 returned (ORD-5013) → 6
ASSERT VALUE cnt = 6
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE shipping_status = 'shipped';

-- Verify pending_count: 6 orders never shipped
ASSERT VALUE cnt = 6
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE shipping_status = 'pending';

-- Verify returned_count: 4 orders returned
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE shipping_status = 'returned';

-- Verify captured_count: 16 payments captured, 4 changed to refunded → 12
ASSERT VALUE cnt = 12
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE payment_status = 'captured';

-- Verify refunded_count: 4 orders refunded
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE payment_status = 'refunded';

-- Verify authorized_count: 4 orders never captured (ORD-5017 through ORD-5020)
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE payment_status = 'authorized';

-- Verify total_refund_amount: 249.50 + 67.50 + 89.10 + 156.80 = 562.90
ASSERT VALUE total_refunds = 562.90
SELECT ROUND(SUM(refund_amount), 2) AS total_refunds
FROM {{zone_name}}.delta_demos.order_status;

-- Verify untouched_orders: 4 orders (ORD-5017 through ORD-5020) had no updates from any source
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE shipping_status = 'pending'
  AND payment_status = 'authorized'
  AND return_status = 'none'
  AND carrier IS NULL
  AND payment_method IS NULL;

-- Verify cross_source_preservation: ORD-5001 has shipping + payment data intact
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE order_id = 'ORD-5001'
  AND shipping_status = 'delivered'
  AND carrier = 'FedEx'
  AND tracking_number = 'FX-78234561'
  AND payment_status = 'captured'
  AND payment_method = 'credit_card'
  AND transaction_id = 'TXN-CC-90001'
  AND return_status = 'none';

-- Verify return_cross_cutting: ORD-5005 has all 3 sources + status overrides
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.order_status
WHERE order_id = 'ORD-5005'
  AND shipping_status = 'returned'
  AND carrier = 'DHL'
  AND payment_status = 'refunded'
  AND payment_method = 'apple_pay'
  AND return_status = 'completed'
  AND return_reason = 'defective'
  AND refund_amount = 67.50;
