-- ============================================================================
-- Delta MERGE — Multi-Source Sequential Updates — Setup Script
-- ============================================================================
-- Creates four tables for the multi-source MERGE demo:
--
--   1. order_status      — 20 orders (target), unified fulfillment table
--   2. shipping_updates  — 14 shipping events (source 1)
--   3. payment_updates   — 16 payment captures (source 2)
--   4. return_updates    —  4 return requests  (source 3)
--
-- Each source MERGEs into order_status sequentially, updating only its own
-- columns while preserving data written by the other sources.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: order_status — 20 orders (target)
-- ============================================================================
-- The unified fulfillment table. All orders start with shipping/payment/return
-- fields in their initial state. Each MERGE will fill in its own columns.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.order_status (
    order_id         VARCHAR,
    customer_name    VARCHAR,
    order_date       VARCHAR,
    total_amount     DOUBLE,
    item_count       INT,
    shipping_status  VARCHAR,
    carrier          VARCHAR,
    tracking_number  VARCHAR,
    ship_date        VARCHAR,
    payment_status   VARCHAR,
    payment_method   VARCHAR,
    transaction_id   VARCHAR,
    return_status    VARCHAR,
    return_reason    VARCHAR,
    refund_amount    DOUBLE,
    last_updated     VARCHAR
) LOCATION 'delta-merge-multi-source/order_status';


INSERT INTO {{zone_name}}.delta_demos.order_status VALUES
    ('ORD-5001', 'Alice Johnson',    '2025-03-01', 129.99, 3,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-01'),
    ('ORD-5002', 'Bob Martinez',     '2025-03-01', 249.50, 5,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-01'),
    ('ORD-5003', 'Carol Chen',       '2025-03-02', 89.99,  1,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-02'),
    ('ORD-5004', 'David Kim',        '2025-03-02', 445.00, 8,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-02'),
    ('ORD-5005', 'Elena Rodriguez',  '2025-03-03', 67.50,  2,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-03'),
    ('ORD-5006', 'Frank O''Brien',   '2025-03-03', 312.75, 6,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-03'),
    ('ORD-5007', 'Grace Patel',      '2025-03-04', 178.20, 4,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-04'),
    ('ORD-5008', 'Henry Nakamura',   '2025-03-04', 95.00,  2,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-04'),
    ('ORD-5009', 'Irene Fischer',    '2025-03-05', 520.30, 10, 'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-05'),
    ('ORD-5010', 'James Cooper',     '2025-03-05', 42.99,  1,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-05'),
    ('ORD-5011', 'Karen Liu',        '2025-03-06', 189.00, 3,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-06'),
    ('ORD-5012', 'Leo Washington',   '2025-03-06', 275.40, 5,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-06'),
    ('ORD-5013', 'Maria Gonzalez',   '2025-03-07', 156.80, 3,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-07'),
    ('ORD-5014', 'Nathan Brooks',    '2025-03-07', 88.50,  2,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-07'),
    ('ORD-5015', 'Olivia Thompson',  '2025-03-08', 634.00, 12, 'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-08'),
    ('ORD-5016', 'Patrick Lee',      '2025-03-08', 210.25, 4,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-08'),
    ('ORD-5017', 'Quinn Adams',      '2025-03-09', 147.60, 3,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-09'),
    ('ORD-5018', 'Rachel Diaz',      '2025-03-09', 399.99, 7,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-09'),
    ('ORD-5019', 'Sam Wilson',       '2025-03-10', 55.00,  1,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-10'),
    ('ORD-5020', 'Tina Romero',      '2025-03-10', 328.80, 6,  'pending', NULL, NULL, NULL, 'authorized', NULL, NULL, 'none', NULL, 0.00, '2025-03-10');


-- ============================================================================
-- TABLE 2: shipping_updates — 14 shipping events (source 1)
-- ============================================================================
-- The shipping system reports tracking info for orders 1-14. Orders 15-20
-- have not shipped yet and remain pending.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.shipping_updates (
    order_id         VARCHAR,
    shipping_status  VARCHAR,
    carrier          VARCHAR,
    tracking_number  VARCHAR,
    ship_date        VARCHAR,
    last_updated     VARCHAR
) LOCATION 'delta-merge-multi-source/shipping_updates';


INSERT INTO {{zone_name}}.delta_demos.shipping_updates VALUES
    ('ORD-5001', 'delivered', 'FedEx', 'FX-78234561', '2025-03-02', '2025-03-15'),
    ('ORD-5002', 'delivered', 'UPS',   'UP-44982137', '2025-03-02', '2025-03-15'),
    ('ORD-5003', 'delivered', 'USPS',  'US-92847561', '2025-03-03', '2025-03-15'),
    ('ORD-5004', 'delivered', 'FedEx', 'FX-78234562', '2025-03-03', '2025-03-15'),
    ('ORD-5005', 'delivered', 'DHL',   'DH-55193827', '2025-03-04', '2025-03-15'),
    ('ORD-5006', 'delivered', 'UPS',   'UP-44982138', '2025-03-04', '2025-03-15'),
    ('ORD-5007', 'delivered', 'FedEx', 'FX-78234563', '2025-03-05', '2025-03-15'),
    ('ORD-5008', 'shipped',  'USPS',  'US-92847562', '2025-03-05', '2025-03-15'),
    ('ORD-5009', 'shipped',  'UPS',   'UP-44982139', '2025-03-06', '2025-03-15'),
    ('ORD-5010', 'shipped',  'DHL',   'DH-55193828', '2025-03-06', '2025-03-15'),
    ('ORD-5011', 'shipped',  'FedEx', 'FX-78234564', '2025-03-07', '2025-03-15'),
    ('ORD-5012', 'shipped',  'UPS',   'UP-44982140', '2025-03-07', '2025-03-15'),
    ('ORD-5013', 'shipped',  'USPS',  'US-92847563', '2025-03-08', '2025-03-15'),
    ('ORD-5014', 'shipped',  'FedEx', 'FX-78234565', '2025-03-08', '2025-03-15');


-- ============================================================================
-- TABLE 3: payment_updates — 16 payment captures (source 2)
-- ============================================================================
-- The payment system confirms captures for orders 1-16. Orders 17-20 are
-- still in "authorized" state waiting for capture.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.payment_updates (
    order_id         VARCHAR,
    payment_status   VARCHAR,
    payment_method   VARCHAR,
    transaction_id   VARCHAR,
    last_updated     VARCHAR
) LOCATION 'delta-merge-multi-source/payment_updates';


INSERT INTO {{zone_name}}.delta_demos.payment_updates VALUES
    ('ORD-5001', 'captured', 'credit_card', 'TXN-CC-90001', '2025-03-15'),
    ('ORD-5002', 'captured', 'paypal',      'TXN-PP-90002', '2025-03-15'),
    ('ORD-5003', 'captured', 'credit_card', 'TXN-CC-90003', '2025-03-15'),
    ('ORD-5004', 'captured', 'debit_card',  'TXN-DC-90004', '2025-03-15'),
    ('ORD-5005', 'captured', 'apple_pay',   'TXN-AP-90005', '2025-03-15'),
    ('ORD-5006', 'captured', 'credit_card', 'TXN-CC-90006', '2025-03-15'),
    ('ORD-5007', 'captured', 'paypal',      'TXN-PP-90007', '2025-03-15'),
    ('ORD-5008', 'captured', 'credit_card', 'TXN-CC-90008', '2025-03-15'),
    ('ORD-5009', 'captured', 'debit_card',  'TXN-DC-90009', '2025-03-15'),
    ('ORD-5010', 'captured', 'apple_pay',   'TXN-AP-90010', '2025-03-15'),
    ('ORD-5011', 'captured', 'credit_card', 'TXN-CC-90011', '2025-03-15'),
    ('ORD-5012', 'captured', 'paypal',      'TXN-PP-90012', '2025-03-15'),
    ('ORD-5013', 'captured', 'credit_card', 'TXN-CC-90013', '2025-03-15'),
    ('ORD-5014', 'captured', 'debit_card',  'TXN-DC-90014', '2025-03-15'),
    ('ORD-5015', 'captured', 'credit_card', 'TXN-CC-90015', '2025-03-15'),
    ('ORD-5016', 'captured', 'apple_pay',   'TXN-AP-90016', '2025-03-15');


-- ============================================================================
-- TABLE 4: return_updates — 4 return requests (source 3)
-- ============================================================================
-- The returns system reports 4 return/refund events. These will also update
-- shipping_status to 'returned' and payment_status to 'refunded'.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.return_updates (
    order_id         VARCHAR,
    return_status    VARCHAR,
    return_reason    VARCHAR,
    refund_amount    DOUBLE,
    last_updated     VARCHAR
) LOCATION 'delta-merge-multi-source/return_updates';


INSERT INTO {{zone_name}}.delta_demos.return_updates VALUES
    ('ORD-5002', 'approved',  'wrong_item',       249.50, '2025-03-15'),
    ('ORD-5005', 'completed', 'defective',        67.50,  '2025-03-15'),
    ('ORD-5007', 'approved',  'not_as_described', 89.10,  '2025-03-15'),
    ('ORD-5013', 'approved',  'changed_mind',     156.80, '2025-03-15');
