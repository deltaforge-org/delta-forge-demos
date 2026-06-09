-- ============================================================================
-- Delta JSON Event Log — Payment Gateway Analysis — Setup Script
-- ============================================================================
-- Demonstrates querying heterogeneous JSON payloads stored in VARCHAR columns.
--
-- Tables created:
--   1. payment_events — 40 payment gateway transactions with JSON payloads
--
-- Operations performed:
--   1. CREATE DELTA TABLE with 7 columns
--   2. INSERT — 15 charge events (successful card payments)
--   3. INSERT — 10 refund events (partial and full refunds)
--   4. INSERT — 8 auth events (pre-authorization holds)
--   5. INSERT — 7 payout events (merchant settlements)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: payment_events — 40 payment gateway transactions
-- ============================================================================
-- Each txn_type has a different payload shape:
--   charge:  {"amount", "currency", "merchant", "card_last4", "status"}
--   refund:  {"amount", "currency", "original_txn", "reason"}
--   auth:    {"amount", "currency", "merchant", "hold_expires"}
--   payout:  {"amount", "currency", "merchant", "account_type"}
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.payment_events (
    id             INT,
    txn_type       VARCHAR,
    source         VARCHAR,
    payload        VARCHAR,
    metadata       VARCHAR,
    status         VARCHAR,
    created_at     VARCHAR
) LOCATION 'delta-json-event-log/payment_events';


-- STEP 2: Insert 15 charge events
INSERT INTO {{zone_name}}.delta_demos.payment_events VALUES
    (1,  'charge', 'web',     '{"amount":149.99,"currency":"USD","merchant":"Electronics Plus","card_last4":"4242","status":"succeeded"}', '{"ip":"10.0.0.1","country":"US"}',  'completed', '2024-03-01 08:15:00'),
    (2,  'charge', 'mobile',  '{"amount":29.50,"currency":"USD","merchant":"Coffee Corner","card_last4":"1234","status":"succeeded"}',     '{"ip":"10.0.1.1","country":"US"}',  'completed', '2024-03-01 08:22:00'),
    (3,  'charge', 'web',     '{"amount":520.00,"currency":"EUR","merchant":"Fashion House","card_last4":"5678","status":"succeeded"}',     '{"ip":"10.0.0.2","country":"DE"}',  'completed', '2024-03-01 09:10:00'),
    (4,  'charge', 'pos',     '{"amount":12.75,"currency":"USD","merchant":"Quick Mart","card_last4":"9012","status":"succeeded"}',         '{"ip":"10.0.2.1","country":"US"}',  'completed', '2024-03-01 09:30:00'),
    (5,  'charge', 'web',     '{"amount":899.99,"currency":"USD","merchant":"Tech Store","card_last4":"3456","status":"declined"}',         '{"ip":"10.0.0.3","country":"US"}',  'failed',    '2024-03-01 10:00:00'),
    (6,  'charge', 'mobile',  '{"amount":45.00,"currency":"GBP","merchant":"Pub Grub","card_last4":"7890","status":"succeeded"}',           '{"ip":"10.0.1.2","country":"GB"}',  'completed', '2024-03-01 10:15:00'),
    (7,  'charge', 'web',     '{"amount":1250.00,"currency":"USD","merchant":"Luxury Goods","card_last4":"2345","status":"succeeded"}',     '{"ip":"10.0.0.4","country":"US"}',  'completed', '2024-03-01 10:45:00'),
    (8,  'charge', 'pos',     '{"amount":8.99,"currency":"USD","merchant":"Coffee Corner","card_last4":"6789","status":"succeeded"}',       '{"ip":"10.0.2.2","country":"US"}',  'completed', '2024-03-01 11:00:00'),
    (9,  'charge', 'web',     '{"amount":350.00,"currency":"EUR","merchant":"Hotel Central","card_last4":"0123","status":"succeeded"}',     '{"ip":"10.0.0.5","country":"FR"}',  'completed', '2024-03-01 11:30:00'),
    (10, 'charge', 'mobile',  '{"amount":75.25,"currency":"USD","merchant":"Grocery World","card_last4":"4567","status":"declined"}',       '{"ip":"10.0.1.3","country":"US"}',  'failed',    '2024-03-01 12:00:00'),
    (11, 'charge', 'web',     '{"amount":199.99,"currency":"USD","merchant":"Electronics Plus","card_last4":"8901","status":"succeeded"}',  '{"ip":"10.0.0.6","country":"US"}',  'completed', '2024-03-01 12:30:00'),
    (12, 'charge', 'pos',     '{"amount":33.50,"currency":"USD","merchant":"Quick Mart","card_last4":"2345","status":"succeeded"}',         '{"ip":"10.0.2.3","country":"US"}',  'completed', '2024-03-01 13:00:00'),
    (13, 'charge', 'web',     '{"amount":2100.00,"currency":"USD","merchant":"Luxury Goods","card_last4":"6780","status":"flagged"}',       '{"ip":"10.0.0.7","country":"US"}',  'review',    '2024-03-01 13:15:00'),
    (14, 'charge', 'mobile',  '{"amount":15.00,"currency":"USD","merchant":"Snack Shack","card_last4":"1122","status":"succeeded"}',        '{"ip":"10.0.1.4","country":"US"}',  'completed', '2024-03-01 13:45:00'),
    (15, 'charge', 'web',     '{"amount":450.00,"currency":"GBP","merchant":"British Airways","card_last4":"3344","status":"succeeded"}',   '{"ip":"10.0.0.8","country":"GB"}',  'completed', '2024-03-01 14:00:00');


-- ============================================================================
-- STEP 3: Insert 10 refund events
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.payment_events
SELECT * FROM (VALUES
    (16, 'refund', 'web',     '{"amount":149.99,"currency":"USD","original_txn":"TXN-001","reason":"defective"}',    '{"agent":"support-1"}', 'completed', '2024-03-01 15:00:00'),
    (17, 'refund', 'web',     '{"amount":260.00,"currency":"EUR","original_txn":"TXN-003","reason":"wrong_size"}',   '{"agent":"support-2"}', 'completed', '2024-03-01 15:10:00'),
    (18, 'refund', 'mobile',  '{"amount":29.50,"currency":"USD","original_txn":"TXN-002","reason":"duplicate"}',     '{"agent":"auto"}',      'completed', '2024-03-01 15:20:00'),
    (19, 'refund', 'web',     '{"amount":899.99,"currency":"USD","original_txn":"TXN-005","reason":"unauthorized"}', '{"agent":"fraud-1"}',   'completed', '2024-03-01 15:30:00'),
    (20, 'refund', 'pos',     '{"amount":12.75,"currency":"USD","original_txn":"TXN-004","reason":"returned"}',      '{"agent":"store-mgr"}', 'completed', '2024-03-01 15:40:00'),
    (21, 'refund', 'web',     '{"amount":100.00,"currency":"USD","original_txn":"TXN-007","reason":"partial"}',      '{"agent":"support-1"}', 'completed', '2024-03-01 15:50:00'),
    (22, 'refund', 'mobile',  '{"amount":45.00,"currency":"GBP","original_txn":"TXN-006","reason":"wrong_item"}',    '{"agent":"support-3"}', 'completed', '2024-03-01 16:00:00'),
    (23, 'refund', 'web',     '{"amount":75.25,"currency":"USD","original_txn":"TXN-010","reason":"not_received"}',  '{"agent":"support-2"}', 'completed', '2024-03-01 16:10:00'),
    (24, 'refund', 'web',     '{"amount":199.99,"currency":"USD","original_txn":"TXN-011","reason":"defective"}',    '{"agent":"support-1"}', 'completed', '2024-03-01 16:20:00'),
    (25, 'refund', 'pos',     '{"amount":33.50,"currency":"USD","original_txn":"TXN-012","reason":"returned"}',      '{"agent":"store-mgr"}', 'completed', '2024-03-01 16:30:00')
) AS t(id, txn_type, source, payload, metadata, status, created_at);


-- ============================================================================
-- STEP 4: Insert 8 auth (pre-authorization) events
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.payment_events
SELECT * FROM (VALUES
    (26, 'auth', 'web',     '{"amount":500.00,"currency":"USD","merchant":"Hotel Central","hold_expires":"2024-03-08"}',    '{"ip":"10.0.0.9","country":"US"}',  'active',   '2024-03-01 17:00:00'),
    (27, 'auth', 'web',     '{"amount":200.00,"currency":"EUR","merchant":"Car Rental EU","hold_expires":"2024-03-04"}',   '{"ip":"10.0.0.10","country":"DE"}', 'active',   '2024-03-01 17:15:00'),
    (28, 'auth', 'mobile',  '{"amount":150.00,"currency":"USD","merchant":"Gas Station","hold_expires":"2024-03-02"}',     '{"ip":"10.0.1.5","country":"US"}',  'captured', '2024-03-01 17:30:00'),
    (29, 'auth', 'pos',     '{"amount":75.00,"currency":"USD","merchant":"Restaurant Row","hold_expires":"2024-03-03"}',   '{"ip":"10.0.2.4","country":"US"}',  'captured', '2024-03-01 17:45:00'),
    (30, 'auth', 'web',     '{"amount":1000.00,"currency":"GBP","merchant":"London Suites","hold_expires":"2024-03-10"}',  '{"ip":"10.0.0.11","country":"GB"}', 'active',   '2024-03-01 18:00:00'),
    (31, 'auth', 'mobile',  '{"amount":50.00,"currency":"USD","merchant":"Parking Garage","hold_expires":"2024-03-02"}',   '{"ip":"10.0.1.6","country":"US"}',  'voided',   '2024-03-01 18:15:00'),
    (32, 'auth', 'web',     '{"amount":3000.00,"currency":"USD","merchant":"Jewelry Mart","hold_expires":"2024-03-08"}',   '{"ip":"10.0.0.12","country":"US"}', 'active',   '2024-03-01 18:30:00'),
    (33, 'auth', 'pos',     '{"amount":25.00,"currency":"USD","merchant":"Toll Bridge","hold_expires":"2024-03-02"}',      '{"ip":"10.0.2.5","country":"US"}',  'captured', '2024-03-01 18:45:00')
) AS t(id, txn_type, source, payload, metadata, status, created_at);


-- ============================================================================
-- STEP 5: Insert 7 payout events
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.payment_events
SELECT * FROM (VALUES
    (34, 'payout', 'internal', '{"amount":4250.00,"currency":"USD","merchant":"Electronics Plus","account_type":"checking"}',  '{"batch":"B100"}', 'completed', '2024-03-01 20:00:00'),
    (35, 'payout', 'internal', '{"amount":1890.50,"currency":"EUR","merchant":"Fashion House","account_type":"business"}',     '{"batch":"B100"}', 'completed', '2024-03-01 20:05:00'),
    (36, 'payout', 'internal', '{"amount":780.00,"currency":"USD","merchant":"Coffee Corner","account_type":"checking"}',      '{"batch":"B100"}', 'completed', '2024-03-01 20:10:00'),
    (37, 'payout', 'internal', '{"amount":12500.00,"currency":"USD","merchant":"Luxury Goods","account_type":"business"}',     '{"batch":"B101"}', 'completed', '2024-03-01 20:15:00'),
    (38, 'payout', 'internal', '{"amount":3200.00,"currency":"GBP","merchant":"British Airways","account_type":"business"}',   '{"batch":"B101"}', 'completed', '2024-03-01 20:20:00'),
    (39, 'payout', 'internal', '{"amount":560.25,"currency":"USD","merchant":"Quick Mart","account_type":"checking"}',         '{"batch":"B101"}', 'completed', '2024-03-01 20:25:00'),
    (40, 'payout', 'internal', '{"amount":950.00,"currency":"USD","merchant":"Grocery World","account_type":"checking"}',      '{"batch":"B102"}', 'completed', '2024-03-01 20:30:00')
) AS t(id, txn_type, source, payload, metadata, status, created_at);
