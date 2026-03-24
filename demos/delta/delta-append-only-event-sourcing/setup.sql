-- ============================================================================
-- Delta Append-Only Event Sourcing — Setup Script
-- ============================================================================
-- Demonstrates:
--   1. Append-only tables as immutable event logs
--   2. Event sourcing: deriving state from event history
--   3. Window functions (LAG) on immutable streams
--   4. Batch appends to prove the log only grows
--
-- Table:
--   order_events — 60 events across 14 orders (append-only)
--
-- TBLPROPERTIES:
--   delta.appendOnly = 'true'  →  UPDATE and DELETE are forbidden
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: order_events — immutable event log
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.order_events (
    id         BIGINT,
    order_id   INT,
    event_type VARCHAR,
    amount     DOUBLE,
    actor      VARCHAR,
    created_at VARCHAR
) LOCATION '{{data_path}}/order_events'
TBLPROPERTIES (
    'delta.appendOnly' = 'true'
);

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.order_events TO USER {{current_user}};

-- Insert 60 events across 14 orders in 2 batches
-- Order lifecycles:
--   1001, 1002, 1004, 1006, 1009, 1010, 1013: created → delivered (complete)
--   1003, 1012: created → cancelled → refunded
--   1005, 1011: created → shipped (in transit)
--   1007: created → paid (processing)
--   1008: created → cancelled (early cancel)
--   1014: created → confirmed (just started)

-- Batch 1: 35 events (orders 1001-1007)
INSERT INTO {{zone_name}}.delta_demos.order_events VALUES
    (1,  1001, 'order.created',     150.00, 'alice',  '2024-06-01 08:00:00'),
    (2,  1001, 'order.confirmed',   150.00, 'system', '2024-06-01 08:05:00'),
    (3,  1001, 'payment.received',  150.00, 'stripe', '2024-06-01 08:12:00'),
    (4,  1001, 'order.shipped',     150.00, 'bob',    '2024-06-01 10:30:00'),
    (5,  1001, 'order.delivered',   150.00, 'courier','2024-06-01 16:00:00'),
    (6,  1002, 'order.created',     89.99,  'carol',  '2024-06-01 08:15:00'),
    (7,  1002, 'order.confirmed',   89.99,  'system', '2024-06-01 08:20:00'),
    (8,  1002, 'payment.received',  89.99,  'stripe', '2024-06-01 08:25:00'),
    (9,  1002, 'order.shipped',     89.99,  'bob',    '2024-06-01 11:00:00'),
    (10, 1002, 'order.delivered',   89.99,  'courier','2024-06-01 17:30:00'),
    (11, 1003, 'order.created',     245.00, 'dave',   '2024-06-01 09:00:00'),
    (12, 1003, 'order.confirmed',   245.00, 'system', '2024-06-01 09:05:00'),
    (13, 1003, 'payment.received',  245.00, 'stripe', '2024-06-01 09:10:00'),
    (14, 1003, 'order.cancelled',   245.00, 'dave',   '2024-06-01 09:45:00'),
    (15, 1003, 'payment.refunded',  245.00, 'stripe', '2024-06-01 10:00:00'),
    (16, 1004, 'order.created',     67.50,  'eve',    '2024-06-01 09:30:00'),
    (17, 1004, 'order.confirmed',   67.50,  'system', '2024-06-01 09:35:00'),
    (18, 1004, 'payment.received',  67.50,  'stripe', '2024-06-01 09:40:00'),
    (19, 1004, 'order.shipped',     67.50,  'bob',    '2024-06-01 12:00:00'),
    (20, 1004, 'order.delivered',   67.50,  'courier','2024-06-01 18:00:00'),
    (21, 1005, 'order.created',     120.00, 'frank',  '2024-06-01 10:00:00'),
    (22, 1005, 'order.confirmed',   120.00, 'system', '2024-06-01 10:05:00'),
    (23, 1005, 'payment.received',  120.00, 'stripe', '2024-06-01 10:10:00'),
    (24, 1005, 'order.shipped',     120.00, 'bob',    '2024-06-01 13:00:00'),
    (25, 1006, 'order.created',     399.99, 'grace',  '2024-06-01 10:15:00'),
    (26, 1006, 'order.confirmed',   399.99, 'system', '2024-06-01 10:20:00'),
    (27, 1006, 'payment.received',  399.99, 'stripe', '2024-06-01 10:25:00'),
    (28, 1006, 'order.shipped',     399.99, 'bob',    '2024-06-01 14:00:00'),
    (29, 1006, 'order.delivered',   399.99, 'courier','2024-06-01 19:00:00'),
    (30, 1007, 'order.created',     55.00,  'henry',  '2024-06-01 11:00:00'),
    (31, 1007, 'order.confirmed',   55.00,  'system', '2024-06-01 11:05:00'),
    (32, 1007, 'payment.received',  55.00,  'paypal', '2024-06-01 11:15:00'),
    (33, 1008, 'order.created',     30.00,  'irene',  '2024-06-01 11:30:00'),
    (34, 1008, 'order.cancelled',   30.00,  'irene',  '2024-06-01 11:35:00'),
    (35, 1009, 'order.created',     175.50, 'jack',   '2024-06-01 12:00:00');

-- Batch 2: 25 events (orders 1009-1014)
INSERT INTO {{zone_name}}.delta_demos.order_events VALUES
    (36, 1009, 'order.confirmed',   175.50, 'system', '2024-06-01 12:05:00'),
    (37, 1009, 'payment.received',  175.50, 'stripe', '2024-06-01 12:10:00'),
    (38, 1009, 'order.shipped',     175.50, 'bob',    '2024-06-01 15:00:00'),
    (39, 1009, 'order.delivered',   175.50, 'courier','2024-06-01 20:00:00'),
    (40, 1010, 'order.created',     210.00, 'karen',  '2024-06-01 12:30:00'),
    (41, 1010, 'order.confirmed',   210.00, 'system', '2024-06-01 12:35:00'),
    (42, 1010, 'payment.received',  210.00, 'stripe', '2024-06-01 12:40:00'),
    (43, 1010, 'order.shipped',     210.00, 'bob',    '2024-06-01 15:30:00'),
    (44, 1010, 'order.delivered',   210.00, 'courier','2024-06-01 20:30:00'),
    (45, 1011, 'order.created',     92.00,  'leo',    '2024-06-01 13:00:00'),
    (46, 1011, 'order.confirmed',   92.00,  'system', '2024-06-01 13:05:00'),
    (47, 1011, 'payment.received',  92.00,  'stripe', '2024-06-01 13:10:00'),
    (48, 1011, 'order.shipped',     92.00,  'bob',    '2024-06-01 16:00:00'),
    (49, 1012, 'order.created',     315.00, 'mike',   '2024-06-01 13:30:00'),
    (50, 1012, 'order.confirmed',   315.00, 'system', '2024-06-01 13:35:00'),
    (51, 1012, 'payment.received',  315.00, 'stripe', '2024-06-01 13:40:00'),
    (52, 1012, 'order.cancelled',   315.00, 'mike',   '2024-06-01 14:00:00'),
    (53, 1012, 'payment.refunded',  315.00, 'stripe', '2024-06-01 14:15:00'),
    (54, 1013, 'order.created',     48.75,  'nancy',  '2024-06-01 14:00:00'),
    (55, 1013, 'order.confirmed',   48.75,  'system', '2024-06-01 14:05:00'),
    (56, 1013, 'payment.received',  48.75,  'stripe', '2024-06-01 14:10:00'),
    (57, 1013, 'order.shipped',     48.75,  'bob',    '2024-06-01 17:00:00'),
    (58, 1013, 'order.delivered',   48.75,  'courier','2024-06-01 21:00:00'),
    (59, 1014, 'order.created',     125.00, 'olivia', '2024-06-01 14:30:00'),
    (60, 1014, 'order.confirmed',   125.00, 'system', '2024-06-01 14:35:00');
