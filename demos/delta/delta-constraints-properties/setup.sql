-- ============================================================================
-- Delta Constraints & Table Properties — Setup Script
-- ============================================================================
-- Demonstrates:
--   1. CHECK constraints for data quality enforcement
--   2. TBLPROPERTIES for table-level settings (append-only)
--
-- Tables:
--   1. invoices  — 30 invoices with CHECK constraints on amounts
--   2. event_log — 50 events with append-only property
--
-- CHECK constraints on invoices:
--   - amount_positive: amount > 0
--   - tax_non_negative: tax >= 0
--   - total_positive: total > 0
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: invoices — with CHECK constraints
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.invoices (
    id           INT,
    customer     VARCHAR,
    amount       DOUBLE,
    tax          DOUBLE,
    total        DOUBLE,
    status       VARCHAR,
    created_date VARCHAR
) LOCATION 'delta-constraints-properties/invoices'
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.constraints.amount_positive' = 'amount > 0',
    'delta.constraints.tax_non_negative' = 'tax >= 0',
    'delta.constraints.total_positive' = 'total > 0'
);


-- Insert 30 valid invoices (all satisfy constraints)
INSERT INTO {{zone_name}}.delta_demos.invoices VALUES
    (1,  'Acme Corp',        100.00, 10.00,  110.00,  'paid',    '2024-01-05'),
    (2,  'Beta Industries',  250.00, 25.00,  275.00,  'paid',    '2024-01-06'),
    (3,  'Gamma LLC',        75.50,  7.55,   83.05,   'pending', '2024-01-07'),
    (4,  'Delta Co',         180.00, 18.00,  198.00,  'paid',    '2024-01-08'),
    (5,  'Epsilon Inc',      45.00,  4.50,   49.50,   'pending', '2024-01-09'),
    (6,  'Zeta Group',       320.00, 32.00,  352.00,  'paid',    '2024-01-10'),
    (7,  'Eta Partners',     90.00,  9.00,   99.00,   'overdue', '2024-01-11'),
    (8,  'Theta Corp',       500.00, 50.00,  550.00,  'paid',    '2024-01-12'),
    (9,  'Iota Ltd',         150.00, 15.00,  165.00,  'pending', '2024-01-13'),
    (10, 'Kappa Systems',    200.00, 20.00,  220.00,  'paid',    '2024-01-14'),
    (11, 'Lambda Tech',      85.00,  8.50,   93.50,   'paid',    '2024-01-15'),
    (12, 'Mu Dynamics',      425.00, 42.50,  467.50,  'pending', '2024-01-16'),
    (13, 'Nu Services',      60.00,  6.00,   66.00,   'paid',    '2024-01-17'),
    (14, 'Xi Solutions',     310.00, 31.00,  341.00,  'overdue', '2024-01-18'),
    (15, 'Omicron Labs',     175.00, 17.50,  192.50,  'paid',    '2024-01-19'),
    (16, 'Pi Ventures',      220.00, 22.00,  242.00,  'paid',    '2024-01-20'),
    (17, 'Rho Capital',      95.00,  9.50,   104.50,  'pending', '2024-01-21'),
    (18, 'Sigma Global',     600.00, 60.00,  660.00,  'paid',    '2024-01-22'),
    (19, 'Tau Logistics',    130.00, 13.00,  143.00,  'overdue', '2024-01-23'),
    (20, 'Upsilon Media',    280.00, 28.00,  308.00,  'paid',    '2024-01-24'),
    (21, 'Phi Analytics',    350.00, 35.00,  385.00,  'paid',    '2024-01-25'),
    (22, 'Chi Robotics',     110.00, 11.00,  121.00,  'pending', '2024-01-26'),
    (23, 'Psi Energy',       450.00, 45.00,  495.00,  'paid',    '2024-01-27'),
    (24, 'Omega Design',     70.00,  7.00,   77.00,   'paid',    '2024-01-28'),
    (25, 'Alpha Prime',      190.00, 19.00,  209.00,  'pending', '2024-01-29'),
    (26, 'Nova Systems',     265.00, 26.50,  291.50,  'paid',    '2024-01-30'),
    (27, 'Stellar Inc',      140.00, 14.00,  154.00,  'overdue', '2024-01-31'),
    (28, 'Cosmos Corp',      380.00, 38.00,  418.00,  'paid',    '2024-02-01'),
    (29, 'Galaxy Ltd',       55.00,  5.50,   60.50,   'pending', '2024-02-02'),
    (30, 'Nebula Tech',      475.00, 47.50,  522.50,  'paid',    '2024-02-03');


-- ============================================================================
-- TABLE 2: event_log — append-only immutable log
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.event_log (
    id         BIGINT,
    event_type VARCHAR,
    payload    VARCHAR,
    created_at VARCHAR
) LOCATION 'delta-constraints-properties/event_log'
TBLPROPERTIES (
    'delta.appendOnly' = 'true'
);


-- Insert 50 events in 2 batches
INSERT INTO {{zone_name}}.delta_demos.event_log VALUES
    (1,  'user.login',      '{"user":"alice","ip":"10.0.1.1"}',     '2024-03-01 08:00:00'),
    (2,  'user.login',      '{"user":"bob","ip":"10.0.1.2"}',       '2024-03-01 08:05:00'),
    (3,  'order.created',   '{"order_id":1001,"amount":99.99}',     '2024-03-01 08:10:00'),
    (4,  'payment.success', '{"order_id":1001,"method":"card"}',     '2024-03-01 08:11:00'),
    (5,  'user.login',      '{"user":"carol","ip":"10.0.1.3"}',     '2024-03-01 08:15:00'),
    (6,  'order.created',   '{"order_id":1002,"amount":249.50}',    '2024-03-01 08:20:00'),
    (7,  'user.logout',     '{"user":"alice"}',                      '2024-03-01 08:25:00'),
    (8,  'order.shipped',   '{"order_id":1001,"carrier":"FedEx"}',   '2024-03-01 08:30:00'),
    (9,  'payment.success', '{"order_id":1002,"method":"paypal"}',   '2024-03-01 08:31:00'),
    (10, 'user.login',      '{"user":"dave","ip":"10.0.1.4"}',      '2024-03-01 08:35:00'),
    (11, 'order.created',   '{"order_id":1003,"amount":50.00}',     '2024-03-01 08:40:00'),
    (12, 'user.logout',     '{"user":"bob"}',                        '2024-03-01 08:45:00'),
    (13, 'payment.failed',  '{"order_id":1003,"reason":"declined"}', '2024-03-01 08:46:00'),
    (14, 'order.cancelled', '{"order_id":1003}',                     '2024-03-01 08:50:00'),
    (15, 'user.login',      '{"user":"eve","ip":"10.0.1.5"}',       '2024-03-01 08:55:00'),
    (16, 'order.created',   '{"order_id":1004,"amount":175.00}',    '2024-03-01 09:00:00'),
    (17, 'payment.success', '{"order_id":1004,"method":"card"}',     '2024-03-01 09:01:00'),
    (18, 'user.logout',     '{"user":"carol"}',                      '2024-03-01 09:05:00'),
    (19, 'order.shipped',   '{"order_id":1002,"carrier":"UPS"}',     '2024-03-01 09:10:00'),
    (20, 'user.login',      '{"user":"frank","ip":"10.0.1.6"}',     '2024-03-01 09:15:00'),
    (21, 'order.created',   '{"order_id":1005,"amount":320.00}',    '2024-03-01 09:20:00'),
    (22, 'payment.success', '{"order_id":1005,"method":"wire"}',     '2024-03-01 09:21:00'),
    (23, 'user.logout',     '{"user":"dave"}',                       '2024-03-01 09:25:00'),
    (24, 'order.shipped',   '{"order_id":1004,"carrier":"DHL"}',     '2024-03-01 09:30:00'),
    (25, 'user.login',      '{"user":"grace","ip":"10.0.1.7"}',     '2024-03-01 09:35:00');

-- Batch 2: 25 more events
INSERT INTO {{zone_name}}.delta_demos.event_log VALUES
    (26, 'order.created',   '{"order_id":1006,"amount":89.99}',     '2024-03-01 09:40:00'),
    (27, 'payment.success', '{"order_id":1006,"method":"card"}',     '2024-03-01 09:41:00'),
    (28, 'user.logout',     '{"user":"eve"}',                        '2024-03-01 09:45:00'),
    (29, 'order.delivered',  '{"order_id":1001}',                    '2024-03-01 09:50:00'),
    (30, 'user.login',      '{"user":"henry","ip":"10.0.1.8"}',     '2024-03-01 09:55:00'),
    (31, 'order.created',   '{"order_id":1007,"amount":410.00}',    '2024-03-01 10:00:00'),
    (32, 'payment.success', '{"order_id":1007,"method":"card"}',     '2024-03-01 10:01:00'),
    (33, 'order.shipped',   '{"order_id":1005,"carrier":"FedEx"}',   '2024-03-01 10:05:00'),
    (34, 'user.logout',     '{"user":"frank"}',                      '2024-03-01 10:10:00'),
    (35, 'order.shipped',   '{"order_id":1006,"carrier":"UPS"}',     '2024-03-01 10:15:00'),
    (36, 'user.login',      '{"user":"alice","ip":"10.0.1.1"}',     '2024-03-01 10:20:00'),
    (37, 'order.delivered',  '{"order_id":1002}',                    '2024-03-01 10:25:00'),
    (38, 'order.created',   '{"order_id":1008,"amount":55.00}',     '2024-03-01 10:30:00'),
    (39, 'payment.success', '{"order_id":1008,"method":"paypal"}',   '2024-03-01 10:31:00'),
    (40, 'user.logout',     '{"user":"grace"}',                      '2024-03-01 10:35:00'),
    (41, 'order.shipped',   '{"order_id":1007,"carrier":"DHL"}',     '2024-03-01 10:40:00'),
    (42, 'order.shipped',   '{"order_id":1008,"carrier":"FedEx"}',   '2024-03-01 10:45:00'),
    (43, 'user.login',      '{"user":"irene","ip":"10.0.1.9"}',     '2024-03-01 10:50:00'),
    (44, 'order.delivered',  '{"order_id":1004}',                    '2024-03-01 10:55:00'),
    (45, 'order.delivered',  '{"order_id":1005}',                    '2024-03-01 11:00:00'),
    (46, 'user.logout',     '{"user":"henry"}',                      '2024-03-01 11:05:00'),
    (47, 'order.delivered',  '{"order_id":1006}',                    '2024-03-01 11:10:00'),
    (48, 'order.delivered',  '{"order_id":1007}',                    '2024-03-01 11:15:00'),
    (49, 'order.delivered',  '{"order_id":1008}',                    '2024-03-01 11:20:00'),
    (50, 'user.logout',     '{"user":"irene"}',                      '2024-03-01 11:25:00');

