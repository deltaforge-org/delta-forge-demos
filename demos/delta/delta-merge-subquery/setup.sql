-- ============================================================================
-- Delta MERGE — Subquery & CTE Source Patterns — Setup Script
-- ============================================================================
-- Creates the target summary table and raw events table for the MERGE demo.
--
-- Tables:
--   1. daily_revenue — 15 existing summary rows (5 products x 3 days)
--   2. order_events  — 40 raw order events (35 unique + 5 duplicates)
--
-- The MERGE in queries.sql will:
--   - Use a CTE to deduplicate events (remove 5 duplicate rows)
--   - Aggregate 35 unique orders into 10 product+date buckets
--   - UPDATE 5 existing summary rows (products on 2024-03-03)
--   - INSERT 5 new summary rows (products on 2024-03-04)
--   - Final count: 15 + 5 = 20
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: daily_revenue — 15 existing summary rows (target)
-- ============================================================================
-- Pre-populated with 3 days of revenue data for 5 products.
-- These represent yesterday's batch run output. Today's events will be
-- merged into this table to keep the summary up to date.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.daily_revenue (
    product_id      VARCHAR,
    product_name    VARCHAR,
    sale_date       VARCHAR,
    total_revenue   DOUBLE,
    order_count     INT,
    avg_order_value DOUBLE,
    last_updated    VARCHAR
) LOCATION '{{data_path}}/daily_revenue';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.daily_revenue TO USER {{current_user}};

INSERT INTO {{zone_name}}.delta_demos.daily_revenue VALUES
    -- PROD-001: Wireless Headphones ($49.99 - $79.99)
    ('PROD-001', 'Wireless Headphones', '2024-03-01',  399.92, 6, 66.65,  '2024-03-04 00:00:00'),
    ('PROD-001', 'Wireless Headphones', '2024-03-02',  479.91, 7, 68.56,  '2024-03-04 00:00:00'),
    ('PROD-001', 'Wireless Headphones', '2024-03-03',  319.94, 5, 63.99,  '2024-03-04 00:00:00'),
    -- PROD-002: Smart Watch ($149.99 - $249.99)
    ('PROD-002', 'Smart Watch',         '2024-03-01',  749.97, 4, 187.49, '2024-03-04 00:00:00'),
    ('PROD-002', 'Smart Watch',         '2024-03-02',  599.98, 3, 200.00, '2024-03-04 00:00:00'),
    ('PROD-002', 'Smart Watch',         '2024-03-03',  949.96, 5, 190.00, '2024-03-04 00:00:00'),
    -- PROD-003: Laptop Sleeve ($24.99 - $39.99)
    ('PROD-003', 'Laptop Sleeve',       '2024-03-01',  179.94, 6, 29.99,  '2024-03-04 00:00:00'),
    ('PROD-003', 'Laptop Sleeve',       '2024-03-02',  209.93, 7, 29.99,  '2024-03-04 00:00:00'),
    ('PROD-003', 'Laptop Sleeve',       '2024-03-03',  149.95, 5, 29.99,  '2024-03-04 00:00:00'),
    -- PROD-004: USB-C Dock ($79.99 - $99.99)
    ('PROD-004', 'USB-C Dock',          '2024-03-01',  359.96, 4, 89.99,  '2024-03-04 00:00:00'),
    ('PROD-004', 'USB-C Dock',          '2024-03-02',  449.95, 5, 89.99,  '2024-03-04 00:00:00'),
    ('PROD-004', 'USB-C Dock',          '2024-03-03',  269.97, 3, 89.99,  '2024-03-04 00:00:00'),
    -- PROD-005: Mechanical Keyboard ($119.99 - $139.99)
    ('PROD-005', 'Mechanical Keyboard', '2024-03-01',  509.94, 4, 127.49, '2024-03-04 00:00:00'),
    ('PROD-005', 'Mechanical Keyboard', '2024-03-02',  764.91, 6, 127.49, '2024-03-04 00:00:00'),
    ('PROD-005', 'Mechanical Keyboard', '2024-03-03',  382.47, 3, 127.49, '2024-03-04 00:00:00');


-- ============================================================================
-- TABLE 2: order_events — 40 raw order events (source)
-- ============================================================================
-- Today's incoming events from multiple channels. Contains:
--   - 10 unique orders for 2024-03-03 (will UPDATE existing summary rows)
--   - 25 unique orders for 2024-03-04 (will INSERT new summary rows)
--   - 5 duplicate events (same order_id, timestamp differs by 1 second)
--     simulating at-least-once delivery from event streaming
--
-- Duplicates: ORD-301, ORD-306, ORD-410, ORD-415, ORD-421
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.order_events (
    event_id        VARCHAR,
    product_id      VARCHAR,
    product_name    VARCHAR,
    order_id        VARCHAR,
    quantity        INT,
    unit_price      DOUBLE,
    event_timestamp VARCHAR,
    channel         VARCHAR,
    region          VARCHAR
) LOCATION '{{data_path}}/order_events';

GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.order_events TO USER {{current_user}};

INSERT INTO {{zone_name}}.delta_demos.order_events VALUES
    -- =====================================================================
    -- 2024-03-03 events (additional orders for existing summary dates)
    -- 10 unique orders, 2 per product
    -- =====================================================================
    -- PROD-001: Wireless Headphones on 2024-03-03
    -- batch_revenue = (2*59.99) + (1*79.99) = 119.98 + 79.99 = 199.97
    ('EVT-1001', 'PROD-001', 'Wireless Headphones', 'ORD-301', 2, 59.99, '2024-03-03 14:22:10', 'web',    'US-East'),
    ('EVT-1002', 'PROD-001', 'Wireless Headphones', 'ORD-302', 1, 79.99, '2024-03-03 15:45:30', 'mobile', 'US-West'),
    -- DUPLICATE of ORD-301 (at-least-once delivery, 1 second later)
    ('EVT-1003', 'PROD-001', 'Wireless Headphones', 'ORD-301', 2, 59.99, '2024-03-03 14:22:11', 'web',    'US-East'),

    -- PROD-002: Smart Watch on 2024-03-03
    -- batch_revenue = (1*199.99) + (1*149.99) = 349.98
    ('EVT-1004', 'PROD-002', 'Smart Watch',         'ORD-303', 1, 199.99, '2024-03-03 09:10:05', 'api',    'EU'),
    ('EVT-1005', 'PROD-002', 'Smart Watch',         'ORD-304', 1, 149.99, '2024-03-03 11:30:20', 'web',    'APAC'),

    -- PROD-003: Laptop Sleeve on 2024-03-03
    -- batch_revenue = (3*29.99) + (2*34.99) = 89.97 + 69.98 = 159.95
    ('EVT-1006', 'PROD-003', 'Laptop Sleeve',       'ORD-305', 3, 29.99, '2024-03-03 08:15:00', 'mobile', 'US-East'),
    ('EVT-1007', 'PROD-003', 'Laptop Sleeve',       'ORD-306', 2, 34.99, '2024-03-03 16:40:45', 'web',    'US-West'),
    -- DUPLICATE of ORD-306 (at-least-once delivery, 1 second later)
    ('EVT-1008', 'PROD-003', 'Laptop Sleeve',       'ORD-306', 2, 34.99, '2024-03-03 16:40:46', 'web',    'US-West'),

    -- PROD-004: USB-C Dock on 2024-03-03
    -- batch_revenue = (1*89.99) + (2*89.99) = 89.99 + 179.98 = 269.97
    ('EVT-1009', 'PROD-004', 'USB-C Dock',          'ORD-307', 1, 89.99, '2024-03-03 10:05:30', 'api',    'EU'),
    ('EVT-1010', 'PROD-004', 'USB-C Dock',          'ORD-308', 2, 89.99, '2024-03-03 13:20:15', 'web',    'US-East'),

    -- PROD-005: Mechanical Keyboard on 2024-03-03
    -- batch_revenue = (1*129.99) + (2*124.99) = 129.99 + 249.98 = 379.97
    ('EVT-1011', 'PROD-005', 'Mechanical Keyboard', 'ORD-309', 1, 129.99, '2024-03-03 12:00:00', 'mobile', 'APAC'),
    ('EVT-1012', 'PROD-005', 'Mechanical Keyboard', 'ORD-310', 2, 124.99, '2024-03-03 17:55:30', 'web',    'US-West'),

    -- =====================================================================
    -- 2024-03-04 events (new day — will INSERT new summary rows)
    -- 25 unique orders, 5 per product
    -- =====================================================================
    -- PROD-001: Wireless Headphones on 2024-03-04
    -- batch_revenue = 49.99 + 119.98 + 69.99 + 149.97 + 79.99 = 469.92
    ('EVT-2001', 'PROD-001', 'Wireless Headphones', 'ORD-401', 1, 49.99,  '2024-03-04 08:10:00', 'web',    'US-East'),
    ('EVT-2002', 'PROD-001', 'Wireless Headphones', 'ORD-402', 2, 59.99,  '2024-03-04 09:25:15', 'mobile', 'US-West'),
    ('EVT-2003', 'PROD-001', 'Wireless Headphones', 'ORD-403', 1, 69.99,  '2024-03-04 10:40:30', 'api',    'EU'),
    ('EVT-2004', 'PROD-001', 'Wireless Headphones', 'ORD-404', 3, 49.99,  '2024-03-04 12:15:45', 'web',    'APAC'),
    ('EVT-2005', 'PROD-001', 'Wireless Headphones', 'ORD-405', 1, 79.99,  '2024-03-04 14:30:00', 'mobile', 'US-East'),

    -- PROD-002: Smart Watch on 2024-03-04
    -- batch_revenue = 199.99 + 249.99 + 299.98 + 179.99 + 229.99 = 1159.94
    ('EVT-2006', 'PROD-002', 'Smart Watch',         'ORD-406', 1, 199.99, '2024-03-04 08:30:00', 'web',    'US-West'),
    ('EVT-2007', 'PROD-002', 'Smart Watch',         'ORD-407', 1, 249.99, '2024-03-04 09:45:20', 'api',    'EU'),
    ('EVT-2008', 'PROD-002', 'Smart Watch',         'ORD-408', 2, 149.99, '2024-03-04 11:00:10', 'mobile', 'APAC'),
    ('EVT-2009', 'PROD-002', 'Smart Watch',         'ORD-409', 1, 179.99, '2024-03-04 13:15:40', 'web',    'US-East'),
    ('EVT-2010', 'PROD-002', 'Smart Watch',         'ORD-410', 1, 229.99, '2024-03-04 15:30:55', 'mobile', 'US-West'),
    -- DUPLICATE of ORD-410 (at-least-once delivery, 1 second later)
    ('EVT-2011', 'PROD-002', 'Smart Watch',         'ORD-410', 1, 229.99, '2024-03-04 15:30:56', 'mobile', 'US-West'),

    -- PROD-003: Laptop Sleeve on 2024-03-04
    -- batch_revenue = 59.98 + 104.97 + 24.99 + 119.96 + 79.98 = 389.88
    ('EVT-2012', 'PROD-003', 'Laptop Sleeve',       'ORD-411', 2, 29.99,  '2024-03-04 07:50:00', 'api',    'EU'),
    ('EVT-2013', 'PROD-003', 'Laptop Sleeve',       'ORD-412', 3, 34.99,  '2024-03-04 09:05:30', 'web',    'US-East'),
    ('EVT-2014', 'PROD-003', 'Laptop Sleeve',       'ORD-413', 1, 24.99,  '2024-03-04 10:20:45', 'mobile', 'US-West'),
    ('EVT-2015', 'PROD-003', 'Laptop Sleeve',       'ORD-414', 4, 29.99,  '2024-03-04 12:35:10', 'web',    'APAC'),
    ('EVT-2016', 'PROD-003', 'Laptop Sleeve',       'ORD-415', 2, 39.99,  '2024-03-04 14:50:25', 'api',    'US-East'),
    -- DUPLICATE of ORD-415 (at-least-once delivery, 1 second later)
    ('EVT-2017', 'PROD-003', 'Laptop Sleeve',       'ORD-415', 2, 39.99,  '2024-03-04 14:50:26', 'api',    'US-East'),

    -- PROD-004: USB-C Dock on 2024-03-04
    -- batch_revenue = 89.99 + 189.98 + 79.99 + 269.97 + 99.99 = 729.92
    ('EVT-2018', 'PROD-004', 'USB-C Dock',          'ORD-416', 1, 89.99,  '2024-03-04 08:00:15', 'mobile', 'EU'),
    ('EVT-2019', 'PROD-004', 'USB-C Dock',          'ORD-417', 2, 94.99,  '2024-03-04 09:15:30', 'web',    'US-East'),
    ('EVT-2020', 'PROD-004', 'USB-C Dock',          'ORD-418', 1, 79.99,  '2024-03-04 10:30:45', 'api',    'APAC'),
    ('EVT-2021', 'PROD-004', 'USB-C Dock',          'ORD-419', 3, 89.99,  '2024-03-04 12:45:00', 'web',    'US-West'),
    ('EVT-2022', 'PROD-004', 'USB-C Dock',          'ORD-420', 1, 99.99,  '2024-03-04 15:00:15', 'mobile', 'US-East'),

    -- PROD-005: Mechanical Keyboard on 2024-03-04
    -- batch_revenue = 129.99 + 239.98 + 139.99 + 134.99 + 259.98 = 904.93
    ('EVT-2023', 'PROD-005', 'Mechanical Keyboard', 'ORD-421', 1, 129.99, '2024-03-04 08:20:00', 'web',    'US-East'),
    -- DUPLICATE of ORD-421 (at-least-once delivery, 1 second later)
    ('EVT-2024', 'PROD-005', 'Mechanical Keyboard', 'ORD-421', 1, 129.99, '2024-03-04 08:20:01', 'web',    'US-East'),
    ('EVT-2025', 'PROD-005', 'Mechanical Keyboard', 'ORD-422', 2, 119.99, '2024-03-04 09:35:20', 'mobile', 'US-West'),
    ('EVT-2026', 'PROD-005', 'Mechanical Keyboard', 'ORD-423', 1, 139.99, '2024-03-04 11:50:40', 'api',    'EU'),
    ('EVT-2027', 'PROD-005', 'Mechanical Keyboard', 'ORD-424', 1, 134.99, '2024-03-04 14:05:55', 'web',    'APAC'),
    ('EVT-2028', 'PROD-005', 'Mechanical Keyboard', 'ORD-425', 2, 129.99, '2024-03-04 16:20:10', 'mobile', 'US-East');
