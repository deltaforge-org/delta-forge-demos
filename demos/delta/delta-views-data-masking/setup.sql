-- ============================================================================
-- Delta Views & Data Masking — Role-Based Access Layers — Setup Script
-- ============================================================================
-- Creates a customer_orders table with 30 e-commerce orders including
-- sensitive fields (credit_card_last4, shipping_address, phone). The
-- queries.sql script builds role-based views on top.
--
-- Tables created:
--   1. customer_orders — 30 orders with PII and financial data
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: customer_orders — 30 e-commerce orders with sensitive fields
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customer_orders (
    id                INT,
    customer_name     VARCHAR,
    customer_email    VARCHAR,
    credit_card_last4 VARCHAR,
    shipping_address  VARCHAR,
    phone             VARCHAR,
    product           VARCHAR,
    quantity          INT,
    unit_price        DECIMAL(10,2),
    order_total       DECIMAL(10,2),
    order_status      VARCHAR,
    order_date        VARCHAR,
    region            VARCHAR
) LOCATION 'delta-views-data-masking/customer_orders';


-- STEP 2: Insert 30 orders spanning 6 regions, 7 products, 4 statuses
INSERT INTO {{zone_name}}.delta_demos.customer_orders VALUES
    (1,  'Alice Monroe',   'alice@shop.com',   '4532', '100 Wall St, New York',        '+1-555-0301', 'Laptop Pro 15',  1, 1299.99, 1299.99, 'delivered',  '2024-01-15', 'North America'),
    (2,  'Bob Chen',       'bob@shop.com',     '7891', '200 State St, Chicago',        '+1-555-0302', 'Wireless Mouse', 2,   29.99,   59.98, 'delivered',  '2024-01-20', 'North America'),
    (3,  'Carol Dupont',   'carol@shop.com',   '2345', '15 Rue Rivoli, Paris',         '+33-1-0303',  'Standing Desk',  1,  549.00,  549.00, 'shipped',    '2024-02-10', 'Europe'),
    (4,  'David Kim',      'david@shop.com',   '6789', '88 Gangnam Ro, Seoul',         '+82-2-0404',  'Monitor 27inch', 1,  399.99,  399.99, 'delivered',  '2024-02-14', 'Asia Pacific'),
    (5,  'Eva Martinez',   'eva@shop.com',     '0123', '42 Gran Via, Madrid',          '+34-91-0505', 'Keyboard Mech',  1,  149.99,  149.99, 'delivered',  '2024-03-01', 'Europe'),
    (6,  'Frank Weber',    'frank@shop.com',   '4567', '7 Unter den Linden, Berlin',   '+49-30-0606', 'USB-C Hub',      3,   45.99,  137.97, 'shipped',    '2024-03-12', 'Europe'),
    (7,  'Grace Tanaka',   'grace@shop.com',   '8901', '3 Shibuya, Tokyo',             '+81-3-0707',  'Laptop Pro 15',  1, 1299.99, 1299.99, 'delivered',  '2024-03-20', 'Asia Pacific'),
    (8,  'Henry Okafor',   'henry@shop.com',   '2345', '12 Victoria Is, Lagos',        '+234-1-0808', 'Webcam HD',      2,   79.99,  159.98, 'pending',    '2024-04-05', 'Africa'),
    (9,  'Irene Costa',    'irene@shop.com',   '6789', '50 Paulista Ave, Sao Paulo',   '+55-11-0909', 'Laptop Pro 15',  1, 1299.99, 1299.99, 'delivered',  '2024-04-15', 'South America'),
    (10, 'Jack Thompson',  'jack@shop.com',    '0123', '300 Market St, San Francisco', '+1-555-0310', 'Standing Desk',  1,  549.00,  549.00, 'returned',   '2024-04-22', 'North America'),
    (11, 'Karen Liu',      'karen@shop.com',   '4532', '28 Nanjing Rd, Shanghai',      '+86-21-1111', 'Monitor 27inch', 2,  399.99,  799.98, 'delivered',  '2024-05-01', 'Asia Pacific'),
    (12, 'Leo Rossi',      'leo@shop.com',     '7891', '5 Via Veneto, Rome',           '+39-06-1212', 'Wireless Mouse', 4,   29.99,  119.96, 'delivered',  '2024-05-10', 'Europe'),
    (13, 'Maria Santos',   'maria@shop.com',   '2345', '90 Av Liberdade, Lisbon',      '+351-21-1313','Keyboard Mech',  1,  149.99,  149.99, 'shipped',    '2024-05-18', 'Europe'),
    (14, 'Nick Petrov',    'nick@shop.com',    '6789', '18 Tverskaya, Moscow',         '+7-495-1414', 'USB-C Hub',      2,   45.99,   91.98, 'delivered',  '2024-06-02', 'Europe'),
    (15, 'Olivia Berg',    'olivia@shop.com',  '0123', '6 Drottninggatan, Stockholm',  '+46-8-1515',  'Webcam HD',      1,   79.99,   79.99, 'delivered',  '2024-06-15', 'Europe');

INSERT INTO {{zone_name}}.delta_demos.customer_orders
SELECT * FROM (VALUES
    (16, 'Paul Singh',     'paul@shop.com',    '4567', '14 MG Road, Mumbai',           '+91-22-1616', 'Laptop Pro 15',  1, 1299.99, 1299.99, 'pending',    '2024-07-01', 'Asia Pacific'),
    (17, 'Quinn O''Brien', 'quinn@shop.com',   '8901', '8 Temple Bar, Dublin',         '+353-1-1717', 'Standing Desk',  1,  549.00,  549.00, 'delivered',  '2024-07-10', 'Europe'),
    (18, 'Rachel Stern',   'rachel@shop.com',  '2345', '22 Rothschild Blvd, Tel Aviv', '+972-3-1818', 'Monitor 27inch', 1,  399.99,  399.99, 'shipped',    '2024-07-22', 'Middle East'),
    (19, 'Sam Al-Rashid',  'sam@shop.com',     '6789', '1 Sheikh Zayed Rd, Dubai',     '+971-4-1919', 'Laptop Pro 15',  2, 1299.99, 2599.98, 'delivered',  '2024-08-05', 'Middle East'),
    (20, 'Tina Müller',    'tina@shop.com',    '0123', '10 Bahnhofstr, Zurich',        '+41-44-2020', 'Keyboard Mech',  2,  149.99,  299.98, 'delivered',  '2024-08-18', 'Europe'),
    (21, 'Uma Patel',      'uma@shop.com',     '4532', '55 Brigade Rd, Bangalore',     '+91-80-2121', 'USB-C Hub',      1,   45.99,   45.99, 'delivered',  '2024-09-01', 'Asia Pacific'),
    (22, 'Victor Novak',   'victor@shop.com',  '7891', '3 Wenceslas Sq, Prague',       '+420-2-2222', 'Wireless Mouse', 3,   29.99,   89.97, 'returned',   '2024-09-12', 'Europe'),
    (23, 'Wendy Zhao',     'wendy@shop.com',   '2345', '99 Chang An Ave, Beijing',     '+86-10-2323', 'Webcam HD',      1,   79.99,   79.99, 'delivered',  '2024-09-25', 'Asia Pacific'),
    (24, 'Xavier Diaz',    'xavier@shop.com',  '6789', '40 Reforma Ave, Mexico City',  '+52-55-2424', 'Standing Desk',  1,  549.00,  549.00, 'shipped',    '2024-10-05', 'North America'),
    (25, 'Yuki Sato',      'yuki@shop.com',    '0123', '15 Namba, Osaka',              '+81-6-2525',  'Laptop Pro 15',  1, 1299.99, 1299.99, 'delivered',  '2024-10-18', 'Asia Pacific'),
    (26, 'Zara Ahmed',     'zara@shop.com',    '4567', '7 Clifton Rd, Karachi',        '+92-21-2626', 'Monitor 27inch', 1,  399.99,  399.99, 'pending',    '2024-11-01', 'Asia Pacific'),
    (27, 'Adam Fischer',   'adam@shop.com',     '8901', '12 Ringstrasse, Vienna',       '+43-1-2727',  'Keyboard Mech',  1,  149.99,  149.99, 'delivered',  '2024-11-15', 'Europe'),
    (28, 'Beth Larsson',   'beth@shop.com',    '2345', '4 Avenyn, Gothenburg',         '+46-31-2828', 'USB-C Hub',      2,   45.99,   91.98, 'delivered',  '2024-11-28', 'Europe'),
    (29, 'Chris Oduya',    'chris@shop.com',   '6789', '20 Kenyatta Ave, Nairobi',     '+254-20-2929','Wireless Mouse',  1,   29.99,   29.99, 'delivered',  '2024-12-05', 'Africa'),
    (30, 'Diana Volkov',   'diana@shop.com',   '0123', '5 Khreschatyk St, Kyiv',       '+380-44-3030','Standing Desk',  1,  549.00,  549.00, 'shipped',    '2024-12-15', 'Europe')
) AS t(id, customer_name, customer_email, credit_card_last4, shipping_address, phone, product, quantity, unit_price, order_total, order_status, order_date, region);
