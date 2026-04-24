-- ============================================================================
-- Delta GDPR Data Erasure — Right to Erasure Lifecycle — Setup Script
-- ============================================================================
-- Creates a bank customer_accounts table with 30 accounts including PII
-- columns (ssn, phone, mailing_address). The queries.sql script performs
-- the full GDPR erasure lifecycle.
--
-- Tables created:
--   1. customer_accounts — 30 bank accounts with PII data
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: customer_accounts — 30 bank accounts with PII columns
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customer_accounts (
    id               INT,
    account_holder   VARCHAR,
    email            VARCHAR,
    ssn              VARCHAR,
    phone            VARCHAR,
    mailing_address  VARCHAR,
    account_type     VARCHAR,
    branch_city      VARCHAR,
    country          VARCHAR,
    opened_date      VARCHAR,
    balance          DECIMAL(12,2)
) LOCATION 'customer_accounts';


-- STEP 2: Insert 30 customer accounts with full PII
INSERT INTO {{zone_name}}.delta_demos.customer_accounts VALUES
    (1,  'Alice Monroe',   'alice@bankmail.com',   '123-45-6789', '+1-555-0201', '100 Wall St, NY',       'checking',   'New York',      'US', '2022-01-10', 15420.50),
    (2,  'Bob Chen',       'bob@bankmail.com',     '234-56-7890', '+1-555-0202', '200 State St, Chicago', 'savings',    'Chicago',       'US', '2022-02-15', 82300.00),
    (3,  'Carol Dupont',   'carol@bankmail.com',   '345-67-8901', '+33-1-0303',  '15 Rue Rivoli, Paris',  'investment', 'Paris',         'FR', '2022-03-20', 250000.75),
    (4,  'David Kim',      'david@bankmail.com',   '456-78-9012', '+82-2-0404',  '88 Gangnam Ro, Seoul',  'checking',   'Seoul',         'KR', '2022-04-05', 34100.00),
    (5,  'Eva Martinez',   'eva@bankmail.com',     '567-89-0123', '+34-91-0505', '42 Gran Via, Madrid',   'savings',    'Madrid',        'ES', '2022-05-12', 67500.25),
    (6,  'Frank Weber',    'frank@bankmail.com',   '678-90-1234', '+49-30-0606', '7 Unter den Linden',    'checking',   'Berlin',        'DE', '2022-06-01', 19800.00),
    (7,  'Grace Tanaka',   'grace@bankmail.com',   '789-01-2345', '+81-3-0707',  '3 Shibuya, Tokyo',      'investment', 'Tokyo',         'JP', '2022-07-18', 445000.00),
    (8,  'Henry Okafor',   'henry@bankmail.com',   '890-12-3456', '+234-1-0808', '12 Victoria Is, Lagos', 'savings',    'Lagos',         'NG', '2022-08-22', 28750.50),
    (9,  'Irene Costa',    'irene@bankmail.com',   '901-23-4567', '+55-11-0909', '50 Paulista Ave, SP',   'checking',   'Sao Paulo',     'BR', '2022-09-10', 41200.00),
    (10, 'Jack Thompson',  'jack@bankmail.com',    '012-34-5678', '+1-555-0210', '300 Market St, SF',     'investment', 'San Francisco', 'US', '2022-10-05', 189000.00),
    (11, 'Karen Liu',      'karen@bankmail.com',   '111-22-3333', '+86-21-1111', '28 Nanjing Rd',         'savings',    'Shanghai',      'CN', '2022-11-15', 156000.00),
    (12, 'Leo Rossi',      'leo@bankmail.com',     '222-33-4444', '+39-06-1212', '5 Via Veneto, Rome',    'checking',   'Rome',          'IT', '2023-01-08', 22400.00),
    (13, 'Maria Santos',   'maria@bankmail.com',   '333-44-5555', '+351-21-1313','90 Av Liberdade',       'savings',    'Lisbon',        'PT', '2023-02-20', 73800.50),
    (14, 'Nick Petrov',    'nick@bankmail.com',    '444-55-6666', '+7-495-1414', '18 Tverskaya, Moscow',  'investment', 'Moscow',        'RU', '2023-03-15', 310000.00),
    (15, 'Olivia Berg',    'olivia@bankmail.com',  '555-66-7777', '+46-8-1515',  '6 Drottninggatan',      'checking',   'Stockholm',     'SE', '2023-04-01', 48900.00);

INSERT INTO {{zone_name}}.delta_demos.customer_accounts
SELECT * FROM (VALUES
    (16, 'Paul Singh',     'paul@bankmail.com',    '666-77-8888', '+91-22-1616', '14 MG Road, Mumbai',    'savings',    'Mumbai',        'IN', '2023-05-10', 95200.00),
    (17, 'Quinn O''Brien', 'quinn@bankmail.com',   '777-88-9999', '+353-1-1717', '8 Temple Bar, Dublin',  'checking',   'Dublin',        'IE', '2023-06-22', 31600.00),
    (18, 'Rachel Stern',   'rachel@bankmail.com',  '888-99-0000', '+972-3-1818', '22 Rothschild Blvd',    'investment', 'Tel Aviv',      'IL', '2023-07-05', 178500.00),
    (19, 'Sam Al-Rashid',  'sam@bankmail.com',     '999-00-1111', '+971-4-1919', '1 Sheikh Zayed Rd',     'savings',    'Dubai',         'AE', '2023-08-18', 520000.00),
    (20, 'Tina Müller',    'tina@bankmail.com',    '000-11-2222', '+41-44-2020', '10 Bahnhofstr, Zurich', 'checking',   'Zurich',        'CH', '2023-09-30', 87600.00),
    (21, 'Uma Patel',      'uma@bankmail.com',     '121-21-3131', '+91-80-2121', '55 Brigade Rd',         'savings',    'Bangalore',     'IN', '2023-10-12', 64300.00),
    (22, 'Victor Novak',   'victor@bankmail.com',  '232-32-4242', '+420-2-2222', '3 Wenceslas Sq',        'checking',   'Prague',        'CZ', '2023-11-25', 27100.00),
    (23, 'Wendy Zhao',     'wendy@bankmail.com',   '343-43-5353', '+86-10-2323', '99 Chang An Ave',       'investment', 'Beijing',       'CN', '2024-01-08', 385000.00),
    (24, 'Xavier Diaz',    'xavier@bankmail.com',  '454-54-6464', '+52-55-2424', '40 Reforma Ave',        'savings',    'Mexico City',   'MX', '2024-02-14', 51700.00),
    (25, 'Yuki Sato',      'yuki@bankmail.com',    '565-65-7575', '+81-6-2525',  '15 Namba, Osaka',       'checking',   'Osaka',         'JP', '2024-03-01', 39200.00),
    (26, 'Zara Ahmed',     'zara@bankmail.com',    '676-76-8686', '+92-21-2626', '7 Clifton Rd',          'investment', 'Karachi',       'PK', '2024-04-10', 142000.00),
    (27, 'Adam Fischer',   'adam@bankmail.com',     '787-87-9797', '+43-1-2727',  '12 Ringstrasse',        'savings',    'Vienna',        'AT', '2024-05-20', 109500.00),
    (28, 'Beth Larsson',   'beth@bankmail.com',    '898-98-0808', '+46-31-2828', '4 Avenyn, Gothenburg',  'checking',   'Gothenburg',    'SE', '2024-06-15', 56800.00),
    (29, 'Chris Oduya',    'chris@bankmail.com',   '909-09-1919', '+254-20-2929','20 Kenyatta Ave',       'savings',    'Nairobi',       'KE', '2024-07-22', 33400.00),
    (30, 'Diana Volkov',   'diana@bankmail.com',   '010-10-3030', '+380-44-3030','5 Khreschatyk St',      'investment', 'Kyiv',          'UA', '2024-08-30', 198000.00)
) AS t(id, account_holder, email, ssn, phone, mailing_address, account_type, branch_city, country, opened_date, balance);
