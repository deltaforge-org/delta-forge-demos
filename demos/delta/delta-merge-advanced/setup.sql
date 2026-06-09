-- ============================================================================
-- Delta MERGE — Advanced Patterns & Conditional Logic — Setup Script
-- ============================================================================
-- Creates two tables for the MERGE demo:
--   1. inventory_master  — 40 baseline products
--   2. inventory_updates — 30 staging rows (mixed updates, deletes, inserts)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: inventory_master — Product inventory (40 baseline products)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.inventory_master (
    id              INT,
    sku             VARCHAR,
    name            VARCHAR,
    category        VARCHAR,
    price           DOUBLE,
    qty             INT,
    supplier        VARCHAR,
    last_updated    VARCHAR
) LOCATION 'delta-merge-advanced/inventory_master';


INSERT INTO {{zone_name}}.delta_demos.inventory_master VALUES
    (1,  'ELEC-001', 'Wireless Mouse',       'Electronics',  29.99,  150, 'TechCorp',    '2024-01-01'),
    (2,  'ELEC-002', 'USB-C Hub',            'Electronics',  49.99,  80,  'TechCorp',    '2024-01-01'),
    (3,  'ELEC-003', 'Bluetooth Speaker',    'Electronics',  79.99,  60,  'AudioMax',    '2024-01-01'),
    (4,  'ELEC-004', 'Webcam HD',            'Electronics',  59.99,  45,  'TechCorp',    '2024-01-01'),
    (5,  'ELEC-005', 'Keyboard Mechanical',  'Electronics',  89.99,  35,  'KeyPro',      '2024-01-01'),
    (6,  'FURN-001', 'Office Chair',         'Furniture',    249.99, 25,  'ComfortPlus', '2024-01-01'),
    (7,  'FURN-002', 'Standing Desk',        'Furniture',    399.99, 15,  'DeskWorks',   '2024-01-01'),
    (8,  'FURN-003', 'Monitor Arm',          'Furniture',    69.99,  40,  'DeskWorks',   '2024-01-01'),
    (9,  'FURN-004', 'Cable Tray',           'Furniture',    24.99,  100, 'DeskWorks',   '2024-01-01'),
    (10, 'FURN-005', 'Desk Lamp',            'Furniture',    34.99,  70,  'LightCo',     '2024-01-01'),
    (11, 'BOOK-001', 'SQL Fundamentals',     'Books',        44.99,  200, 'TechBooks',   '2024-01-01'),
    (12, 'BOOK-002', 'Data Engineering',     'Books',        54.99,  120, 'TechBooks',   '2024-01-01'),
    (13, 'BOOK-003', 'Cloud Architecture',   'Books',        49.99,  90,  'CloudPress',  '2024-01-01'),
    (14, 'BOOK-004', 'Python Cookbook',       'Books',        39.99,  180, 'TechBooks',   '2024-01-01'),
    (15, 'BOOK-005', 'Machine Learning',     'Books',        59.99,  75,  'CloudPress',  '2024-01-01'),
    (16, 'ELEC-006', 'Monitor 27"',          'Electronics',  349.99, 20,  'DisplayTech', '2024-01-01'),
    (17, 'ELEC-007', 'Headphones ANC',       'Electronics',  199.99, 50,  'AudioMax',    '2024-01-01'),
    (18, 'ELEC-008', 'Power Bank',           'Electronics',  39.99,  200, 'PowerUp',     '2024-01-01'),
    (19, 'ELEC-009', 'USB Cable Pack',       'Electronics',  14.99,  300, 'TechCorp',    '2024-01-01'),
    (20, 'ELEC-010', 'Laptop Stand',         'Electronics',  44.99,  85,  'DeskWorks',   '2024-01-01'),
    (21, 'SUPP-001', 'Sticky Notes',         'Supplies',     5.99,   500, 'OfficePro',   '2024-01-01'),
    (22, 'SUPP-002', 'Pen Set',              'Supplies',     12.99,  350, 'OfficePro',   '2024-01-01'),
    (23, 'SUPP-003', 'Notebook A5',          'Supplies',     8.99,   400, 'OfficePro',   '2024-01-01'),
    (24, 'SUPP-004', 'Whiteboard Markers',   'Supplies',     9.99,   250, 'OfficePro',   '2024-01-01'),
    (25, 'SUPP-005', 'Paper Clips',          'Supplies',     3.99,   600, 'OfficePro',   '2024-01-01'),
    (26, 'FURN-006', 'Filing Cabinet',       'Furniture',    149.99, 30,  'ComfortPlus', '2024-01-01'),
    (27, 'FURN-007', 'Bookshelf',            'Furniture',    119.99, 20,  'ComfortPlus', '2024-01-01'),
    (28, 'FURN-008', 'Whiteboard Large',     'Furniture',    89.99,  15,  'OfficePro',   '2024-01-01'),
    (29, 'BOOK-006', 'DevOps Handbook',      'Books',        42.99,  110, 'TechBooks',   '2024-01-01'),
    (30, 'BOOK-007', 'System Design',        'Books',        47.99,  95,  'CloudPress',  '2024-01-01'),
    (31, 'ELEC-011', 'HDMI Cable',           'Electronics',  12.99,  400, 'TechCorp',    '2024-01-01'),
    (32, 'ELEC-012', 'Mouse Pad XL',         'Electronics',  19.99,  150, 'TechCorp',    '2024-01-01'),
    (33, 'SUPP-006', 'Desk Organizer',       'Supplies',     15.99,  180, 'OfficePro',   '2024-01-01'),
    (34, 'SUPP-007', 'Binder Set',           'Supplies',     11.99,  220, 'OfficePro',   '2024-01-01'),
    (35, 'SUPP-008', 'Label Maker',          'Supplies',     29.99,  60,  'OfficePro',   '2024-01-01'),
    (36, 'FURN-009', 'Foot Rest',            'Furniture',    39.99,  50,  'ComfortPlus', '2024-01-01'),
    (37, 'FURN-010', 'Privacy Screen',       'Furniture',    54.99,  35,  'DisplayTech', '2024-01-01'),
    (38, 'BOOK-008', 'Rust Programming',     'Books',        44.99,  85,  'TechBooks',   '2024-01-01'),
    (39, 'ELEC-013', 'Surge Protector',      'Electronics',  24.99,  120, 'PowerUp',     '2024-01-01'),
    (40, 'ELEC-014', 'Ethernet Cable',       'Electronics',  9.99,   350, 'TechCorp',    '2024-01-01');


-- ============================================================================
-- TABLE: inventory_updates — Staging table for MERGE operations
-- ============================================================================
-- 30 rows: 15 matching existing SKUs (updates) + 15 new SKUs (inserts)
-- Of the 15 matches: 12 have qty > 0 (price/qty updates), 3 have qty = 0 (delete triggers)
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.inventory_updates (
    id              INT,
    sku             VARCHAR,
    name            VARCHAR,
    category        VARCHAR,
    price           DOUBLE,
    qty             INT,
    supplier        VARCHAR,
    last_updated    VARCHAR
) LOCATION 'delta-merge-advanced/inventory_updates';


INSERT INTO {{zone_name}}.delta_demos.inventory_updates VALUES
    -- Existing SKU updates (qty > 0 → UPDATE price & qty)
    (1,  'ELEC-001', 'Wireless Mouse',       'Electronics',  27.99,  180, 'TechCorp',    '2024-06-01'),
    (3,  'ELEC-003', 'Bluetooth Speaker',    'Electronics',  74.99,  90,  'AudioMax',    '2024-06-01'),
    (5,  'ELEC-005', 'Keyboard Mechanical',  'Electronics',  94.99,  50,  'KeyPro',      '2024-06-01'),
    (7,  'FURN-002', 'Standing Desk',        'Furniture',    379.99, 25,  'DeskWorks',   '2024-06-01'),
    (11, 'BOOK-001', 'SQL Fundamentals',     'Books',        42.99,  220, 'TechBooks',   '2024-06-01'),
    (14, 'BOOK-004', 'Python Cookbook',       'Books',        37.99,  200, 'TechBooks',   '2024-06-01'),
    (17, 'ELEC-007', 'Headphones ANC',       'Electronics',  179.99, 70,  'AudioMax',    '2024-06-01'),
    (21, 'SUPP-001', 'Sticky Notes',         'Supplies',     6.49,   550, 'OfficePro',   '2024-06-01'),
    (25, 'SUPP-005', 'Paper Clips',          'Supplies',     4.49,   650, 'OfficePro',   '2024-06-01'),
    (31, 'ELEC-011', 'HDMI Cable',           'Electronics',  11.99,  450, 'TechCorp',    '2024-06-01'),
    (33, 'SUPP-006', 'Desk Organizer',       'Supplies',     17.99,  200, 'OfficePro',   '2024-06-01'),
    (38, 'BOOK-008', 'Rust Programming',     'Books',        49.99,  100, 'TechBooks',   '2024-06-01'),
    -- Existing SKU updates (qty = 0 → DELETE out-of-stock)
    (9,  'FURN-004', 'Cable Tray',           'Furniture',    24.99,  0,   'DeskWorks',   '2024-06-01'),
    (28, 'FURN-008', 'Whiteboard Large',     'Furniture',    89.99,  0,   'OfficePro',   '2024-06-01'),
    (35, 'SUPP-008', 'Label Maker',          'Supplies',     29.99,  0,   'OfficePro',   '2024-06-01'),
    -- New SKUs (NOT MATCHED → INSERT)
    (41, 'ELEC-015', 'Wireless Charger',     'Electronics',  34.99,  100, 'PowerUp',     '2024-06-01'),
    (42, 'ELEC-016', 'USB Microphone',       'Electronics',  69.99,  45,  'AudioMax',    '2024-06-01'),
    (43, 'ELEC-017', 'Desk Fan USB',         'Electronics',  19.99,  80,  'TechCorp',    '2024-06-01'),
    (44, 'FURN-011', 'Ergonomic Wrist Rest', 'Furniture',    22.99,  120, 'ComfortPlus', '2024-06-01'),
    (45, 'FURN-012', 'Under-Desk Drawer',    'Furniture',    44.99,  40,  'DeskWorks',   '2024-06-01'),
    (46, 'FURN-013', 'Acoustic Panel Set',   'Furniture',    79.99,  30,  'ComfortPlus', '2024-06-01'),
    (47, 'BOOK-009', 'Go Programming',       'Books',        39.99,  90,  'TechBooks',   '2024-06-01'),
    (48, 'BOOK-010', 'GraphQL in Action',    'Books',        34.99,  70,  'CloudPress',  '2024-06-01'),
    (49, 'BOOK-011', 'Kubernetes Up',        'Books',        52.99,  60,  'CloudPress',  '2024-06-01'),
    (50, 'SUPP-009', 'Cable Sleeves',        'Supplies',     7.99,   300, 'OfficePro',   '2024-06-01'),
    (51, 'SUPP-010', 'Monitor Wipes',        'Supplies',     4.99,   400, 'OfficePro',   '2024-06-01'),
    (52, 'SUPP-011', 'Desk Mat',             'Supplies',     18.99,  150, 'OfficePro',   '2024-06-01'),
    (53, 'ELEC-018', 'Smart Plug',           'Electronics',  14.99,  200, 'PowerUp',     '2024-06-01'),
    (54, 'ELEC-019', 'LED Desk Strip',       'Electronics',  16.99,  170, 'LightCo',     '2024-06-01'),
    (55, 'ELEC-020', 'Portable SSD',         'Electronics',  79.99,  55,  'TechCorp',    '2024-06-01');

