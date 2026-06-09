-- ============================================================================
-- Delta Time Travel & OPTIMIZE — Setup Script
-- ============================================================================
-- Builds a version history through 4 operations on an inventory table.
-- Each operation creates a new Delta version that can be browsed in the GUI.
--
-- Version History:
--   V0: CREATE + INSERT 25 items               → 25 rows
--   V1: UPDATE  — restock 5 items (qty += 100) → 25 rows (5 changed)
--   V2: DELETE  — remove 3 discontinued items   → 22 rows
--   V3: INSERT  — add 10 new items              → 32 rows
--
-- After setup, the table has accumulated many small files from these
-- DML operations. The queries.sql script will demonstrate OPTIMIZE
-- to compact them.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- VERSION 0: CREATE + INSERT 25 items
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.inventory (
    id        INT,
    item      VARCHAR,
    category  VARCHAR,
    qty       INT,
    price     DOUBLE,
    warehouse VARCHAR
) LOCATION 'delta-time-travel-optimize/inventory';


INSERT INTO {{zone_name}}.delta_demos.inventory VALUES
    (1,  'Laptop Pro 15',     'Electronics', 50,  1299.99, 'WH-East'),
    (2,  'Wireless Mouse',    'Electronics', 200, 29.99,   'WH-East'),
    (3,  'USB-C Dock',        'Electronics', 80,  89.99,   'WH-West'),
    (4,  'Monitor 27"',       'Electronics', 45,  449.99,  'WH-East'),
    (5,  'Keyboard MX',       'Electronics', 120, 99.99,   'WH-West'),
    (6,  'Office Chair',      'Furniture',   30,  349.99,  'WH-Central'),
    (7,  'Standing Desk',     'Furniture',   25,  599.99,  'WH-Central'),
    (8,  'Bookshelf Oak',     'Furniture',   40,  179.99,  'WH-East'),
    (9,  'Desk Lamp LED',     'Furniture',   90,  45.99,   'WH-West'),
    (10, 'Filing Cabinet',    'Furniture',   55,  129.99,  'WH-Central'),
    (11, 'A4 Notebook',       'Stationery',  500, 5.99,    'WH-West'),
    (12, 'Gel Pen Set',       'Stationery',  800, 8.99,    'WH-West'),
    (13, 'Sticky Notes',      'Stationery',  600, 3.49,    'WH-East'),
    (14, 'Highlighters',      'Stationery',  300, 7.99,    'WH-East'),
    (15, 'Binder Clips',      'Stationery',  400, 2.49,    'WH-West'),
    (16, 'Headphones Pro',    'Audio',       70,  199.99,  'WH-East'),
    (17, 'BT Speaker',        'Audio',       100, 79.99,   'WH-West'),
    (18, 'Studio Mic',        'Audio',       35,  249.99,  'WH-Central'),
    (19, 'Wireless Earbuds',  'Audio',       150, 59.99,   'WH-East'),
    (20, 'Sound Bar',         'Audio',       20,  299.99,  'WH-Central'),
    (21, 'Webcam HD',         'Electronics', 60,  69.99,   'WH-West'),
    (22, 'Laptop Stand',      'Furniture',   75,  39.99,   'WH-East'),
    (23, 'Whiteboard 4x3',    'Stationery',  15,  89.99,   'WH-Central'),
    (24, 'Cable Organizer',   'Electronics', 200, 12.99,   'WH-West'),
    (25, 'Desk Mat XL',       'Furniture',   110, 24.99,   'WH-East');


-- ============================================================================
-- VERSION 1: UPDATE — restock 5 low-stock items (qty += 100)
-- ============================================================================
-- Items restocked: id=7 (25→125), id=18 (35→135), id=20 (20→120),
--                  id=23 (15→115), id=4 (45→145)
UPDATE {{zone_name}}.delta_demos.inventory
SET qty = qty + 100
WHERE id IN (4, 7, 18, 20, 23);


-- ============================================================================
-- VERSION 2: DELETE — remove 3 discontinued items
-- ============================================================================
-- Removed: id=15 (Binder Clips), id=24 (Cable Organizer), id=9 (Desk Lamp LED)
DELETE FROM {{zone_name}}.delta_demos.inventory
WHERE id IN (9, 15, 24);


-- ============================================================================
-- VERSION 3: INSERT — add 10 new items
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.inventory VALUES
    (26, 'Tablet Stand',      'Furniture',   85,  34.99,   'WH-East'),
    (27, 'Power Strip',       'Electronics', 150, 19.99,   'WH-West'),
    (28, 'Desk Fan',          'Furniture',   60,  29.99,   'WH-Central'),
    (29, 'Label Maker',       'Stationery',  45,  49.99,   'WH-West'),
    (30, 'Noise Canceller',   'Audio',       40,  179.99,  'WH-East'),
    (31, 'Ergonomic Mouse',   'Electronics', 90,  59.99,   'WH-West'),
    (32, 'Footrest',          'Furniture',   55,  44.99,   'WH-Central'),
    (33, 'Stapler Heavy',     'Stationery',  200, 15.99,   'WH-East'),
    (34, 'DAC Amplifier',     'Audio',       30,  149.99,  'WH-Central'),
    (35, 'Privacy Screen',    'Electronics', 70,  39.99,   'WH-East');
