-- ============================================================================
-- Delta VACUUM & CDC Interaction — Retention & Change Tracking — Setup Script
-- ============================================================================
-- Creates the order_lifecycle table with Change Data Feed (CDF) enabled
-- and inserts baseline data. All status transitions, deletions, and VACUUM
-- operations are in queries.sql so you can follow along step by step.
--
-- Tables created:
--   1. order_lifecycle — 40 initial rows (all status='pending'), CDF enabled
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: order_lifecycle — Order management with Change Data Feed
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.order_lifecycle (
    id              INT,
    order_id        VARCHAR,
    customer        VARCHAR,
    product         VARCHAR,
    amount          DOUBLE,
    status          VARCHAR,
    updated_by      VARCHAR,
    updated_at      VARCHAR
) LOCATION 'delta-vacuum-cdc-interaction/order_lifecycle'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');


-- V0: Insert 40 orders — all start as 'pending'
INSERT INTO {{zone_name}}.delta_demos.order_lifecycle VALUES
    (1,  'ORD-1001', 'Acme Corp',          'Widget Pro',         149.99, 'pending', 'system',    '2025-01-15 09:00:00'),
    (2,  'ORD-1002', 'Globex Inc',         'Gadget Max',         234.50, 'pending', 'system',    '2025-01-15 09:05:00'),
    (3,  'ORD-1003', 'Initech LLC',        'Sprocket Basic',      89.95, 'pending', 'system',    '2025-01-15 09:10:00'),
    (4,  'ORD-1004', 'Umbrella Corp',      'Gear Assembly Kit',  312.00, 'pending', 'system',    '2025-01-15 09:15:00'),
    (5,  'ORD-1005', 'Stark Industries',   'Bolt Set Premium',   175.25, 'pending', 'system',    '2025-01-15 09:20:00'),
    (6,  'ORD-1006', 'Wayne Enterprises',  'Cable Bundle 50m',   267.80, 'pending', 'system',    '2025-01-15 10:00:00'),
    (7,  'ORD-1007', 'Oscorp',             'Sensor Array',       423.15, 'pending', 'system',    '2025-01-15 10:05:00'),
    (8,  'ORD-1008', 'Cyberdyne Systems',  'Micro Controller',    56.90, 'pending', 'system',    '2025-01-15 10:10:00'),
    (9,  'ORD-1009', 'Soylent Corp',       'Filter Cartridge',   198.45, 'pending', 'system',    '2025-01-15 10:15:00'),
    (10, 'ORD-1010', 'Tyrell Corp',        'Power Module',       345.60, 'pending', 'system',    '2025-01-15 10:20:00'),
    (11, 'ORD-1011', 'Massive Dynamic',    'Relay Switch',       129.99, 'pending', 'system',    '2025-01-16 09:00:00'),
    (12, 'ORD-1012', 'Hooli',              'Display Panel',      510.00, 'pending', 'system',    '2025-01-16 09:05:00'),
    (13, 'ORD-1013', 'Pied Piper',         'USB Hub',             78.50, 'pending', 'system',    '2025-01-16 09:10:00'),
    (14, 'ORD-1014', 'Dunder Mifflin',     'Paper Tray XL',     245.75, 'pending', 'system',    '2025-01-16 09:15:00'),
    (15, 'ORD-1015', 'Wonka Industries',   'Mixing Valve',       189.30, 'pending', 'system',    '2025-01-16 09:20:00'),
    (16, 'ORD-1016', 'Weyland-Yutani',     'Thermal Pad',         92.40, 'pending', 'system',    '2025-01-17 09:00:00'),
    (17, 'ORD-1017', 'Aperture Science',   'Lens Assembly',      367.85, 'pending', 'system',    '2025-01-17 09:05:00'),
    (18, 'ORD-1018', 'Black Mesa',         'Coolant Hose',       154.20, 'pending', 'system',    '2025-01-17 09:10:00'),
    (19, 'ORD-1019', 'LexCorp',            'Transformer Unit',   278.90, 'pending', 'system',    '2025-01-17 09:15:00'),
    (20, 'ORD-1020', 'Virtucon',           'Fuse Box Mini',       43.50, 'pending', 'system',    '2025-01-17 09:20:00'),
    (21, 'ORD-1021', 'Omni Consumer',      'Spring Set',         199.95, 'pending', 'system',    '2025-01-18 09:00:00'),
    (22, 'ORD-1022', 'Rekall Inc',         'Washer Pack',         88.60, 'pending', 'system',    '2025-01-18 09:05:00'),
    (23, 'ORD-1023', 'Nakatomi Corp',      'Bearing Sleeve',     456.30, 'pending', 'system',    '2025-01-18 09:10:00'),
    (24, 'ORD-1024', 'Abstergo Industries','Clamp Ring',         321.75, 'pending', 'system',    '2025-01-18 09:15:00'),
    (25, 'ORD-1025', 'Momcorp',            'Seal Kit',            67.80, 'pending', 'system',    '2025-01-18 09:20:00'),
    (26, 'ORD-1026', 'Planet Express',     'Motor Drive',        534.20, 'pending', 'system',    '2025-01-19 09:00:00'),
    (27, 'ORD-1027', 'InGen',              'Bracket Mount',      112.45, 'pending', 'system',    '2025-01-19 09:05:00'),
    (28, 'ORD-1028', 'Dharma Initiative',  'Regulator Valve',    289.90, 'pending', 'system',    '2025-01-19 09:10:00'),
    (29, 'ORD-1029', 'Vault-Tec',          'Gasket Set',          73.25, 'pending', 'system',    '2025-01-19 09:15:00'),
    (30, 'ORD-1030', 'Cybertruck LLC',     'Axle Pin',           405.60, 'pending', 'system',    '2025-01-19 09:20:00'),
    (31, 'ORD-1031', 'Zorg Industries',    'Coupling Joint',     168.35, 'pending', 'system',    '2025-01-20 09:00:00'),
    (32, 'ORD-1032', 'Shinra Electric',    'Rotor Blade',        247.80, 'pending', 'system',    '2025-01-20 09:05:00'),
    (33, 'ORD-1033', 'Sarif Industries',   'Piston Ring',         99.95, 'pending', 'system',    '2025-01-20 09:10:00'),
    (34, 'ORD-1034', 'Tessier-Ashpool',    'Cam Shaft',          382.10, 'pending', 'system',    '2025-01-20 09:15:00'),
    (35, 'ORD-1035', 'Veidt Enterprises',  'Flywheel',           215.50, 'pending', 'system',    '2025-01-20 09:20:00'),
    (36, 'ORD-1036', 'Bluth Company',      'Hinge Pin',          143.70, 'pending', 'system',    '2025-01-21 09:00:00'),
    (37, 'ORD-1037', 'Sterling Cooper',    'Rivet Pack',         298.55, 'pending', 'system',    '2025-01-21 09:05:00'),
    (38, 'ORD-1038', 'CHOAM',              'O-Ring Set',         176.40, 'pending', 'system',    '2025-01-21 09:10:00'),
    (39, 'ORD-1039', 'Sirius Cybernetics', 'Toggle Switch',       62.85, 'pending', 'system',    '2025-01-21 09:15:00'),
    (40, 'ORD-1040', 'Prestige Worldwide', 'Lock Washer',        331.25, 'pending', 'system',    '2025-01-21 09:20:00');

