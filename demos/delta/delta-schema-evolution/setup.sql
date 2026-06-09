-- ============================================================================
-- Delta Schema Evolution — Setup Script
-- ============================================================================
-- Creates the contacts table with the initial 4-column schema and inserts
-- the first 30 rows of baseline data. Schema evolution (ALTER TABLE,
-- new inserts, backfill UPDATEs) happens in queries.sql.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- STEP 2: Create table with initial 4-column schema
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.contacts (
    id         INT,
    first_name VARCHAR,
    last_name  VARCHAR,
    email      VARCHAR
) LOCATION 'delta-schema-evolution/contacts';


-- STEP 3: Insert 30 baseline contacts
INSERT INTO {{zone_name}}.delta_demos.contacts VALUES
    (1,  'Alice',    'Johnson',   'alice.johnson@example.com'),
    (2,  'Bob',      'Smith',     'bob.smith@example.com'),
    (3,  'Carol',    'Williams',  'carol.williams@example.com'),
    (4,  'David',    'Brown',     'david.brown@example.com'),
    (5,  'Eve',      'Davis',     'eve.davis@example.com'),
    (6,  'Frank',    'Miller',    'frank.miller@example.com'),
    (7,  'Grace',    'Wilson',    'grace.wilson@example.com'),
    (8,  'Henry',    'Moore',     'henry.moore@example.com'),
    (9,  'Irene',    'Taylor',    'irene.taylor@example.com'),
    (10, 'Jack',     'Anderson',  'jack.anderson@example.com'),
    (11, 'Karen',    'Thomas',    'karen.thomas@example.com'),
    (12, 'Leo',      'Jackson',   'leo.jackson@example.com'),
    (13, 'Maria',    'White',     'maria.white@example.com'),
    (14, 'Nathan',   'Harris',    'nathan.harris@example.com'),
    (15, 'Olivia',   'Martin',    'olivia.martin@example.com'),
    (16, 'Paul',     'Garcia',    'paul.garcia@example.com'),
    (17, 'Quinn',    'Martinez',  'quinn.martinez@example.com'),
    (18, 'Rachel',   'Robinson',  'rachel.robinson@example.com'),
    (19, 'Sam',      'Clark',     'sam.clark@example.com'),
    (20, 'Tina',     'Lewis',     'tina.lewis@example.com'),
    (21, 'Uma',      'Lee',       'uma.lee@example.com'),
    (22, 'Victor',   'Walker',    'victor.walker@example.com'),
    (23, 'Wendy',    'Hall',      'wendy.hall@example.com'),
    (24, 'Xander',   'Allen',     'xander.allen@example.com'),
    (25, 'Yolanda',  'Young',     'yolanda.young@example.com'),
    (26, 'Zach',     'King',      'zach.king@example.com'),
    (27, 'Amber',    'Wright',    'amber.wright@example.com'),
    (28, 'Brian',    'Lopez',     'brian.lopez@example.com'),
    (29, 'Cindy',    'Hill',      'cindy.hill@example.com'),
    (30, 'Derek',    'Scott',     'derek.scott@example.com');

