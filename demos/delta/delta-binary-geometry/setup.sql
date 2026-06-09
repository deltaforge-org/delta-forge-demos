-- ============================================================================
-- Delta Binary & Spatial Data Types — Setup Script
-- ============================================================================
-- Demonstrates binary-like data and spatial geometry patterns:
--   - SHA-256 content hashes as fingerprints for documents
--   - WKT (Well-Known Text) geometry strings for spatial data
--   - POINT, POLYGON, and LINESTRING geometry types
--
-- Tables created:
--   1. document_store — 25 documents (23 after DELETE)
--   2. geo_locations  — 30 locations with WKT geometry
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE + INSERT document_store (25 rows)
--   3. CREATE + INSERT geo_locations (20 POINTs + 10 POLYGON/LINESTRING)
--   4. UPDATE — reclassify 3 location regions
--   5. DELETE — remove 2 deprecated documents
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: document_store — file metadata with content hashes
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.document_store (
    id              INT,
    name            VARCHAR,
    mime_type       VARCHAR,
    content_hash    VARCHAR,
    size_bytes      INT,
    created_at      VARCHAR
) LOCATION 'delta-binary-geometry/document_store';


-- STEP 2: Insert 25 documents
INSERT INTO {{zone_name}}.delta_demos.document_store VALUES
    (1,  'annual_report_2024.pdf',       'application/pdf',                    'a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890', 2458901, '2024-01-15'),
    (2,  'logo_dark.png',                'image/png',                          'b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1', 345678,  '2024-01-20'),
    (3,  'team_photo.jpeg',              'image/jpeg',                         'c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2', 1892345, '2024-02-01'),
    (4,  'budget_q1.xlsx',               'application/vnd.ms-excel',           'd4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3', 567890,  '2024-02-10'),
    (5,  'policy_handbook.pdf',          'application/pdf',                    'e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4', 3210456, '2024-02-15'),
    (6,  'product_catalog.json',         'application/json',                   'f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5', 89012,   '2024-02-20'),
    (7,  'banner_hero.png',              'image/png',                          '7890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f6', 2345678, '2024-03-01'),
    (8,  'sales_data_q1.csv',            'text/csv',                           '890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67', 123456,  '2024-03-05'),
    (9,  'security_audit.pdf',           'application/pdf',                    '90a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f678', 1567890, '2024-03-10'),
    (10, 'office_floorplan.png',         'image/png',                          '0a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f6789', 4567890, '2024-03-15'),
    (11, 'employee_handbook.pdf',        'application/pdf',                    '1a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f7890', 2890123, '2024-03-20'),
    (12, 'quarterly_review.pdf',         'application/pdf',                    '2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a', 1234567, '2024-04-01'),
    (13, 'marketing_assets.zip',         'application/zip',                    '3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b', 8901234, '2024-04-05'),
    (14, 'headshot_ceo.jpeg',            'image/jpeg',                         '4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c', 678901,  '2024-04-10'),
    (15, 'budget_q2.xlsx',               'application/vnd.ms-excel',           '5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d', 654321,  '2024-04-15'),
    (16, 'api_documentation.json',       'application/json',                   '6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e', 234567,  '2024-04-20'),
    (17, 'compliance_report.pdf',        'application/pdf',                    '78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f', 1890123, '2024-05-01'),
    (18, 'inventory_data.csv',           'text/csv',                           '8901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f7', 456789,  '2024-05-05'),
    (19, 'product_photo_set.jpeg',       'image/jpeg',                         '901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78', 3456789, '2024-05-10'),
    (20, 'training_manual.pdf',          'application/pdf',                    'a12b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f7890', 2345678, '2024-05-15'),
    (21, 'icon_set.png',                 'image/png',                          'b23c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a', 567890,  '2024-05-20'),
    (22, 'sales_data_q2.csv',            'text/csv',                           'c34d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b', 198765,  '2024-06-01'),
    (23, 'architecture_diagram.png',     'image/png',                          'd45e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c', 1234567, '2024-06-05'),
    (24, 'deprecated_schema_v1.json',    'application/json',                   'e56f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d', 45678,   '2023-06-15'),
    (25, 'old_migration_notes.csv',      'text/csv',                           'f6789a1b2c3d4e5f6789a1b2c3d4e5f6789a1b2c3d4e5f6789a1b2c3d4e5f678', 12345,   '2023-01-10');


-- ============================================================================
-- TABLE 2: geo_locations — spatial data with WKT geometry
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.geo_locations (
    id          INT,
    name        VARCHAR,
    loc_type    VARCHAR,
    wkt         VARCHAR,
    latitude    DOUBLE,
    longitude   DOUBLE,
    region      VARCHAR
) LOCATION 'delta-binary-geometry/geo_locations';


-- STEP 3: Insert 20 POINT locations (batch 1)
INSERT INTO {{zone_name}}.delta_demos.geo_locations VALUES
    (1,  'Empire State Building',    'POINT', 'POINT(-73.9857 40.7484)',       40.7484,  -73.9857,  'North America'),
    (2,  'Statue of Liberty',        'POINT', 'POINT(-74.0445 40.6892)',       40.6892,  -74.0445,  'North America'),
    (3,  'Golden Gate Bridge',        'POINT', 'POINT(-122.4783 37.8199)',      37.8199,  -122.4783, 'North America'),
    (4,  'CN Tower',                  'POINT', 'POINT(-79.3871 43.6426)',       43.6426,  -79.3871,  'North America'),
    (5,  'Eiffel Tower',              'POINT', 'POINT(2.2945 48.8584)',         48.8584,  2.2945,    'Europe'),
    (6,  'Big Ben',                   'POINT', 'POINT(-0.1246 51.5007)',        51.5007,  -0.1246,   'Europe'),
    (7,  'Colosseum',                 'POINT', 'POINT(12.4924 41.8902)',        41.8902,  12.4924,   'Europe'),
    (8,  'Sagrada Familia',           'POINT', 'POINT(2.1744 41.4036)',         41.4036,  2.1744,    'Europe'),
    (9,  'Tokyo Tower',               'POINT', 'POINT(139.7454 35.6586)',       35.6586,  139.7454,  'Asia'),
    (10, 'Great Wall Badaling',       'POINT', 'POINT(116.0046 40.3540)',       40.3540,  116.0046,  'Asia'),
    (11, 'Taj Mahal',                 'POINT', 'POINT(78.0421 27.1751)',        27.1751,  78.0421,   'Asia'),
    (12, 'Angkor Wat',                'POINT', 'POINT(103.8670 13.4125)',       13.4125,  103.8670,  'Asia'),
    (13, 'Sydney Opera House',        'POINT', 'POINT(151.2153 -33.8568)',     -33.8568,  151.2153,  'Oceania'),
    (14, 'Uluru',                     'POINT', 'POINT(131.0369 -25.3444)',     -25.3444,  131.0369,  'Oceania'),
    (15, 'Christ the Redeemer',       'POINT', 'POINT(-43.2105 -22.9519)',     -22.9519,  -43.2105,  'South America'),
    (16, 'Machu Picchu',              'POINT', 'POINT(-72.5450 -13.1631)',     -13.1631,  -72.5450,  'South America'),
    (17, 'Table Mountain',            'POINT', 'POINT(18.4041 -33.9628)',      -33.9628,  18.4041,   'Africa'),
    (18, 'Pyramids of Giza',          'POINT', 'POINT(31.1342 29.9792)',        29.9792,  31.1342,   'Africa'),
    (19, 'Mount Kilimanjaro',         'POINT', 'POINT(37.3556 -3.0674)',        -3.0674,  37.3556,   'Africa'),
    (20, 'Victoria Falls',            'POINT', 'POINT(25.8572 -17.9243)',      -17.9243,  25.8572,   'Africa');


-- STEP 4: Insert 10 POLYGON and LINESTRING locations (batch 2)
INSERT INTO {{zone_name}}.delta_demos.geo_locations VALUES
    (21, 'Central Park',              'POLYGON',    'POLYGON((-73.9819 40.7681, -73.9580 40.8006, -73.9494 40.7968, -73.9733 40.7644, -73.9819 40.7681))',   40.7829, -73.9654, 'North America'),
    (22, 'Hyde Park',                 'POLYGON',    'POLYGON((-0.1870 51.5111, -0.1527 51.5133, -0.1527 51.5028, -0.1870 51.5028, -0.1870 51.5111))',        51.5073, -0.1657,  'Europe'),
    (23, 'Yoyogi Park',              'POLYGON',    'POLYGON((139.6926 35.6720, 139.7012 35.6720, 139.7012 35.6680, 139.6926 35.6680, 139.6926 35.6720))',    35.6700, 139.6969, 'Asia'),
    (24, 'Amazon Basin',             'POLYGON',    'POLYGON((-74.0000 -2.0000, -54.0000 -2.0000, -54.0000 -10.0000, -74.0000 -10.0000, -74.0000 -2.0000))', -6.0000, -64.0000, 'South America'),
    (25, 'Serengeti National Park',  'POLYGON',    'POLYGON((34.0000 -1.5000, 35.5000 -1.5000, 35.5000 -3.5000, 34.0000 -3.5000, 34.0000 -1.5000))',       -2.5000, 34.7500,  'Africa'),
    (26, 'Route 66 Segment',         'LINESTRING', 'LINESTRING(-87.6298 41.8781, -89.6501 39.7817, -90.1994 38.6270)',                                       40.1123, -89.1598, 'North America'),
    (27, 'Rhine River Path',         'LINESTRING', 'LINESTRING(6.7735 51.2277, 6.9603 50.9375, 7.0986 50.7374)',                                             50.9675, 6.9441,   'Europe'),
    (28, 'Silk Road Segment',        'LINESTRING', 'LINESTRING(69.2401 41.2995, 71.4197 42.8746, 75.0000 42.0000)',                                          42.0580, 71.8866,  'Asia'),
    (29, 'Pan-American Segment',     'LINESTRING', 'LINESTRING(-77.0428 -12.0464, -71.5375 -16.3989, -68.1193 -16.4897)',                                   -14.8450, -72.2332, 'South America'),
    (30, 'Great Barrier Reef Line',  'LINESTRING', 'LINESTRING(145.7781 -16.9186, 147.7000 -18.2861, 149.2000 -20.0000)',                                   -18.4016, 147.5594, 'Oceania');

-- ============================================================================
-- STEP 5: UPDATE — reclassify 3 location regions
-- ============================================================================
-- Victoria Falls (id=20) spans Zambia/Zimbabwe — reclassify from Africa to South America
--   (simulating a reporting region reassignment for cross-continental analysis)
-- Angkor Wat (id=12) — reclassify from Asia to Oceania
--   (simulating a tourism region consolidation)
-- Sagrada Familia (id=8) — reclassify from Europe to North America
--   (simulating a reporting region reassignment)
-- Net effect: region distribution changes but distinct count remains 6
UPDATE {{zone_name}}.delta_demos.geo_locations SET region = 'South America' WHERE id = 20;
UPDATE {{zone_name}}.delta_demos.geo_locations SET region = 'Oceania' WHERE id = 12;
UPDATE {{zone_name}}.delta_demos.geo_locations SET region = 'North America' WHERE id = 8;


-- ============================================================================
-- STEP 6: DELETE — remove 2 deprecated document entries
-- ============================================================================
-- Remove old/deprecated documents: ids 24 (deprecated_schema_v1.json) and 25 (old_migration_notes.csv)
-- Final document_store count: 25 - 2 = 23
DELETE FROM {{zone_name}}.delta_demos.document_store
WHERE id IN (24, 25);
