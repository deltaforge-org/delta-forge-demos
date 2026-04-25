-- ============================================================================
-- Delta Binary & Geometry Advanced — Setup Script
-- ============================================================================
-- Builds on the foundational binary/geometry demo with richer data and
-- relationships designed for advanced analytical queries:
--
-- Tables created:
--   1. documents       — 30 documents with intentional hash duplicates for
--                        deduplication analysis, geo-tagged with location_id
--   2. locations       — 25 locations with WKT geometry (POINT, POLYGON,
--                        LINESTRING) spanning 6 regions
--   3. audit_log       — 40 access events linking users to documents at
--                        locations for cross-table analytics
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE + INSERT documents (30 rows, 5 duplicate hash pairs)
--   3. CREATE + INSERT locations (25 rows: 15 POINT + 5 POLYGON + 5 LINESTRING)
--   4. CREATE + INSERT audit_log (40 access events)
--   5. UPDATE — reclassify 2 location regions
--   6. DELETE — remove 3 revoked documents
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: documents — file metadata with content hashes and geo-tags
-- ============================================================================
-- 30 documents. 5 pairs share the same content_hash (duplicate content).
-- Each document is optionally tagged with a location_id for spatial joins.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.documents (
    id              INT,
    name            VARCHAR,
    mime_type       VARCHAR,
    content_hash    VARCHAR,
    size_bytes      INT,
    location_id     INT,
    created_at      VARCHAR
) LOCATION 'documents';


-- Insert 30 documents (ids 1-30)
-- Duplicate hash pairs: (1,16), (2,17), (5,18), (8,19), (10,20)
INSERT INTO {{zone_name}}.delta_demos.documents VALUES
    (1,  'annual_report_2024.pdf',       'application/pdf',  'a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890', 2458901, 1,    '2024-01-15'),
    (2,  'logo_dark.png',                'image/png',        'b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1', 345678,  2,    '2024-01-20'),
    (3,  'team_photo.jpeg',              'image/jpeg',       'c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2', 1892345, 3,    '2024-02-01'),
    (4,  'budget_q1.xlsx',               'application/vnd.ms-excel', 'd4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3', 567890, 1, '2024-02-10'),
    (5,  'policy_handbook.pdf',          'application/pdf',  'e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4', 3210456, NULL, '2024-02-15'),
    (6,  'product_catalog.json',         'application/json', 'f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5', 89012,   4,    '2024-02-20'),
    (7,  'banner_hero.png',              'image/png',        '7890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f6', 2345678, 5,    '2024-03-01'),
    (8,  'sales_data_q1.csv',            'text/csv',         '890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67', 123456,  6,    '2024-03-05'),
    (9,  'security_audit.pdf',           'application/pdf',  '90a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f678', 1567890, 7,    '2024-03-10'),
    (10, 'office_floorplan.png',         'image/png',        '0a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f6789', 4567890, 8,    '2024-03-15'),
    (11, 'employee_handbook.pdf',        'application/pdf',  '1a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f7890', 2890123, 9,    '2024-03-20'),
    (12, 'quarterly_review.pdf',         'application/pdf',  '2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a', 1234567, 10,   '2024-04-01'),
    (13, 'marketing_assets.zip',         'application/zip',  '3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b', 8901234, NULL, '2024-04-05'),
    (14, 'headshot_ceo.jpeg',            'image/jpeg',       '4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c', 678901,  11,   '2024-04-10'),
    (15, 'budget_q2.xlsx',               'application/vnd.ms-excel', '5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d', 654321, 12, '2024-04-15'),
    (16, 'annual_report_2024_copy.pdf',  'application/pdf',  'a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890', 2458901, 1,    '2024-04-20'),
    (17, 'logo_dark_backup.png',         'image/png',        'b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1', 345678,  2,    '2024-05-01'),
    (18, 'policy_handbook_v2.pdf',       'application/pdf',  'e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4', 3210456, 3,    '2024-05-05'),
    (19, 'sales_data_q1_archive.csv',    'text/csv',         '890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67', 123456,  6,    '2024-05-10'),
    (20, 'office_floorplan_v2.png',      'image/png',        '0a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f6789', 4567890, 8,    '2024-05-15'),
    (21, 'compliance_report.pdf',        'application/pdf',  '78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f', 1890123, 13,   '2024-05-20'),
    (22, 'inventory_data.csv',           'text/csv',         '8901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f7', 456789,  14,   '2024-06-01'),
    (23, 'product_photos.zip',           'application/zip',  'aa01a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f7', 5678901, 15,   '2024-06-05'),
    (24, 'training_manual.pdf',          'application/pdf',  'bb12b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f789', 2345678, NULL, '2024-06-10'),
    (25, 'api_docs.json',               'application/json', 'cc23c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901', 234567,  16,   '2024-06-15'),
    (26, 'architecture_diagram.png',     'image/png',        'dd34d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f7890123', 1234567, 17,   '2024-06-20'),
    (27, 'deprecated_schema_v1.json',    'application/json', 'ee45e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3c4d5e6f78901a2b3', 45678,   NULL, '2023-06-15'),
    (28, 'old_migration_notes.csv',      'text/csv',         'ff56f6789a1b2c3d4e5f6789a1b2c3d4e5f6789a1b2c3d4e5f6789a1b2c3d4e5', 12345,   NULL, '2023-01-10'),
    (29, 'legacy_config.json',           'application/json', 'ab67a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f6', 8901,    NULL, '2022-11-01'),
    (30, 'temp_export.csv',              'text/csv',         'bc78b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f678', 5432,    NULL, '2022-08-20');


-- ============================================================================
-- TABLE 2: locations — spatial data with WKT geometry
-- ============================================================================
-- 25 locations: 15 POINT, 5 POLYGON, 5 LINESTRING
-- Latitude/longitude stored as numeric columns alongside WKT for filtering
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.locations (
    id          INT,
    name        VARCHAR,
    loc_type    VARCHAR,
    wkt         VARCHAR,
    latitude    DOUBLE,
    longitude   DOUBLE,
    region      VARCHAR,
    elevation_m INT
) LOCATION 'locations';


-- 15 POINT locations
INSERT INTO {{zone_name}}.delta_demos.locations VALUES
    (1,  'Empire State Building',    'POINT', 'POINT(-73.9857 40.7484)',   40.7484,  -73.9857,  'North America', 443),
    (2,  'Statue of Liberty',        'POINT', 'POINT(-74.0445 40.6892)',   40.6892,  -74.0445,  'North America', 93),
    (3,  'Golden Gate Bridge',        'POINT', 'POINT(-122.4783 37.8199)',  37.8199,  -122.4783, 'North America', 227),
    (4,  'CN Tower',                  'POINT', 'POINT(-79.3871 43.6426)',   43.6426,  -79.3871,  'North America', 553),
    (5,  'Eiffel Tower',              'POINT', 'POINT(2.2945 48.8584)',     48.8584,  2.2945,    'Europe',        330),
    (6,  'Big Ben',                   'POINT', 'POINT(-0.1246 51.5007)',    51.5007,  -0.1246,   'Europe',        96),
    (7,  'Colosseum',                 'POINT', 'POINT(12.4924 41.8902)',    41.8902,  12.4924,   'Europe',        48),
    (8,  'Tokyo Tower',               'POINT', 'POINT(139.7454 35.6586)',   35.6586,  139.7454,  'Asia',          333),
    (9,  'Taj Mahal',                 'POINT', 'POINT(78.0421 27.1751)',    27.1751,  78.0421,   'Asia',          171),
    (10, 'Great Wall Badaling',       'POINT', 'POINT(116.0046 40.3540)',   40.3540,  116.0046,  'Asia',          888),
    (11, 'Sydney Opera House',        'POINT', 'POINT(151.2153 -33.8568)', -33.8568,  151.2153,  'Oceania',       65),
    (12, 'Christ the Redeemer',       'POINT', 'POINT(-43.2105 -22.9519)', -22.9519,  -43.2105,  'South America', 710),
    (13, 'Table Mountain',            'POINT', 'POINT(18.4041 -33.9628)',  -33.9628,  18.4041,   'Africa',        1085),
    (14, 'Pyramids of Giza',          'POINT', 'POINT(31.1342 29.9792)',    29.9792,  31.1342,   'Africa',        138),
    (15, 'Mount Kilimanjaro',         'POINT', 'POINT(37.3556 -3.0674)',    -3.0674,  37.3556,   'Africa',        5895);

-- 5 POLYGON locations
INSERT INTO {{zone_name}}.delta_demos.locations VALUES
    (16, 'Central Park',              'POLYGON',    'POLYGON((-73.9819 40.7681, -73.9580 40.8006, -73.9494 40.7968, -73.9733 40.7644, -73.9819 40.7681))',   40.7829, -73.9654, 'North America', 42),
    (17, 'Hyde Park',                 'POLYGON',    'POLYGON((-0.1870 51.5111, -0.1527 51.5133, -0.1527 51.5028, -0.1870 51.5028, -0.1870 51.5111))',        51.5073, -0.1657,  'Europe',        30),
    (18, 'Yoyogi Park',              'POLYGON',    'POLYGON((139.6926 35.6720, 139.7012 35.6720, 139.7012 35.6680, 139.6926 35.6680, 139.6926 35.6720))',    35.6700, 139.6969, 'Asia',          38),
    (19, 'Amazon Basin',             'POLYGON',    'POLYGON((-74.0000 -2.0000, -54.0000 -2.0000, -54.0000 -10.0000, -74.0000 -10.0000, -74.0000 -2.0000))', -6.0000, -64.0000, 'South America', 80),
    (20, 'Serengeti National Park',  'POLYGON',    'POLYGON((34.0000 -1.5000, 35.5000 -1.5000, 35.5000 -3.5000, 34.0000 -3.5000, 34.0000 -1.5000))',       -2.5000, 34.7500,  'Africa',        1500);

-- 5 LINESTRING locations
INSERT INTO {{zone_name}}.delta_demos.locations VALUES
    (21, 'Route 66 Segment',         'LINESTRING', 'LINESTRING(-87.6298 41.8781, -89.6501 39.7817, -90.1994 38.6270)',                  40.1123, -89.1598, 'North America',  200),
    (22, 'Rhine River Path',         'LINESTRING', 'LINESTRING(6.7735 51.2277, 6.9603 50.9375, 7.0986 50.7374)',                        50.9675, 6.9441,   'Europe',          50),
    (23, 'Silk Road Segment',        'LINESTRING', 'LINESTRING(69.2401 41.2995, 71.4197 42.8746, 75.0000 42.0000)',                     42.0580, 71.8866,  'Asia',           800),
    (24, 'Pan-American Segment',     'LINESTRING', 'LINESTRING(-77.0428 -12.0464, -71.5375 -16.3989, -68.1193 -16.4897)',              -14.8450, -72.2332, 'South America',  3500),
    (25, 'Great Barrier Reef Line',  'LINESTRING', 'LINESTRING(145.7781 -16.9186, 147.7000 -18.2861, 149.2000 -20.0000)',              -18.4016, 147.5594, 'Oceania',          0);


-- ============================================================================
-- TABLE 3: audit_log — document access events for cross-table analytics
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.audit_log (
    id          INT,
    doc_id      INT,
    location_id INT,
    user_name   VARCHAR,
    action      VARCHAR,
    accessed_at VARCHAR
) LOCATION 'audit_log';


-- 40 access events across documents and locations
INSERT INTO {{zone_name}}.delta_demos.audit_log VALUES
    (1,  1,  1,  'alice',   'view',     '2024-01-16 09:00:00'),
    (2,  1,  1,  'bob',     'download', '2024-01-16 10:30:00'),
    (3,  2,  2,  'alice',   'view',     '2024-01-21 08:15:00'),
    (4,  3,  3,  'carol',   'view',     '2024-02-02 14:00:00'),
    (5,  4,  1,  'bob',     'edit',     '2024-02-11 11:00:00'),
    (6,  5,  NULL, 'alice',   'view',     '2024-02-16 09:30:00'),
    (7,  6,  4,  'dave',    'download', '2024-02-21 16:00:00'),
    (8,  7,  5,  'carol',   'view',     '2024-03-02 10:00:00'),
    (9,  8,  6,  'alice',   'edit',     '2024-03-06 13:45:00'),
    (10, 9,  7,  'bob',     'view',     '2024-03-11 08:00:00'),
    (11, 10, 8,  'dave',    'download', '2024-03-16 15:30:00'),
    (12, 11, 9,  'carol',   'view',     '2024-03-21 09:00:00'),
    (13, 12, 10, 'alice',   'view',     '2024-04-02 10:15:00'),
    (14, 13, NULL, 'bob',     'download', '2024-04-06 14:00:00'),
    (15, 14, 11, 'dave',    'view',     '2024-04-11 11:30:00'),
    (16, 15, 12, 'carol',   'edit',     '2024-04-16 08:45:00'),
    (17, 1,  1,  'carol',   'view',     '2024-04-20 09:00:00'),
    (18, 2,  2,  'dave',    'view',     '2024-05-02 10:00:00'),
    (19, 5,  NULL, 'bob',     'download', '2024-05-06 14:30:00'),
    (20, 8,  6,  'alice',   'view',     '2024-05-11 08:00:00'),
    (21, 10, 8,  'carol',   'edit',     '2024-05-16 11:00:00'),
    (22, 1,  1,  'alice',   'download', '2024-05-20 16:00:00'),
    (23, 16, 1,  'bob',     'view',     '2024-05-21 09:15:00'),
    (24, 17, 2,  'alice',   'view',     '2024-05-22 10:00:00'),
    (25, 18, 3,  'carol',   'download', '2024-05-23 14:00:00'),
    (26, 19, 6,  'dave',    'view',     '2024-05-24 08:30:00'),
    (27, 20, 8,  'bob',     'edit',     '2024-05-25 11:45:00'),
    (28, 21, 13, 'alice',   'view',     '2024-05-26 09:00:00'),
    (29, 22, 14, 'carol',   'view',     '2024-06-02 10:30:00'),
    (30, 23, 15, 'dave',    'download', '2024-06-06 15:00:00'),
    (31, 24, NULL, 'bob',     'view',     '2024-06-11 08:00:00'),
    (32, 25, 16, 'alice',   'edit',     '2024-06-16 13:00:00'),
    (33, 26, 17, 'carol',   'view',     '2024-06-21 09:30:00'),
    (34, 1,  1,  'dave',    'view',     '2024-07-01 10:00:00'),
    (35, 5,  NULL, 'carol',   'view',     '2024-07-05 14:00:00'),
    (36, 10, 8,  'bob',     'download', '2024-07-10 08:15:00'),
    (37, 12, 10, 'dave',    'edit',     '2024-07-15 11:30:00'),
    (38, 3,  3,  'alice',   'view',     '2024-07-20 09:00:00'),
    (39, 7,  5,  'bob',     'view',     '2024-07-25 16:00:00'),
    (40, 9,  7,  'carol',   'download', '2024-08-01 10:45:00');


-- ============================================================================
-- STEP 5: UPDATE — reclassify 2 location regions
-- ============================================================================
-- Great Wall Badaling (id=10) — reclassify from Asia to North America
--   (simulating a reporting region reassignment)
-- Pyramids of Giza (id=14) — reclassify from Africa to Europe
--   (simulating a tourism region consolidation)
UPDATE {{zone_name}}.delta_demos.locations SET region = 'North America' WHERE id = 10;
UPDATE {{zone_name}}.delta_demos.locations SET region = 'Europe' WHERE id = 14;


-- ============================================================================
-- STEP 6: DELETE — remove 3 revoked documents
-- ============================================================================
-- Remove legacy/deprecated documents: ids 28, 29, 30
-- Final document count: 30 - 3 = 27
DELETE FROM {{zone_name}}.delta_demos.documents WHERE id IN (28, 29, 30);
