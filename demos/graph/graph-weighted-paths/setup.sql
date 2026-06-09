-- ============================================================================
-- Graph Weighted Paths — Setup Script
-- ============================================================================
-- Creates a global shipping network with 25 major world ports connected by
-- 55 directed shipping routes across 5 regions: Asia, Europe, Americas,
-- Middle East, and South Asia.
--
-- Data model:
--   1. ports   — 25 vertex nodes (major container ports)
--   2. routes  — 55 directed edges with distance_nm weight
--   3. graph   — named graph definition (shipping_network)
--
-- Route topology:
--   Batch 1: Asia intra-regional trunk routes    (~16 edges)
--   Batch 2: Asia-Europe trunk routes            (~10 edges)
--   Batch 3: Asia-Americas trunk routes          (~10 edges)
--   Batch 4: Europe intra-regional feeder routes (~10 edges)
--   Batch 5: Transshipment hub connections        (~9 edges)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.shipping_network
    COMMENT 'Global shipping route optimization with weighted paths';

-- ============================================================================
-- TABLE 1: ports — 25 major world container ports
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.shipping_network.ports (
    id              BIGINT,
    name            STRING,
    region          STRING,
    capacity_teu    INT,
    crane_count     INT
) LOCATION 'graph-weighted-paths/ports';


INSERT INTO {{zone_name}}.shipping_network.ports VALUES
    (1,  'Shanghai',     'Asia',        43500, 6),
    (2,  'Singapore',    'Asia',        37200, 7),
    (3,  'Rotterdam',    'Europe',      14800, 8),
    (4,  'Busan',        'Asia',        21500, 9),
    (5,  'Hong_Kong',    'Asia',        19800, 10),
    (6,  'Guangzhou',    'Asia',        24300, 11),
    (7,  'Qingdao',      'Asia',        22000, 12),
    (8,  'Dubai',        'Middle_East', 15200, 13),
    (9,  'Antwerp',      'Europe',      12000, 14),
    (10, 'Ningbo',       'Asia',        28500, 15),
    (11, 'Hamburg',      'Europe',       8900, 16),
    (12, 'LA',           'Americas',     9500, 17),
    (13, 'Long_Beach',   'Americas',     8200, 18),
    (14, 'Tanjung',      'Asia',        11000, 19),
    (15, 'Dalian',       'Asia',        18000, 20),
    (16, 'Xiamen',       'Asia',        17000, 21),
    (17, 'Kaohsiung',    'Asia',        10700, 22),
    (18, 'Felixstowe',   'Europe',       4000, 23),
    (19, 'Valencia',     'Europe',       5300, 24),
    (20, 'Colombo',      'South_Asia',   7200, 5),
    (21, 'Mumbai',       'South_Asia',   5000, 6),
    (22, 'Piraeus',      'Europe',       5500, 7),
    (23, 'Santos',       'Americas',     4200, 8),
    (24, 'Manzanillo',   'Americas',     3200, 9),
    (25, 'Algeciras',    'Europe',       4800, 10);

-- ============================================================================
-- TABLE 2: routes — 55 directed shipping routes with distance weights
-- ============================================================================
-- Each route has distance_nm (weight), transit_days, route_type, and fuel_cost.
-- transit_days = ROUND(distance_nm / 400)
-- fuel_cost_usd = distance_nm * 0.15
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.shipping_network.routes (
    id              BIGINT,
    src             BIGINT,
    dst             BIGINT,
    distance_nm     DOUBLE,
    transit_days    INT,
    route_type      STRING,
    fuel_cost_usd   DOUBLE
) LOCATION 'graph-weighted-paths/routes';


-- ============================================================================
-- Batch 1: Asia intra-regional trunk routes (16 edges)
-- ============================================================================
INSERT INTO {{zone_name}}.shipping_network.routes VALUES
    (1,  1,  2,  2200.0, 6,  'trunk', 330.00),
    (2,  1,  4,   530.0, 1,  'trunk',  79.50),
    (3,  1,  5,   820.0, 2,  'trunk', 123.00),
    (4,  1,  10,  120.0, 0,  'trunk',  18.00),
    (5,  1,  7,   380.0, 1,  'trunk',  57.00),
    (6,  2,  5,  1600.0, 4,  'trunk', 240.00),
    (7,  2,  14,  550.0, 1,  'trunk',  82.50),
    (8,  4,  1,   530.0, 1,  'trunk',  79.50),
    (9,  4,  7,   600.0, 2,  'trunk',  90.00),
    (10, 5,  6,    80.0, 0,  'trunk',  12.00),
    (11, 5,  17,  370.0, 1,  'trunk',  55.50),
    (12, 6,  16,  350.0, 1,  'trunk',  52.50),
    (13, 10, 1,   120.0, 0,  'trunk',  18.00),
    (14, 15, 1,   590.0, 1,  'trunk',  88.50),
    (15, 15, 4,   600.0, 2,  'trunk',  90.00),
    (16, 1,  15,  590.0, 1,  'trunk',  88.50);

-- ============================================================================
-- Batch 2: Asia-Europe trunk routes (10 edges)
-- ============================================================================
INSERT INTO {{zone_name}}.shipping_network.routes VALUES
    (17, 1,  3,  10500.0, 26, 'trunk', 1575.00),
    (18, 1,  11, 10200.0, 26, 'trunk', 1530.00),
    (19, 2,  3,   8400.0, 21, 'trunk', 1260.00),
    (20, 2,  8,   3300.0,  8, 'trunk',  495.00),
    (21, 5,  3,   9800.0, 24, 'trunk', 1470.00),
    (22, 8,  3,   6400.0, 16, 'trunk',  960.00),
    (23, 8,  22,  4400.0, 11, 'trunk',  660.00),
    (24, 3,  11,   280.0,  1, 'trunk',   42.00),
    (25, 3,  9,     80.0,  0, 'trunk',   12.00),
    (26, 22, 3,   3100.0,  8, 'trunk',  465.00);

-- ============================================================================
-- Batch 3: Asia-Americas trunk routes (10 edges)
-- ============================================================================
INSERT INTO {{zone_name}}.shipping_network.routes VALUES
    (27, 1,  12,  6500.0, 16, 'trunk',  975.00),
    (28, 1,  13,  6500.0, 16, 'trunk',  975.00),
    (29, 4,  12,  5500.0, 14, 'trunk',  825.00),
    (30, 2,  12,  7900.0, 20, 'trunk', 1185.00),
    (31, 12, 13,    25.0,  0, 'trunk',    3.75),
    (32, 13, 12,    25.0,  0, 'trunk',    3.75),
    (33, 12, 24,  1500.0,  4, 'trunk',  225.00),
    (34, 3,  23,  5800.0, 14, 'trunk',  870.00),
    (35, 3,  12,  5500.0, 14, 'trunk',  825.00),
    (36, 23, 24,  4800.0, 12, 'trunk',  720.00);

-- ============================================================================
-- Batch 4: Europe intra-regional feeder routes (10 edges)
-- ============================================================================
INSERT INTO {{zone_name}}.shipping_network.routes VALUES
    (37, 3,  18,   180.0, 0, 'feeder',  27.00),
    (38, 3,  19,  1500.0, 4, 'feeder', 225.00),
    (39, 3,  25,  1700.0, 4, 'feeder', 255.00),
    (40, 11, 18,   450.0, 1, 'feeder',  67.50),
    (41, 11, 9,    310.0, 1, 'feeder',  46.50),
    (42, 9,  18,   200.0, 0, 'feeder',  30.00),
    (43, 19, 25,   350.0, 1, 'feeder',  52.50),
    (44, 25, 22,  1500.0, 4, 'feeder', 225.00),
    (45, 22, 19,  1300.0, 3, 'feeder', 195.00),
    (46, 18, 11,   450.0, 1, 'feeder',  67.50);

-- ============================================================================
-- Batch 5: Transshipment hub connections (9 edges)
-- ============================================================================
INSERT INTO {{zone_name}}.shipping_network.routes VALUES
    (47, 2,  20,  1650.0, 4, 'transshipment', 247.50),
    (48, 2,  21,  2400.0, 6, 'transshipment', 360.00),
    (49, 20, 8,   1500.0, 4, 'transshipment', 225.00),
    (50, 20, 21,   650.0, 2, 'transshipment',  97.50),
    (51, 21, 8,   1200.0, 3, 'transshipment', 180.00),
    (52, 14, 2,    550.0, 1, 'transshipment',  82.50),
    (53, 17, 5,    370.0, 1, 'transshipment',  55.50),
    (54, 16, 5,    400.0, 1, 'transshipment',  60.00),
    (55, 7,  4,    600.0, 2, 'transshipment',  90.00);

-- ============================================================================
-- PHYSICAL LAYOUT — Z-ORDER for fast data skipping
-- ============================================================================
-- The data was inserted in id-generation order, which has reasonable locality
-- for `id` but scatters the frequent filter column (region) across files.
-- Z-ORDER rewrites files so rows with similar values on the ordering keys
-- co-locate, giving Parquet min/max statistics much tighter ranges per file.
-- This benefits three hot paths:
--
--   1. CSR build from the routes table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `id` co-location lets the Parquet reader skip
--      almost every row group for targeted port lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE p.region = 'Asia'` skip entire files instead of reading the
--      whole ports table.
--
-- One-time cost at setup; every subsequent query benefits.  These OPTIMIZE
-- statements also compact small files written by the five-batch route load.
OPTIMIZE {{zone_name}}.shipping_network.ports
    ZORDER BY (id, region);

OPTIMIZE {{zone_name}}.shipping_network.routes
    ZORDER BY (src, dst);

-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
CREATE GRAPH IF NOT EXISTS {{zone_name}}.shipping_network.shipping_network
    VERTEX TABLE {{zone_name}}.shipping_network.ports ID COLUMN id NODE TYPE COLUMN region NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.shipping_network.routes SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN distance_nm
    EDGE TYPE COLUMN route_type
    DIRECTED;

-- ============================================================================
-- WARM CSR CACHE — Pre-build the Compressed Sparse Row topology
-- ============================================================================
-- CREATE GRAPHCSR writes the binary .dcsr file to disk, so the first Cypher
-- query loads in ~200 ms instead of rebuilding from Delta tables (6-14 s for
-- large graphs). Safe to re-run after bulk edge loads to refresh the cache.
CREATE GRAPHCSR {{zone_name}}.shipping_network.shipping_network;
