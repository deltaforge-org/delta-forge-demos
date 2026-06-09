-- ============================================================================
-- Graph Advanced Cypher — Setup Script
-- ============================================================================
-- Creates a university research collaboration network for demonstrating
-- advanced Cypher patterns: negative patterns, aggregation functions,
-- multi-hop traversals, edge type filtering, and mixed-type queries.
--
--   1. researchers     — 40 vertex nodes (5 departments, 4 ranks)
--   2. collaborations  — 170 directed edges (co-author, advisor, committee, reviewer)
--
-- Dataset: 40 researchers across CompSci, Physics, Biology, Math, Chemistry
-- with deterministic properties derived from modular arithmetic on id.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.research_network
    COMMENT 'University research collaboration network — researchers and co-authorship edges';

-- ============================================================================
-- TABLE 1: researchers — 40 vertex nodes
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.research_network.researchers (
    id          BIGINT,
    name        STRING,
    department  STRING,
    rank        STRING,
    h_index     INT,
    active      BOOLEAN
) LOCATION 'graph-advanced-cypher/researchers';


INSERT INTO {{zone_name}}.research_network.researchers
SELECT
    id,
    'Prof. ' || CASE (id % 10)
        WHEN 0 THEN 'Chen'    WHEN 1 THEN 'Patel'   WHEN 2 THEN 'Mueller'
        WHEN 3 THEN 'Kim'     WHEN 4 THEN 'Garcia'   WHEN 5 THEN 'Okafor'
        WHEN 6 THEN 'Larsson' WHEN 7 THEN 'Tanaka'   WHEN 8 THEN 'Santos'
        WHEN 9 THEN 'Ali'
    END || '_' || CAST(id AS VARCHAR) AS name,
    CASE (id % 5)
        WHEN 0 THEN 'CompSci'   WHEN 1 THEN 'Physics'
        WHEN 2 THEN 'Biology'   WHEN 3 THEN 'Math'
        WHEN 4 THEN 'Chemistry'
    END AS department,
    CASE
        WHEN CAST((id - 1) / 5 AS INT) = 0 THEN 'Dean'
        WHEN CAST((id - 1) / 5 AS INT) = 1 THEN 'Professor'
        WHEN CAST((id - 1) / 5 AS INT) <= 3 THEN 'Associate'
        ELSE 'Assistant'
    END AS rank,
    5 + CAST((id * 7) % 45 AS INT) AS h_index,
    (id % 9 != 0) AS active
FROM generate_series(1, 40) AS t(id);

-- ============================================================================
-- TABLE 2: collaborations — 170 directed edges
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.research_network.collaborations (
    id              BIGINT,
    src             BIGINT,
    dst             BIGINT,
    weight          DOUBLE,
    collab_type     STRING,
    project_count   INT,
    since_year      INT
) LOCATION 'graph-advanced-cypher/collaborations';


-- Batch 1: Intra-department co-authors (~70 edges)
-- Each researcher connects to the next (+1 position) and back-skip (-2 positions)
-- within their department's circular member list. Positions are spaced by 5 in id.
-- Forward: dst = ((src + 5 - 1) % 40) + 1
-- Backward: dst = ((src + 29) % 40) + 1
-- Sources limited to ids 1-35 (ids 36-40 are isolated — no outgoing edges).
INSERT INTO {{zone_name}}.research_network.collaborations
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.1 + 0.9 * CAST((src * 7 + dst * 13) % 97 AS DOUBLE) / 97.0, 2) AS weight,
    'co-author' AS collab_type,
    1 + CAST((src + dst) % 5 AS INT) AS project_count,
    2015 + CAST((src + dst) % 10 AS INT) AS since_year
FROM (
    SELECT gs AS src, ((gs + 5 - 1) % 40) + 1 AS dst
    FROM generate_series(1, 35) AS t(gs)
    UNION ALL
    SELECT gs AS src, ((gs + 29) % 40) + 1 AS dst
    FROM generate_series(1, 35) AS t(gs)
) sub
WHERE src != dst;

-- Batch 2: Advisor-advisee hierarchy (35 edges)
-- Deans (ids 1-5, position 0) advise Professors (+5) and Associates (+10, +15).
-- Professors (ids 6-10, position 1) advise Associates (+5, +10) and Assistants (+15, +20).
INSERT INTO {{zone_name}}.research_network.collaborations
SELECT
    1000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.1 + 0.9 * CAST((src * 7 + dst * 13) % 97 AS DOUBLE) / 97.0, 2) AS weight,
    'advisor' AS collab_type,
    1 + CAST((src + dst) % 5 AS INT) AS project_count,
    2015 + CAST((src + dst) % 10 AS INT) AS since_year
FROM (
    SELECT gs AS src, gs + 5 AS dst FROM generate_series(1, 5) AS t(gs)
    UNION ALL
    SELECT gs AS src, gs + 10 AS dst FROM generate_series(1, 5) AS t(gs)
    UNION ALL
    SELECT gs AS src, gs + 15 AS dst FROM generate_series(1, 5) AS t(gs)
    UNION ALL
    SELECT gs AS src, gs + 5 AS dst FROM generate_series(6, 10) AS t(gs)
    UNION ALL
    SELECT gs AS src, gs + 10 AS dst FROM generate_series(6, 10) AS t(gs)
    UNION ALL
    SELECT gs AS src, gs + 15 AS dst FROM generate_series(6, 10) AS t(gs)
    UNION ALL
    SELECT gs AS src, gs + 20 AS dst FROM generate_series(6, 10) AS t(gs)
) sub
WHERE dst <= 40;

-- Batch 3: Cross-department committee work (35 edges)
-- Offset 3 from each source; only edges crossing department boundaries.
INSERT INTO {{zone_name}}.research_network.collaborations
SELECT
    2000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.1 + 0.9 * CAST((src * 7 + dst * 13) % 97 AS DOUBLE) / 97.0, 2) AS weight,
    'committee' AS collab_type,
    1 + CAST((src + dst) % 5 AS INT) AS project_count,
    2015 + CAST((src + dst) % 10 AS INT) AS since_year
FROM (
    SELECT gs AS src, ((gs - 1 + 3) % 40) + 1 AS dst
    FROM generate_series(1, 35) AS t(gs)
) sub
WHERE src % 5 != dst % 5;

-- Batch 4: Reviewer connections (30 edges)
-- Prime-scatter pattern: odd sources offset by 11, every-3rd sources offset by 17.
INSERT INTO {{zone_name}}.research_network.collaborations
SELECT
    3000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.1 + 0.9 * CAST((src * 7 + dst * 13) % 97 AS DOUBLE) / 97.0, 2) AS weight,
    'reviewer' AS collab_type,
    1 + CAST((src + dst) % 5 AS INT) AS project_count,
    2015 + CAST((src + dst) % 10 AS INT) AS since_year
FROM (
    SELECT gs AS src, ((gs - 1 + 11) % 40) + 1 AS dst
    FROM generate_series(1, 35, 2) AS t(gs)
    UNION ALL
    SELECT gs AS src, ((gs - 1 + 17) % 40) + 1 AS dst
    FROM generate_series(2, 35, 3) AS t(gs)
) sub
WHERE src != dst
  AND src % 5 != dst % 5;

-- ============================================================================
-- PHYSICAL LAYOUT — Z-ORDER for fast data skipping
-- ============================================================================
-- The data was inserted in id-generation order, which has reasonable locality
-- for `id` but scatters frequent filter columns (department, rank) across
-- files.  Z-ORDER rewrites files so rows with similar values on the ordering
-- keys co-locate, giving Parquet min/max statistics much tighter ranges per
-- file.  This benefits three hot paths:
--
--   1. CSR build from the collaborations table — sequential I/O on
--      `(src, dst)` ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `id` co-location lets the Parquet reader skip
--      almost every row group for targeted researcher scans.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE r.department = 'Physics' AND r.rank = 'Full Professor'` skip
--      entire files instead of reading the whole table.
--
-- One-time cost at setup; every subsequent query benefits.  These OPTIMIZE
-- statements also compact small files written by the batched edge load.
OPTIMIZE {{zone_name}}.research_network.researchers
    ZORDER BY (id, department, rank);

OPTIMIZE {{zone_name}}.research_network.collaborations
    ZORDER BY (src, dst);

-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
CREATE GRAPH IF NOT EXISTS {{zone_name}}.research_network.research_network
    VERTEX TABLE {{zone_name}}.research_network.researchers ID COLUMN id NODE TYPE COLUMN department NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.research_network.collaborations SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN collab_type
    DIRECTED;

-- ============================================================================
-- WARM CSR CACHE — Pre-build the Compressed Sparse Row topology
-- ============================================================================
-- CREATE GRAPHCSR writes the binary .dcsr file to disk, so the first Cypher
-- query loads in ~200 ms instead of rebuilding from Delta tables (6-14 s for
-- large graphs). Safe to re-run after bulk edge loads to refresh the cache.
CREATE GRAPHCSR {{zone_name}}.research_network.research_network;
