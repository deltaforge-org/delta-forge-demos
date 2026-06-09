-- ============================================================================
-- Graph Stress Test — Setup Script
-- ============================================================================
-- Creates a 1,000,000-node graph with ~5,000,000 directed edges simulating
-- a realistic enterprise organization network. The graph has genuine
-- community structure: departments form tight clusters, project teams
-- form nested sub-clusters, cities create cross-department communities,
-- and explicit bridge nodes connect the clusters.
--
-- Topology features:
--   • Dense intra-department neighborhoods (stride-20 arithmetic)
--   • Nested project-team sub-clusters (stride-200)
--   • Cross-department city communities (stride-15)
--   • Hierarchical mentorship trees (L5+ → subordinates)
--   • Bridge nodes (2% of people) connecting departments
--   • Power-law degree distribution (VPs ~150+ edges, Associates ~2-5)
--   • Small-world weak ties for low diameter
--
-- Tables and views:
--   1. st_departments   — 20 department lookup records
--   2. st_people        — 1,000,000 vertex nodes (deterministic generation)
--   3. st_edges         — ~5,000,000 directed edges (7 batches, deterministic)
--   4. st_people_stats  (VIEW) — per-person degree centrality metrics
--   5. st_dept_matrix   (VIEW) — cross-department connection matrix
--
-- WARNING: This demo generates very large datasets. Setup may take several
-- minutes depending on hardware.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.stress_test_network
    COMMENT '1M-node enterprise organization network stress test';
-- ============================================================================
-- TABLE 1: departments — 20 department lookup records
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.stress_test_network.st_departments (
    dept_id     INT,
    dept_name   STRING,
    floor_num   INT,
    budget_k    INT,
    region      STRING
) LOCATION 'graph-stress-test/st_departments';


INSERT INTO {{zone_name}}.stress_test_network.st_departments VALUES
    (0,  'Engineering',       3, 8000, 'Americas'),
    (1,  'Marketing',         2, 3000, 'Americas'),
    (2,  'HR',                1, 2000, 'Americas'),
    (3,  'Finance',           4, 2500, 'EMEA'),
    (4,  'Sales',             2, 5000, 'EMEA'),
    (5,  'Operations',        1, 3500, 'Americas'),
    (6,  'Legal',             4, 1800, 'EMEA'),
    (7,  'Product',           3, 4000, 'Americas'),
    (8,  'Data Science',      3, 3500, 'APAC'),
    (9,  'DevOps',            3, 2800, 'Americas'),
    (10, 'Security',          4, 3000, 'EMEA'),
    (11, 'Customer Support',  1, 2200, 'APAC'),
    (12, 'Research',          5, 6000, 'Americas'),
    (13, 'Design',            2, 2000, 'EMEA'),
    (14, 'QA',                3, 1500, 'APAC'),
    (15, 'Platform',          3, 4500, 'Americas'),
    (16, 'Infrastructure',    5, 5000, 'EMEA'),
    (17, 'Analytics',         2, 2800, 'APAC'),
    (18, 'Mobile',            3, 3200, 'Americas'),
    (19, 'AI/ML',             5, 7000, 'APAC');

-- ============================================================================
-- TABLE 2: st_people — 1,000,000 vertex nodes
-- ============================================================================
-- Deterministic generation using modular arithmetic on generate_series IDs.
-- Each person gets name, department, city, project team, hire year, level,
-- and salary band derived from their ID for full reproducibility.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.stress_test_network.st_people (
    id              BIGINT,
    name            STRING,
    age             INT,
    department      STRING,
    city            STRING,
    project_team    STRING,
    title           STRING,
    hire_year       INT,
    level           STRING,
    salary_band     STRING,
    active          BOOLEAN
) LOCATION 'graph-stress-test/st_people';


INSERT INTO {{zone_name}}.stress_test_network.st_people
SELECT
    id,
    -- First name from pool of 40 + ID suffix for uniqueness
    CASE (id % 40)
        WHEN 0  THEN 'Priya'    WHEN 1  THEN 'Marcus'   WHEN 2  THEN 'Sofia'
        WHEN 3  THEN 'James'    WHEN 4  THEN 'Wei'      WHEN 5  THEN 'Elena'
        WHEN 6  THEN 'Raj'      WHEN 7  THEN 'Kenji'    WHEN 8  THEN 'Amara'
        WHEN 9  THEN 'Luca'     WHEN 10 THEN 'Fatima'   WHEN 11 THEN 'Carlos'
        WHEN 12 THEN 'Yuki'     WHEN 13 THEN 'Nadia'    WHEN 14 THEN 'Omar'
        WHEN 15 THEN 'Ingrid'   WHEN 16 THEN 'Dmitri'   WHEN 17 THEN 'Aisha'
        WHEN 18 THEN 'Tomas'    WHEN 19 THEN 'Mei'      WHEN 20 THEN 'Henrik'
        WHEN 21 THEN 'Zara'     WHEN 22 THEN 'Mateo'    WHEN 23 THEN 'Suki'
        WHEN 24 THEN 'Andre'    WHEN 25 THEN 'Leila'    WHEN 26 THEN 'Chen'
        WHEN 27 THEN 'Rosa'     WHEN 28 THEN 'Vikram'   WHEN 29 THEN 'Astrid'
        WHEN 30 THEN 'Felix'    WHEN 31 THEN 'Naomi'    WHEN 32 THEN 'Pavel'
        WHEN 33 THEN 'Lucia'    WHEN 34 THEN 'Tariq'    WHEN 35 THEN 'Elin'
        WHEN 36 THEN 'Kofi'     WHEN 37 THEN 'Maren'    WHEN 38 THEN 'Dante'
        WHEN 39 THEN 'Isla'
    END || '_' || CAST(id AS VARCHAR) AS name,
    -- Age: 22–59 range, deterministic via golden ratio (truncated CAST to INT)
    22 + CAST(((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0) * 38.0 AS INT) AS age,
    -- Department: 20 departments
    CASE (id % 20)
        WHEN 0  THEN 'Engineering'      WHEN 1  THEN 'Marketing'
        WHEN 2  THEN 'HR'               WHEN 3  THEN 'Finance'
        WHEN 4  THEN 'Sales'            WHEN 5  THEN 'Operations'
        WHEN 6  THEN 'Legal'            WHEN 7  THEN 'Product'
        WHEN 8  THEN 'Data Science'     WHEN 9  THEN 'DevOps'
        WHEN 10 THEN 'Security'         WHEN 11 THEN 'Customer Support'
        WHEN 12 THEN 'Research'         WHEN 13 THEN 'Design'
        WHEN 14 THEN 'QA'              WHEN 15 THEN 'Platform'
        WHEN 16 THEN 'Infrastructure'   WHEN 17 THEN 'Analytics'
        WHEN 18 THEN 'Mobile'           WHEN 19 THEN 'AI/ML'
    END AS department,
    -- City: 15 cities
    CASE (id % 15)
        WHEN 0  THEN 'NYC'         WHEN 1  THEN 'SF'
        WHEN 2  THEN 'Chicago'     WHEN 3  THEN 'London'
        WHEN 4  THEN 'Berlin'      WHEN 5  THEN 'Tokyo'
        WHEN 6  THEN 'Sydney'      WHEN 7  THEN 'Toronto'
        WHEN 8  THEN 'Singapore'   WHEN 9  THEN 'Dublin'
        WHEN 10 THEN 'Seattle'     WHEN 11 THEN 'Austin'
        WHEN 12 THEN 'Amsterdam'   WHEN 13 THEN 'Mumbai'
        WHEN 14 THEN 'Paris'
    END AS city,
    -- Project team: 200 teams
    'Team_' || CAST((id % 200) + 1 AS VARCHAR) AS project_team,
    -- Title based on seniority band
    CASE
        WHEN id % 1000 = 0 THEN 'VP'
        WHEN id % 500  = 0 THEN 'Director'
        WHEN id % 100  = 0 THEN 'Senior Manager'
        WHEN id % 50   = 0 THEN 'Manager'
        WHEN id % 20   = 0 THEN 'Senior Engineer'
        WHEN id % 5    = 0 THEN 'Engineer'
        ELSE 'Associate'
    END AS title,
    -- Hire year: 2010–2025
    2010 + CAST(id % 16 AS INT) AS hire_year,
    -- Level: derived from title
    CASE
        WHEN id % 1000 = 0 THEN 'L8'
        WHEN id % 500  = 0 THEN 'L7'
        WHEN id % 100  = 0 THEN 'L6'
        WHEN id % 50   = 0 THEN 'L5'
        WHEN id % 20   = 0 THEN 'L4'
        WHEN id % 5    = 0 THEN 'L3'
        WHEN id % 3    = 0 THEN 'L2'
        ELSE 'L1'
    END AS level,
    -- Salary band
    CASE
        WHEN id % 1000 = 0 THEN 'Executive'
        WHEN id % 500  = 0 THEN 'Band-5'
        WHEN id % 100  = 0 THEN 'Band-4'
        WHEN id % 50   = 0 THEN 'Band-3'
        WHEN id % 20   = 0 THEN 'Band-2'
        ELSE 'Band-1'
    END AS salary_band,
    -- Active: ~95% active
    (id % 21 != 0) AS active
FROM generate_series(1, 1000000) AS t(id);

-- ============================================================================
-- TABLE 3: st_edges — ~5,000,000 directed edges (7 batches)
-- ============================================================================
-- Realistic clustered graph with community structure, bridge nodes,
-- hierarchical mentorship, and power-law degree distribution.
--
-- Key insight: department = id%20, city = id%15, team = id%200.
-- People in the same group are NOT contiguous — they are interleaved
-- with stride equal to the modulus. Adding multiples of the stride
-- to an ID preserves group membership:
--   Same department: dst = src + k*20
--   Same team:       dst = src + k*200
--   Same city:       dst = src + k*15
--
--   Batch 1: Intra-department neighborhoods   — ~1.5M edges  (stride 20)
--   Batch 2: Intra-team project connections   — ~1.0M edges  (stride 200)
--   Batch 3: City cross-department social     — ~800K edges   (stride 15)
--   Batch 4: Hierarchical mentorship          — ~550K edges   (L5+ → dept)
--   Batch 5: Bridge node inter-department     — ~400K edges   (id%100<2)
--   Batch 6: Hub node extra connections       — ~490K edges   (L4+ wide reach)
--   Batch 7: Weak ties (pseudo-random)        — ~300K edges   (prime scatter)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.stress_test_network.st_edges (
    id                  BIGINT,
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    since_year          INT
) LOCATION 'graph-stress-test/st_edges';

-- ============================================================================
-- Batch 1: Intra-department local neighborhood (~1.5M edges)
-- ============================================================================
-- Each person connects to 1-2 same-department colleagues nearby in ID space.
-- Adding multiples of 20 preserves department assignment (id % 20).
-- Creates dense local clusters within each of the 20 departments.
-- ============================================================================
INSERT INTO {{zone_name}}.stress_test_network.st_edges
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 7 + dst * 13 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 4)
        WHEN 0 THEN 'colleague'
        WHEN 1 THEN 'desk-neighbor'
        WHEN 2 THEN 'teammate'
        WHEN 3 THEN 'collaborator'
    END AS relationship_type,
    2015 + CAST((src + dst) % 11 AS INT) AS since_year
FROM (
    -- Sub-batch 1a: Each person to person at +20 (one dept stride) — ~1M edges
    SELECT
        ((gs - 1) % 1000000) + 1 AS src,
        (((gs - 1) % 1000000 + 20) % 1000000) + 1 AS dst
    FROM generate_series(1, 1000000) AS t(gs)
    UNION ALL
    -- Sub-batch 1b: First 500K people also connect to person at +40 — ~500K edges
    SELECT
        ((gs - 1) % 1000000) + 1 AS src,
        (((gs - 1) % 1000000 + 40) % 1000000) + 1 AS dst
    FROM generate_series(1, 500000) AS t(gs)
) sub
WHERE src != dst
  AND src BETWEEN 1 AND 1000000
  AND dst BETWEEN 1 AND 1000000;
-- ============================================================================
-- Batch 2: Intra-team project connections (~1.0M edges)
-- ============================================================================
-- Connects teammates within project teams (id % 200 groups).
-- Adding multiples of 200 preserves team assignment. Since 200 = 20×10,
-- this also preserves department — teams are tight sub-clusters nested
-- inside department clusters.
-- ============================================================================
INSERT INTO {{zone_name}}.stress_test_network.st_edges
SELECT
    10000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.5 + 0.4 * ((CAST(src * 11 + dst * 17 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src * 3 + dst AS BIGINT) % 3)
        WHEN 0 THEN 'project-mate'
        WHEN 1 THEN 'sprint-partner'
        WHEN 2 THEN 'code-reviewer'
    END AS relationship_type,
    2018 + CAST((src + dst) % 8 AS INT) AS since_year
FROM (
    -- Sub-batch 2a: Each person to person at +200 (one team stride) — ~700K edges
    SELECT
        ((gs - 1) % 1000000) + 1 AS src,
        (((gs - 1) % 1000000 + 200) % 1000000) + 1 AS dst
    FROM generate_series(1, 700000) AS t(gs)
    UNION ALL
    -- Sub-batch 2b: First 300K people also connect to person at +400 — ~300K edges
    SELECT
        ((gs - 1) % 1000000) + 1 AS src,
        (((gs - 1) % 1000000 + 400) % 1000000) + 1 AS dst
    FROM generate_series(1, 300000) AS t(gs)
) sub
WHERE src != dst
  AND src BETWEEN 1 AND 1000000
  AND dst BETWEEN 1 AND 1000000;
-- ============================================================================
-- Batch 3: City-local cross-department connections (~800K edges)
-- ============================================================================
-- People in the same city but different departments form social bonds.
-- Adding multiples of 15 preserves city assignment (id % 15).
-- Offsets 15/30/45 shift department by 15/10/5 (all ≠ 0), creating a
-- secondary clustering dimension that overlaps with departments.
-- ============================================================================
INSERT INTO {{zone_name}}.stress_test_network.st_edges
SELECT
    20000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.2 + 0.3 * ((CAST(src * 23 + dst * 29 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst * 2 AS BIGINT) % 4)
        WHEN 0 THEN 'city-social'
        WHEN 1 THEN 'lunch-buddy'
        WHEN 2 THEN 'commute-buddy'
        WHEN 3 THEN 'gym-partner'
    END AS relationship_type,
    2019 + CAST((src + dst) % 7 AS INT) AS since_year
FROM (
    -- Sub-batch 3a: offset +15 (shifts dept by 15) — ~400K edges
    SELECT
        ((gs - 1) % 1000000) + 1 AS src,
        (((gs - 1) % 1000000 + 15) % 1000000) + 1 AS dst
    FROM generate_series(1, 400000) AS t(gs)
    UNION ALL
    -- Sub-batch 3b: offset +30 (shifts dept by 10) — ~250K edges
    SELECT
        ((gs - 1) % 1000000) + 1 AS src,
        (((gs - 1) % 1000000 + 30) % 1000000) + 1 AS dst
    FROM generate_series(1, 250000) AS t(gs)
    UNION ALL
    -- Sub-batch 3c: offset +45 (shifts dept by 5) — ~150K edges
    SELECT
        ((gs - 1) % 1000000) + 1 AS src,
        (((gs - 1) % 1000000 + 45) % 1000000) + 1 AS dst
    FROM generate_series(1, 150000) AS t(gs)
) sub
WHERE src != dst
  AND src BETWEEN 1 AND 1000000
  AND dst BETWEEN 1 AND 1000000
  AND (src % 20) != (dst % 20);
-- ============================================================================
-- Batch 4: Hierarchical mentorship (~550K edges)
-- ============================================================================
-- Senior employees (L5+) mentor subordinates in their own department.
-- Each mentor connects to people at mentor_id + k*20 (same department).
-- Higher-level executives mentor more people, creating a power-law:
--   L8 (VP, id%1000=0):        1,000 people × 100 mentees = 100K edges
--   L7 (Director, id%500=0):   1,000 people ×  60 mentees =  60K edges
--   L6 (Sr Mgr, id%100=0):     8,000 people ×  30 mentees = 240K edges
--   L5 (Manager, id%50=0):     10,000 people ×  15 mentees = 150K edges
-- ============================================================================
INSERT INTO {{zone_name}}.stress_test_network.st_edges
SELECT
    30000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 3 + dst * 7 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    'mentor' AS relationship_type,
    2016 + CAST((src + dst) % 10 AS INT) AS since_year
FROM (
    SELECT
        mentor_id AS src,
        ((mentor_id - 1 + k * 20) % 1000000) + 1 AS dst
    FROM (
        SELECT
            m.mentor_id,
            o.k
        FROM (
            SELECT gs * 50 AS mentor_id
            FROM generate_series(1, 20000) AS t(gs)
        ) m
        CROSS JOIN (
            SELECT gs AS k FROM generate_series(1, 100) AS t(gs)
        ) o
        WHERE
            (m.mentor_id % 1000 = 0 AND o.k <= 100)
            OR (m.mentor_id % 1000 != 0 AND m.mentor_id % 500 = 0 AND o.k <= 60)
            OR (m.mentor_id % 500 != 0 AND m.mentor_id % 100 = 0 AND o.k <= 30)
            OR (m.mentor_id % 100 != 0 AND o.k <= 15)
    ) pairs
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 1000000;
-- ============================================================================
-- Batch 5: Bridge node cross-department connections (~400K edges)
-- ============================================================================
-- Bridge nodes (id % 100 < 2, ~20,000 people = 2% of workforce) serve as
-- explicit inter-department connectors — the liaison/cross-functional roles.
-- Each bridge connects to 20 people in different departments via small
-- offsets (1..21 excluding 20). Each offset shifts department differently,
-- creating star-shaped cross-department structures around bridge nodes.
-- ============================================================================
INSERT INTO {{zone_name}}.stress_test_network.st_edges
SELECT
    40000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.3 + 0.3 * ((CAST(src * 19 + dst * 23 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'liaison'
        WHEN 1 THEN 'cross-dept-bridge'
        WHEN 2 THEN 'inter-team-link'
    END AS relationship_type,
    2017 + CAST((src + dst) % 9 AS INT) AS since_year
FROM (
    SELECT
        bridge_id AS src,
        ((bridge_id - 1 + offset) % 1000000) + 1 AS dst
    FROM (
        SELECT gs AS bridge_id
        FROM generate_series(1, 1000000) AS t(gs)
        WHERE gs % 100 < 2
    ) bridges
    CROSS JOIN (
        -- 20 offsets: 1..21 excluding 20 (which preserves department)
        SELECT gs AS offset
        FROM generate_series(1, 21) AS t(gs)
        WHERE gs != 20
    ) offsets
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 1000000;
-- ============================================================================
-- Batch 6: Hub node extra connections (~490K edges)
-- ============================================================================
-- Power-law degree boost for senior people (L4+, id%20=0).
-- Stride 7 is coprime to 20, so hub connections span across departments,
-- giving leaders broad cross-organizational networks.
--   L8 (id%1000=0):   1,000 × 50 =  50K edges
--   L7 (id%500=0):    1,000 × 40 =  40K edges
--   L6 (id%100=0):    8,000 × 25 = 200K edges
--   L4 (id%20=0):    40,000 ×  5 = 200K edges
-- ============================================================================
INSERT INTO {{zone_name}}.stress_test_network.st_edges
SELECT
    50000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.4 + 0.4 * ((CAST(src * 31 + dst * 37 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'leadership-network'
        WHEN 1 THEN 'executive-link'
        WHEN 2 THEN 'strategic-partner'
    END AS relationship_type,
    2014 + CAST((src + dst) % 12 AS INT) AS since_year
FROM (
    SELECT
        hub_id AS src,
        ((hub_id - 1 + k * 7) % 1000000) + 1 AS dst
    FROM (
        SELECT
            m.hub_id,
            o.k
        FROM (
            SELECT gs * 20 AS hub_id
            FROM generate_series(1, 50000) AS t(gs)
        ) m
        CROSS JOIN (
            SELECT gs AS k FROM generate_series(1, 50) AS t(gs)
        ) o
        WHERE
            (m.hub_id % 1000 = 0 AND o.k <= 50)
            OR (m.hub_id % 1000 != 0 AND m.hub_id % 500 = 0 AND o.k <= 40)
            OR (m.hub_id % 500 != 0 AND m.hub_id % 100 = 0 AND o.k <= 25)
            OR (m.hub_id % 100 != 0 AND o.k <= 5)
    ) pairs
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 1000000;
-- ============================================================================
-- Batch 7: Weak ties — pseudo-random long-range connections (~300K edges)
-- ============================================================================
-- Small-world property: deterministic pseudo-random connections using
-- large prime multipliers for uniform scattering across the ID space.
-- These are the "random acquaintances" — conference contacts, alumni
-- connections, referrals — that cut the graph diameter from O(N) to
-- O(log N) while contributing minimal edge weight.
-- ============================================================================
INSERT INTO {{zone_name}}.stress_test_network.st_edges
SELECT
    60000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.05 + 0.15 * ((CAST(src * 43 + dst * 47 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src * 7 + dst * 3 AS BIGINT) % 4)
        WHEN 0 THEN 'acquaintance'
        WHEN 1 THEN 'conference-contact'
        WHEN 2 THEN 'alumni-connection'
        WHEN 3 THEN 'referral'
    END AS relationship_type,
    2022 + CAST((src + dst) % 4 AS INT) AS since_year
FROM (
    SELECT
        ((i * 104729 + 56891) % 1000000) + 1 AS src,
        ((i * 224737 + 31547) % 1000000) + 1 AS dst
    FROM generate_series(1, 320000) AS t(i)
) sub
WHERE src != dst;

-- ============================================================================
-- PHYSICAL LAYOUT — Z-ORDER for fast data skipping
-- ============================================================================
-- At 1M nodes / 5M edges, rows were inserted in id-generation order, which
-- has reasonable locality for `id` but scatters frequent filter columns
-- (department, level) across files.  Z-ORDER rewrites files so rows with
-- similar values on the ordering keys co-locate, giving Parquet min/max
-- statistics much tighter ranges per file.  This benefits three hot paths:
--
--   1. CSR build from the edges table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `id` co-location lets the Parquet reader skip
--      almost every row group for targeted person scans.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE p.department = 'Engineering' AND p.level = 'L5'` skip entire
--      files instead of reading the whole table.
--
-- One-time cost at setup; every subsequent query benefits.
OPTIMIZE {{zone_name}}.stress_test_network.st_people
    ZORDER BY (id, department, level);

OPTIMIZE {{zone_name}}.stress_test_network.st_edges
    ZORDER BY (src, dst);

-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
CREATE GRAPH IF NOT EXISTS {{zone_name}}.stress_test_network.stress_test_network
    VERTEX TABLE {{zone_name}}.stress_test_network.st_people ID COLUMN id NODE TYPE COLUMN department NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.stress_test_network.st_edges SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN relationship_type
    DIRECTED;

-- ============================================================================
-- WARM CSR CACHE — Pre-build the Compressed Sparse Row topology
-- ============================================================================
-- At 1M nodes and 5M edges, pre-building the CSR once upfront writes a .dcsr
-- sidecar to disk so the first Cypher query loads in ~200 ms instead of
-- rebuilding from Delta tables. Safe to re-run after bulk edge loads to
-- refresh the cache.
CREATE GRAPHCSR {{zone_name}}.stress_test_network.stress_test_network;

-- ============================================================================
-- VIEW 4: st_people_stats — per-person degree centrality
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.stress_test_network.st_people_stats AS
SELECT
    p.id,
    p.name,
    p.department,
    p.city,
    p.level,
    p.project_team,
    COALESCE(out_deg.out_degree, 0) AS out_degree,
    COALESCE(in_deg.in_degree, 0) AS in_degree,
    COALESCE(out_deg.out_degree, 0) + COALESCE(in_deg.in_degree, 0) AS total_degree
FROM {{zone_name}}.stress_test_network.st_people p
LEFT JOIN (
    SELECT src, COUNT(*) AS out_degree FROM {{zone_name}}.stress_test_network.st_edges GROUP BY src
) out_deg ON p.id = out_deg.src
LEFT JOIN (
    SELECT dst, COUNT(*) AS in_degree FROM {{zone_name}}.stress_test_network.st_edges GROUP BY dst
) in_deg ON p.id = in_deg.dst;
-- ============================================================================
-- VIEW 5: st_dept_matrix — cross-department connection matrix
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.stress_test_network.st_dept_matrix AS
SELECT
    src_p.department AS src_dept,
    dst_p.department AS dst_dept,
    COUNT(*) AS connection_count,
    ROUND(AVG(e.weight), 3) AS avg_weight,
    COUNT(DISTINCT e.relationship_type) AS rel_type_count
FROM {{zone_name}}.stress_test_network.st_edges e
JOIN {{zone_name}}.stress_test_network.st_people src_p ON e.src = src_p.id
JOIN {{zone_name}}.stress_test_network.st_people dst_p ON e.dst = dst_p.id
GROUP BY src_p.department, dst_p.department;
