-- ============================================================================
-- Graph Flattened Mode — Setup Script
-- ============================================================================
-- Creates Delta graph tables using the FLATTENED property storage mode.
-- All vertex and edge properties are stored as individual columns — the
-- fastest mode with full predicate pushdown and direct column access.
--
--   1. persons_flattened     — 50 vertex nodes (all properties as columns)
--   2. friendships_flattened — ~150 directed edges (all properties as columns)
--
-- Dataset: 50-employee startup across 5 departments and 4 cities.
-- The graph has genuine community structure:
--   * Dense intra-department clusters (stride-5 connections)
--   * Cross-department city bonds (stride-4 connections)
--   * Hierarchical mentorship (Directors/Managers -> subordinates)
--   * Bridge employees connecting departmental silos
--   * Weak ties for small-world diameter
--
-- FLATTENED MODE ADVANTAGES:
--   - Full predicate pushdown on every column
--   - Direct column access — no JSON extraction overhead
--   - Type safety and indexing on all properties
--   - Best when the schema is known and stable
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.graph
    COMMENT 'Graph property storage mode demo tables';


-- ============================================================================
-- TABLE 1: persons_flattened — 50 vertex nodes (all properties as columns)
-- ============================================================================
-- Deterministic generation using modular arithmetic on generate_series IDs.
--   Department: id % 5  (5 depts, 10 people each)
--   City:       id % 4  (4 cities, ~12-13 people each)
--   Level:      id%10=0 -> Director, id%5=0 -> Manager, id%3=0 -> Senior, else IC
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.persons_flattened (
    id          BIGINT,
    name        STRING,
    age         INT,
    department  STRING,
    city        STRING,
    title       STRING,
    level       STRING,
    active      BOOLEAN
) LOCATION '{{data_path}}/persons_flattened';
GRANT ADMIN ON TABLE {{zone_name}}.graph.persons_flattened TO USER {{current_user}};

INSERT INTO {{zone_name}}.graph.persons_flattened
SELECT
    id,
    CASE (id % 10)
        WHEN 1 THEN 'Alice'  WHEN 2 THEN 'Bob'    WHEN 3 THEN 'Carol'
        WHEN 4 THEN 'Dave'   WHEN 5 THEN 'Eve'    WHEN 6 THEN 'Frank'
        WHEN 7 THEN 'Grace'  WHEN 8 THEN 'Hank'   WHEN 9 THEN 'Iris'
        WHEN 0 THEN 'Jack'
    END || '_' || CAST(id AS VARCHAR) AS name,
    25 + CAST(((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0) * 30.0 AS INT) AS age,
    CASE (id % 5)
        WHEN 0 THEN 'Engineering'  WHEN 1 THEN 'Marketing'
        WHEN 2 THEN 'HR'           WHEN 3 THEN 'Finance'
        WHEN 4 THEN 'Sales'
    END AS department,
    CASE (id % 4)
        WHEN 0 THEN 'NYC'     WHEN 1 THEN 'SF'
        WHEN 2 THEN 'Chicago' WHEN 3 THEN 'London'
    END AS city,
    CASE
        WHEN id % 10 = 0 THEN 'Director'
        WHEN id % 5  = 0 THEN 'Manager'
        WHEN id % 3  = 0 THEN 'Senior Engineer'
        ELSE 'Individual Contributor'
    END AS title,
    CASE
        WHEN id % 10 = 0 THEN 'L5'
        WHEN id % 5  = 0 THEN 'L4'
        WHEN id % 3  = 0 THEN 'L3'
        WHEN id % 2  = 0 THEN 'L2'
        ELSE 'L1'
    END AS level,
    (id % 7 != 0) AS active
FROM generate_series(1, 50) AS t(id);
DETECT SCHEMA FOR TABLE {{zone_name}}.graph.persons_flattened;


-- ============================================================================
-- TABLE 2: friendships_flattened — ~150 directed edges (all properties as columns)
-- ============================================================================
-- Realistic clustered edge generation using stride arithmetic.
-- Key insight: department = id%5, city = id%4.
-- Adding multiples of the stride preserves group membership:
--   Same department: dst = src + k*5
--   Same city:       dst = src + k*4
--
--   Batch 1: Intra-department colleagues  — ~50 edges (stride 5)
--   Batch 2: City cross-department social — ~30 edges (stride 4)
--   Batch 3: Hierarchical mentorship      — ~30 edges
--   Batch 4: Bridge node connections      — ~20 edges
--   Batch 5: Weak ties (pseudo-random)    — ~20 edges
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.friendships_flattened (
    id                  BIGINT,
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    since_year          INT,
    frequency           STRING,
    context             STRING,
    rating              INT
) LOCATION '{{data_path}}/friendships_flattened';
GRANT ADMIN ON TABLE {{zone_name}}.graph.friendships_flattened TO USER {{current_user}};


-- ============================================================================
-- Batch 1: Intra-department colleagues (~50 edges)
-- ============================================================================
-- Each employee connects to same-department colleagues at +5 and +10 stride.
-- Creates dense clusters within each of the 5 departments.
-- ============================================================================
INSERT INTO {{zone_name}}.graph.friendships_flattened
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 7 + dst * 13 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'colleague'
        WHEN 1 THEN 'teammate'
        WHEN 2 THEN 'desk-neighbor'
    END AS relationship_type,
    2018 + CAST((src + dst) % 7 AS INT) AS since_year,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'daily'
        WHEN 1 THEN 'weekly'
        WHEN 2 THEN 'daily'
    END AS frequency,
    'work' AS context,
    3 + CAST((src * 3 + dst) % 3 AS INT) AS rating
FROM (
    -- Sub-batch 1a: offset +5 (same department, different city)
    SELECT gs AS src, ((gs - 1 + 5) % 50) + 1 AS dst
    FROM generate_series(1, 50) AS t(gs)
    UNION ALL
    -- Sub-batch 1b: offset +10 (same department)
    SELECT gs AS src, ((gs - 1 + 10) % 50) + 1 AS dst
    FROM generate_series(1, 30) AS t(gs)
) sub
WHERE src != dst;


-- ============================================================================
-- Batch 2: City cross-department social (~30 edges)
-- ============================================================================
-- Employees in the same city but different departments form social bonds.
-- Offset +4 preserves city (id%4) but shifts department (4%5 = 4 != 0).
-- Offset +8 also preserves city (8%4=0) and shifts dept (8%5 = 3 != 0).
-- ============================================================================
INSERT INTO {{zone_name}}.graph.friendships_flattened
SELECT
    1000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.2 + 0.3 * ((CAST(src * 11 + dst * 17 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst * 2 AS BIGINT) % 3)
        WHEN 0 THEN 'city-social'
        WHEN 1 THEN 'lunch-buddy'
        WHEN 2 THEN 'gym-partner'
    END AS relationship_type,
    2020 + CAST((src + dst) % 5 AS INT) AS since_year,
    CASE (CAST(src + dst AS BIGINT) % 2)
        WHEN 0 THEN 'weekly'
        WHEN 1 THEN 'monthly'
    END AS frequency,
    'social' AS context,
    2 + CAST((src + dst) % 3 AS INT) AS rating
FROM (
    -- Sub-batch 2a: offset +4 — same city, different dept
    SELECT gs AS src, ((gs - 1 + 4) % 50) + 1 AS dst
    FROM generate_series(1, 25) AS t(gs)
    UNION ALL
    -- Sub-batch 2b: offset +8 — same city, different dept
    SELECT gs AS src, ((gs - 1 + 8) % 50) + 1 AS dst
    FROM generate_series(1, 15) AS t(gs)
) sub
WHERE src != dst
  AND (src % 5) != (dst % 5);


-- ============================================================================
-- Batch 3: Hierarchical mentorship (~30 edges)
-- ============================================================================
-- Directors (id%10=0, 5 people) mentor 3 same-dept subordinates each.
-- Managers (id%5=0, 5 more) mentor 2 same-dept subordinates each.
-- Uses stride-5 offsets to guarantee same department.
-- ============================================================================
INSERT INTO {{zone_name}}.graph.friendships_flattened
SELECT
    2000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.7 + 0.3 * ((CAST(src * 3 + dst * 7 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    'mentor' AS relationship_type,
    2019 + CAST((src + dst) % 6 AS INT) AS since_year,
    'weekly' AS frequency,
    'work' AS context,
    4 + CAST((src + dst) % 2 AS INT) AS rating
FROM (
    SELECT
        mentor_id AS src,
        ((mentor_id - 1 + k * 5) % 50) + 1 AS dst
    FROM (
        SELECT m.mentor_id, o.k
        FROM (
            -- Directors (id%10=0): 5 people
            SELECT gs * 10 AS mentor_id
            FROM generate_series(1, 5) AS t(gs)
            UNION ALL
            -- Managers (id%5=0, id%10!=0): 5 people
            SELECT gs * 5 AS mentor_id
            FROM generate_series(1, 10) AS t(gs)
            WHERE (gs * 5) % 10 != 0
        ) m
        CROSS JOIN (
            SELECT gs AS k FROM generate_series(1, 4) AS t(gs)
        ) o
        WHERE
            (m.mentor_id % 10 = 0 AND o.k <= 3)
            OR (m.mentor_id % 10 != 0 AND o.k <= 2)
    ) pairs
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 50;


-- ============================================================================
-- Batch 4: Bridge node connections (~20 edges)
-- ============================================================================
-- Bridge employees (id=13, id=26) connect departments via small offsets
-- that cross department boundaries (exclude multiples of 5).
-- ============================================================================
INSERT INTO {{zone_name}}.graph.friendships_flattened
SELECT
    3000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.3 + 0.3 * ((CAST(src * 19 + dst * 23 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'liaison'
        WHEN 1 THEN 'cross-dept-bridge'
        WHEN 2 THEN 'inter-team-link'
    END AS relationship_type,
    2021 + CAST((src + dst) % 4 AS INT) AS since_year,
    'monthly' AS frequency,
    'work' AS context,
    3 + CAST((src + dst) % 2 AS INT) AS rating
FROM (
    SELECT
        bridge_id AS src,
        ((bridge_id - 1 + offset) % 50) + 1 AS dst
    FROM (
        SELECT 13 AS bridge_id
        UNION ALL SELECT 26
    ) bridges
    CROSS JOIN (
        SELECT gs AS offset
        FROM generate_series(1, 12) AS t(gs)
        WHERE gs % 5 != 0
    ) offsets
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 50;


-- ============================================================================
-- Batch 5: Weak ties — pseudo-random long-range connections (~20 edges)
-- ============================================================================
-- Random acquaintances: conference contacts, alumni connections.
-- Uses prime multipliers for deterministic scattering.
-- ============================================================================
INSERT INTO {{zone_name}}.graph.friendships_flattened
SELECT
    4000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.1 + 0.15 * ((CAST(src * 43 + dst * 47 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src * 7 + dst * 3 AS BIGINT) % 4)
        WHEN 0 THEN 'acquaintance'
        WHEN 1 THEN 'conference-contact'
        WHEN 2 THEN 'alumni'
        WHEN 3 THEN 'referral'
    END AS relationship_type,
    2022 + CAST((src + dst) % 3 AS INT) AS since_year,
    'rarely' AS frequency,
    'social' AS context,
    1 + CAST((src + dst) % 3 AS INT) AS rating
FROM (
    SELECT
        ((i * 17 + 3) % 50) + 1 AS src,
        ((i * 31 + 11) % 50) + 1 AS dst
    FROM generate_series(1, 25) AS t(i)
) sub
WHERE src != dst;
DETECT SCHEMA FOR TABLE {{zone_name}}.graph.friendships_flattened;


-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
CREATE GRAPH IF NOT EXISTS {{zone_name}}.graph.flattened_demo
    VERTEX TABLE {{zone_name}}.graph.persons_flattened ID COLUMN id NODE TYPE COLUMN department NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.graph.friendships_flattened SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN relationship_type
    DIRECTED;
