-- ============================================================================
-- Graph Social Network — Setup Script
-- ============================================================================
-- Creates a realistic 100-employee startup social network with ~300 directed
-- connections across 8 departments and 5 cities. The graph has genuine
-- community structure visible at human-readable scale:
--
-- Topology features:
--   • Dense intra-department clusters (stride-8 arithmetic)
--   • Cross-department city communities (stride-5)
--   • Hierarchical mentorship (Directors/Sr Managers → subordinates)
--   • Bridge employees connecting departments
--   • Weighted edges reflecting relationship strength
--
-- Tables and views:
--   1. departments       — 8 department lookup records
--   2. employees         — 100 employee vertex nodes
--   3. connections       — ~300 directed edges (5 batches)
--   4. employee_stats    (VIEW) — per-employee degree centrality
--   5. dept_connections  (VIEW) — cross-department connection matrix
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.graph
    COMMENT 'Graph property storage mode demo tables';


-- ============================================================================
-- TABLE 1: departments — 8 department lookup records
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.departments (
    dept_id     INT,
    dept_name   STRING,
    floor_num   INT,
    budget_k    INT
) LOCATION '{{data_path}}/departments';

GRANT ADMIN ON TABLE {{zone_name}}.graph.departments TO USER {{current_user}};

INSERT INTO {{zone_name}}.graph.departments VALUES
    (0, 'Engineering',  3, 5000),
    (1, 'Marketing',    2, 2000),
    (2, 'HR',           1, 1500),
    (3, 'Finance',      4, 1800),
    (4, 'Sales',        2, 3000),
    (5, 'Operations',   1, 2500),
    (6, 'Legal',        4, 1200),
    (7, 'Product',      3, 2200);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.departments;


-- ============================================================================
-- TABLE 2: employees — 100 employee vertex nodes
-- ============================================================================
-- Deterministic generation using modular arithmetic on generate_series IDs.
--   Department: id % 8  (8 depts, ~12-13 people each)
--   City:       id % 5  (5 cities, 20 people each)
--   Level:      id%10=0 → Director, id%5=0 → Sr Mgr, id%3=0 → Manager, else IC
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.employees (
    id          BIGINT,
    name        STRING,
    age         INT,
    department  STRING,
    city        STRING,
    title       STRING,
    hire_year   INT,
    level       STRING,
    active      BOOLEAN
) LOCATION '{{data_path}}/employees';

GRANT ADMIN ON TABLE {{zone_name}}.graph.employees TO USER {{current_user}};

INSERT INTO {{zone_name}}.graph.employees
SELECT
    id,
    CASE (id % 20)
        WHEN 0  THEN 'Priya'    WHEN 1  THEN 'Marcus'   WHEN 2  THEN 'Sofia'
        WHEN 3  THEN 'James'    WHEN 4  THEN 'Wei'      WHEN 5  THEN 'Elena'
        WHEN 6  THEN 'Raj'      WHEN 7  THEN 'Kenji'    WHEN 8  THEN 'Amara'
        WHEN 9  THEN 'Luca'     WHEN 10 THEN 'Fatima'   WHEN 11 THEN 'Carlos'
        WHEN 12 THEN 'Yuki'     WHEN 13 THEN 'Nadia'    WHEN 14 THEN 'Omar'
        WHEN 15 THEN 'Ingrid'   WHEN 16 THEN 'Dmitri'   WHEN 17 THEN 'Aisha'
        WHEN 18 THEN 'Tomas'    WHEN 19 THEN 'Mei'
    END || '_' || CAST(id AS VARCHAR) AS name,
    -- Age: 23–55 range, deterministic via golden ratio
    23 + CAST(((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0) * 32.0 AS INT) AS age,
    -- Department: 8 departments
    CASE (id % 8)
        WHEN 0 THEN 'Engineering'  WHEN 1 THEN 'Marketing'
        WHEN 2 THEN 'HR'           WHEN 3 THEN 'Finance'
        WHEN 4 THEN 'Sales'        WHEN 5 THEN 'Operations'
        WHEN 6 THEN 'Legal'        WHEN 7 THEN 'Product'
    END AS department,
    -- City: 5 cities
    CASE (id % 5)
        WHEN 0 THEN 'NYC'     WHEN 1 THEN 'SF'       WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'London'  WHEN 4 THEN 'Berlin'
    END AS city,
    -- Title based on seniority band
    CASE
        WHEN id % 10 = 0 THEN 'Director'
        WHEN id % 5  = 0 THEN 'Senior Manager'
        WHEN id % 3  = 0 THEN 'Manager'
        ELSE 'Individual Contributor'
    END AS title,
    -- Hire year: 2015–2024
    2015 + CAST(id % 10 AS INT) AS hire_year,
    -- Level: derived from title
    CASE
        WHEN id % 10 = 0 THEN 'L6'
        WHEN id % 5  = 0 THEN 'L5'
        WHEN id % 3  = 0 THEN 'L4'
        WHEN id % 2  = 0 THEN 'L3'
        ELSE 'L2'
    END AS level,
    -- Active: ~90% active
    (id % 11 != 0) AS active
FROM generate_series(1, 100) AS t(id);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.employees;


-- ============================================================================
-- TABLE 3: connections — ~300 directed edges (5 batches)
-- ============================================================================
-- Realistic clustered edge generation using stride arithmetic.
--
-- Key insight: department = id%8, city = id%5.
-- Adding multiples of the stride preserves group membership:
--   Same department: dst = src + k*8
--   Same city:       dst = src + k*5
--
--   Batch 1: Intra-department colleagues    — ~100 edges (stride 8)
--   Batch 2: City cross-department social   — ~60 edges  (stride 5)
--   Batch 3: Hierarchical mentorship        — ~60 edges  (Directors/Sr Mgrs → dept)
--   Batch 4: Bridge node connections        — ~40 edges  (cross-dept connectors)
--   Batch 5: Weak ties (pseudo-random)      — ~40 edges  (long-range acquaintances)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.connections (
    id                  BIGINT,
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    since_year          INT
) LOCATION '{{data_path}}/connections';

GRANT ADMIN ON TABLE {{zone_name}}.graph.connections TO USER {{current_user}};


-- ============================================================================
-- Batch 1: Intra-department colleagues (~100 edges)
-- ============================================================================
-- Each employee connects to 1 same-department colleague at +8 stride.
-- Creates dense clusters within each of the 8 departments.
-- ============================================================================
INSERT INTO {{zone_name}}.graph.connections
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 7 + dst * 13 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'colleague'
        WHEN 1 THEN 'desk-neighbor'
        WHEN 2 THEN 'teammate'
    END AS relationship_type,
    2018 + CAST((src + dst) % 7 AS INT) AS since_year
FROM (
    SELECT
        gs AS src,
        ((gs - 1 + 8) % 100) + 1 AS dst
    FROM generate_series(1, 100) AS t(gs)
) sub
WHERE src != dst;


-- ============================================================================
-- Batch 2: City cross-department social (~60 edges)
-- ============================================================================
-- Employees in the same city but different departments form social bonds.
-- Offset +5 preserves city (id%5) but shifts department (5%8 = 5 ≠ 0).
-- Offset +10 also preserves city (10%5=0) and shifts dept (10%8 = 2 ≠ 0).
-- Only first 60 people get these to avoid duplicating the whole graph.
-- ============================================================================
INSERT INTO {{zone_name}}.graph.connections
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
    2020 + CAST((src + dst) % 5 AS INT) AS since_year
FROM (
    -- Sub-batch 2a: offset +5 — ~40 edges
    SELECT
        gs AS src,
        ((gs - 1 + 5) % 100) + 1 AS dst
    FROM generate_series(1, 40) AS t(gs)
    UNION ALL
    -- Sub-batch 2b: offset +10 — ~20 edges
    SELECT
        gs AS src,
        ((gs - 1 + 10) % 100) + 1 AS dst
    FROM generate_series(1, 20) AS t(gs)
) sub
WHERE src != dst
  AND (src % 8) != (dst % 8);


-- ============================================================================
-- Batch 3: Hierarchical mentorship (~60 edges)
-- ============================================================================
-- Directors (id%10=0, 10 people) and Senior Managers (id%5=0, 10 more)
-- mentor subordinates in their department via stride-8 connections.
-- Directors get 4 mentees each, Sr Managers get 2, creating a visible
-- hierarchical spine within each department.
-- ============================================================================
INSERT INTO {{zone_name}}.graph.connections
SELECT
    2000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src,
    dst,
    ROUND(0.7 + 0.3 * ((CAST(src * 3 + dst * 7 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    'mentor' AS relationship_type,
    2019 + CAST((src + dst) % 6 AS INT) AS since_year
FROM (
    SELECT
        mentor_id AS src,
        ((mentor_id - 1 + k * 8) % 100) + 1 AS dst
    FROM (
        SELECT
            m.mentor_id,
            o.k
        FROM (
            -- Directors (id%10=0): 10 people
            SELECT gs * 10 AS mentor_id
            FROM generate_series(1, 10) AS t(gs)
            UNION ALL
            -- Senior Managers (id%5=0, id%10!=0): 10 people
            SELECT gs * 5 AS mentor_id
            FROM generate_series(1, 20) AS t(gs)
            WHERE (gs * 5) % 10 != 0
        ) m
        CROSS JOIN (
            SELECT gs AS k FROM generate_series(1, 4) AS t(gs)
        ) o
        WHERE
            (m.mentor_id % 10 = 0 AND o.k <= 4)    -- Directors: 4 mentees
            OR (m.mentor_id % 10 != 0 AND o.k <= 2) -- Sr Managers: 2 mentees
    ) pairs
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 100;


-- ============================================================================
-- Batch 4: Bridge node connections (~40 edges)
-- ============================================================================
-- Bridge employees (id%25=0: employees 25, 50, 75, 100) are cross-functional
-- connectors. Each bridge connects to ~10 employees in other departments
-- via small offsets (1..12 excluding multiples of 8).
-- ============================================================================
INSERT INTO {{zone_name}}.graph.connections
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
    2021 + CAST((src + dst) % 4 AS INT) AS since_year
FROM (
    SELECT
        bridge_id AS src,
        ((bridge_id - 1 + offset) % 100) + 1 AS dst
    FROM (
        SELECT gs * 25 AS bridge_id
        FROM generate_series(1, 4) AS t(gs)
    ) bridges
    CROSS JOIN (
        -- 10 offsets that change department (exclude multiples of 8)
        SELECT gs AS offset
        FROM generate_series(1, 12) AS t(gs)
        WHERE gs % 8 != 0
    ) offsets
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 100;


-- ============================================================================
-- Batch 5: Weak ties — pseudo-random long-range connections (~40 edges)
-- ============================================================================
-- A handful of random acquaintances: conference contacts, alumni connections.
-- Uses prime multipliers for deterministic scattering.
-- ============================================================================
INSERT INTO {{zone_name}}.graph.connections
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
    2022 + CAST((src + dst) % 3 AS INT) AS since_year
FROM (
    SELECT
        ((i * 37 + 11) % 100) + 1 AS src,
        ((i * 53 + 29) % 100) + 1 AS dst
    FROM generate_series(1, 50) AS t(i)
) sub
WHERE src != dst;

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.connections;


-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
-- Creates a named graph coupling vertex and edge tables together.
-- Cypher queries reference this by name: USE {{zone_name}}.graph.social_network MATCH ...
-- ============================================================================
CREATE GRAPH IF NOT EXISTS {{zone_name}}.graph.social_network
    VERTEX TABLE {{zone_name}}.graph.employees ID COLUMN id NODE TYPE COLUMN department NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.graph.connections SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN relationship_type
    DIRECTED;


-- ============================================================================
-- VIEW 4: employee_stats — per-employee degree centrality
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.graph.employee_stats AS
SELECT
    e.id,
    e.name,
    e.department,
    e.city,
    e.level,
    COALESCE(out_deg.out_degree, 0) AS out_degree,
    COALESCE(in_deg.in_degree, 0) AS in_degree,
    COALESCE(out_deg.out_degree, 0) + COALESCE(in_deg.in_degree, 0) AS total_degree
FROM {{zone_name}}.graph.employees e
LEFT JOIN (
    SELECT src, COUNT(*) AS out_degree FROM {{zone_name}}.graph.connections GROUP BY src
) out_deg ON e.id = out_deg.src
LEFT JOIN (
    SELECT dst, COUNT(*) AS in_degree FROM {{zone_name}}.graph.connections GROUP BY dst
) in_deg ON e.id = in_deg.dst;


-- ============================================================================
-- VIEW 5: dept_connections — cross-department connection matrix
-- ============================================================================
CREATE OR REPLACE VIEW {{zone_name}}.graph.dept_connections AS
SELECT
    src_e.department AS src_dept,
    dst_e.department AS dst_dept,
    COUNT(*) AS connection_count,
    ROUND(AVG(c.weight), 2) AS avg_weight
FROM {{zone_name}}.graph.connections c
JOIN {{zone_name}}.graph.employees src_e ON c.src = src_e.id
JOIN {{zone_name}}.graph.employees dst_e ON c.dst = dst_e.id
GROUP BY src_e.department, dst_e.department;
