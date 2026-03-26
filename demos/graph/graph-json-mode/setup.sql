-- ============================================================================
-- Graph JSON Mode — Setup Script
-- ============================================================================
-- Creates Delta graph tables using the JSON property storage mode.
-- All vertex and edge properties are stored in a single JSON string column —
-- the most flexible mode for schema-free, evolving graph data.
--
--   1. persons_json     — 50 vertex nodes (id + JSON props column)
--   2. friendships_json — ~150 directed edges (src, dst + JSON props column)
--
-- Dataset: Same 50-employee startup as flattened and hybrid modes — 5
-- departments, 4 cities, hierarchical mentorship, bridge employees.
--
-- JSON MODE ADVANTAGES:
--   - Schema-free: add new properties without ALTER TABLE
--   - Extensible: each vertex/edge can have different properties
--   - Ideal for evolving data models and heterogeneous graphs
--   - SIMD-accelerated JSON extraction in Delta Forge
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.graph
    COMMENT 'Graph property storage mode demo tables';


-- ============================================================================
-- TABLE 1: persons_json — 50 vertex nodes (id + JSON properties)
-- ============================================================================
-- Only the vertex ID and label are dedicated columns. All other properties
-- live inside a single JSON string. Use json_get_str/json_get_int to extract.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.persons_json (
    id      BIGINT,
    label   STRING,
    props   STRING
) LOCATION '{{data_path}}/persons_json';

GRANT ADMIN ON TABLE {{zone_name}}.graph.persons_json TO USER {{current_user}};

INSERT INTO {{zone_name}}.graph.persons_json
SELECT
    id,
    -- label = department
    CASE (id % 5)
        WHEN 0 THEN 'Engineering'  WHEN 1 THEN 'Marketing'
        WHEN 2 THEN 'HR'           WHEN 3 THEN 'Finance'
        WHEN 4 THEN 'Sales'
    END AS label,
    -- All properties in JSON
    '{"name": "' ||
    CASE (id % 10)
        WHEN 1 THEN 'Priya'  WHEN 2 THEN 'Marcus'  WHEN 3 THEN 'Sofia'
        WHEN 4 THEN 'James'  WHEN 5 THEN 'Wei'     WHEN 6 THEN 'Elena'
        WHEN 7 THEN 'Raj'    WHEN 8 THEN 'Kenji'   WHEN 9 THEN 'Amara'
        WHEN 0 THEN 'Luca'
    END || '_' || CAST(id AS VARCHAR) ||
    '", "age": ' || CAST(25 + CAST(((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0) * 30.0 AS INT) AS VARCHAR) ||
    ', "department": "' ||
    CASE (id % 5)
        WHEN 0 THEN 'Engineering'  WHEN 1 THEN 'Marketing'
        WHEN 2 THEN 'HR'           WHEN 3 THEN 'Finance'
        WHEN 4 THEN 'Sales'
    END ||
    '", "city": "' ||
    CASE (id % 4)
        WHEN 0 THEN 'NYC'     WHEN 1 THEN 'SF'
        WHEN 2 THEN 'Chicago' WHEN 3 THEN 'London'
    END ||
    '", "title": "' ||
    CASE
        WHEN id % 10 = 0 THEN 'Director'
        WHEN id % 5  = 0 THEN 'Manager'
        WHEN id % 3  = 0 THEN 'Senior Engineer'
        ELSE 'Individual Contributor'
    END ||
    '", "level": "' ||
    CASE
        WHEN id % 10 = 0 THEN 'L5'
        WHEN id % 5  = 0 THEN 'L4'
        WHEN id % 3  = 0 THEN 'L3'
        WHEN id % 2  = 0 THEN 'L2'
        ELSE 'L1'
    END ||
    '", "active": ' ||
    CASE WHEN (id % 7 != 0) THEN 'true' ELSE 'false' END ||
    ', "hire_year": ' || CAST(2015 + (id % 10) AS VARCHAR) ||
    '}' AS props
FROM generate_series(1, 50) AS t(id);

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.persons_json;


-- ============================================================================
-- TABLE 2: friendships_json — ~150 directed edges (src, dst + JSON props)
-- ============================================================================
-- Only src/dst vertex IDs are dedicated columns. All edge properties are
-- stored in a single JSON string column.
--
-- Same 5-batch edge generation as flattened and hybrid modes.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.graph.friendships_json (
    src                BIGINT,
    dst                BIGINT,
    weight             DOUBLE,
    relationship_type  VARCHAR,
    props              STRING
) LOCATION '{{data_path}}/friendships_json';

GRANT ADMIN ON TABLE {{zone_name}}.graph.friendships_json TO USER {{current_user}};


-- Batch 1: Intra-department colleagues (~50 edges)
INSERT INTO {{zone_name}}.graph.friendships_json
SELECT
    src,
    dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 7 + dst * 13 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'colleague' WHEN 1 THEN 'teammate' WHEN 2 THEN 'desk-neighbor'
    END AS relationship_type,
    '{"since_year": ' || CAST(2018 + CAST((src + dst) % 7 AS INT) AS VARCHAR) ||
    ', "frequency": "' ||
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'daily' WHEN 1 THEN 'weekly' WHEN 2 THEN 'daily'
    END ||
    '", "context": "work", "rating": ' ||
    CAST(3 + CAST((src * 3 + dst) % 3 AS INT) AS VARCHAR) || '}' AS props
FROM (
    SELECT gs AS src, ((gs - 1 + 5) % 50) + 1 AS dst
    FROM generate_series(1, 50) AS t(gs)
    UNION ALL
    SELECT gs AS src, ((gs - 1 + 10) % 50) + 1 AS dst
    FROM generate_series(1, 30) AS t(gs)
) sub
WHERE src != dst;


-- Batch 2: City cross-department social (~30 edges)
INSERT INTO {{zone_name}}.graph.friendships_json
SELECT
    src,
    dst,
    ROUND(0.2 + 0.3 * ((CAST(src * 11 + dst * 17 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst * 2 AS BIGINT) % 3)
        WHEN 0 THEN 'city-social' WHEN 1 THEN 'lunch-buddy' WHEN 2 THEN 'gym-partner'
    END AS relationship_type,
    '{"since_year": ' || CAST(2020 + CAST((src + dst) % 5 AS INT) AS VARCHAR) ||
    ', "frequency": "' ||
    CASE (CAST(src + dst AS BIGINT) % 2)
        WHEN 0 THEN 'weekly' WHEN 1 THEN 'monthly'
    END ||
    '", "context": "social", "rating": ' ||
    CAST(2 + CAST((src + dst) % 3 AS INT) AS VARCHAR) || '}' AS props
FROM (
    SELECT gs AS src, ((gs - 1 + 4) % 50) + 1 AS dst
    FROM generate_series(1, 25) AS t(gs)
    UNION ALL
    SELECT gs AS src, ((gs - 1 + 8) % 50) + 1 AS dst
    FROM generate_series(1, 15) AS t(gs)
) sub
WHERE src != dst
  AND (src % 5) != (dst % 5);


-- Batch 3: Hierarchical mentorship (~30 edges)
INSERT INTO {{zone_name}}.graph.friendships_json
SELECT
    src,
    dst,
    ROUND(0.7 + 0.3 * ((CAST(src * 3 + dst * 7 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    'mentor' AS relationship_type,
    '{"since_year": ' ||
    CAST(2019 + CAST((src + dst) % 6 AS INT) AS VARCHAR) ||
    ', "frequency": "weekly", "context": "work", "rating": ' ||
    CAST(4 + CAST((src + dst) % 2 AS INT) AS VARCHAR) || '}' AS props
FROM (
    SELECT
        mentor_id AS src,
        ((mentor_id - 1 + k * 5) % 50) + 1 AS dst
    FROM (
        SELECT m.mentor_id, o.k
        FROM (
            SELECT gs * 10 AS mentor_id FROM generate_series(1, 5) AS t(gs)
            UNION ALL
            SELECT gs * 5 AS mentor_id FROM generate_series(1, 10) AS t(gs)
            WHERE (gs * 5) % 10 != 0
        ) m
        CROSS JOIN (
            SELECT gs AS k FROM generate_series(1, 4) AS t(gs)
        ) o
        WHERE (m.mentor_id % 10 = 0 AND o.k <= 3)
           OR (m.mentor_id % 10 != 0 AND o.k <= 2)
    ) pairs
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 50;


-- Batch 4: Bridge node connections (~20 edges)
INSERT INTO {{zone_name}}.graph.friendships_json
SELECT
    src,
    dst,
    ROUND(0.3 + 0.3 * ((CAST(src * 19 + dst * 23 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'liaison' WHEN 1 THEN 'cross-dept-bridge' WHEN 2 THEN 'inter-team-link'
    END AS relationship_type,
    '{"since_year": ' || CAST(2021 + CAST((src + dst) % 4 AS INT) AS VARCHAR) ||
    ', "frequency": "monthly", "context": "work", "rating": ' ||
    CAST(3 + CAST((src + dst) % 2 AS INT) AS VARCHAR) || '}' AS props
FROM (
    SELECT
        bridge_id AS src,
        ((bridge_id - 1 + offset) % 50) + 1 AS dst
    FROM (
        SELECT 13 AS bridge_id UNION ALL SELECT 26
    ) bridges
    CROSS JOIN (
        SELECT gs AS offset FROM generate_series(1, 12) AS t(gs) WHERE gs % 5 != 0
    ) offsets
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 50;


-- Batch 5: Weak ties (~20 edges)
INSERT INTO {{zone_name}}.graph.friendships_json
SELECT
    src,
    dst,
    ROUND(0.1 + 0.15 * ((CAST(src * 43 + dst * 47 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src * 7 + dst * 3 AS BIGINT) % 4)
        WHEN 0 THEN 'acquaintance' WHEN 1 THEN 'conference-contact'
        WHEN 2 THEN 'alumni' WHEN 3 THEN 'referral'
    END AS relationship_type,
    '{"since_year": ' || CAST(2022 + CAST((src + dst) % 3 AS INT) AS VARCHAR) ||
    ', "frequency": "rarely", "context": "social", "rating": ' ||
    CAST(1 + CAST((src + dst) % 3 AS INT) AS VARCHAR) || '}' AS props
FROM (
    SELECT
        ((i * 17 + 3) % 50) + 1 AS src,
        ((i * 31 + 11) % 50) + 1 AS dst
    FROM generate_series(1, 25) AS t(i)
) sub
WHERE src != dst;

DETECT SCHEMA FOR TABLE {{zone_name}}.graph.friendships_json;


-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
CREATE GRAPH IF NOT EXISTS {{zone_name}}.graph.json_demo
    VERTEX TABLE {{zone_name}}.graph.persons_json ID COLUMN id NODE TYPE COLUMN label
    EDGE TABLE {{zone_name}}.graph.friendships_json SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN relationship_type
    DIRECTED
    VERTEX PROPERTIES JSON COLUMN props
    EDGE PROPERTIES JSON COLUMN props;
