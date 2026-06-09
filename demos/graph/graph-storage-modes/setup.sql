-- ============================================================================
-- Graph Storage Modes — Setup Script
-- ============================================================================
-- Creates the SAME 50-employee startup graph three times using different
-- property storage modes:
--
--   FLATTENED — All properties as individual columns (fastest, full pushdown)
--   HYBRID   — Core columns + JSON extras (balanced)
--   JSON     — Single JSON blob per node/edge (most flexible)
--
-- The flattened tables are the canonical source. Hybrid and JSON tables are
-- derived from them via INSERT-SELECT, guaranteeing data equivalence.
--
-- Dataset: 50 employees across 5 departments and 4 cities with ~189 directed
-- connections (intra-department clusters, city bonds, mentorship hierarchy,
-- bridge nodes, and weak ties).
-- ============================================================================

-- ============================================================================
-- ZONE & SCHEMA
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.storage_modes
    COMMENT 'Flattened, hybrid, and JSON graph property storage comparison';

-- ############################################################################
--  MODE 1: FLATTENED — All properties as individual columns
-- ############################################################################

-- ============================================================================
-- persons_flat — 50 vertex nodes (all properties as columns)
-- ============================================================================
-- Deterministic generation using modular arithmetic:
--   Department: id % 5  →  Engineering(0), Marketing(1), HR(2), Finance(3), Sales(4)
--   City:       id % 4  →  NYC(0), SF(1), Chicago(2), London(3)
--   Level:      id%10=0 → L5/Director, id%5=0 → L4/Manager, id%3=0 → L3/Senior, else L2/L1
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.storage_modes.persons_flat (
    id          BIGINT,
    name        STRING,
    age         INT,
    department  STRING,
    city        STRING,
    title       STRING,
    level       STRING,
    active      BOOLEAN
) LOCATION 'graph-storage-modes/persons_flat';


INSERT INTO {{zone_name}}.storage_modes.persons_flat
SELECT
    id,
    CASE (id % 10)
        WHEN 1 THEN 'Priya'  WHEN 2 THEN 'Marcus'  WHEN 3 THEN 'Sofia'
        WHEN 4 THEN 'James'  WHEN 5 THEN 'Wei'     WHEN 6 THEN 'Elena'
        WHEN 7 THEN 'Raj'    WHEN 8 THEN 'Kenji'   WHEN 9 THEN 'Amara'
        WHEN 0 THEN 'Luca'
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

-- ============================================================================
-- edges_flat — ~189 directed edges (all properties as columns)
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.storage_modes.edges_flat (
    id                  BIGINT,
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    since_year          INT,
    frequency           STRING,
    context             STRING,
    rating              INT
) LOCATION 'graph-storage-modes/edges_flat';


-- Batch 1: Intra-department colleagues (~80 edges, stride 5)
INSERT INTO {{zone_name}}.storage_modes.edges_flat
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 7 + dst * 13 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'colleague' WHEN 1 THEN 'teammate' WHEN 2 THEN 'desk-neighbor'
    END AS relationship_type,
    2018 + CAST((src + dst) % 7 AS INT) AS since_year,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'daily' WHEN 1 THEN 'weekly' WHEN 2 THEN 'daily'
    END AS frequency,
    'work' AS context,
    3 + CAST((src * 3 + dst) % 3 AS INT) AS rating
FROM (
    SELECT gs AS src, ((gs - 1 + 5) % 50) + 1 AS dst
    FROM generate_series(1, 50) AS t(gs)
    UNION ALL
    SELECT gs AS src, ((gs - 1 + 10) % 50) + 1 AS dst
    FROM generate_series(1, 30) AS t(gs)
) sub
WHERE src != dst;

-- Batch 2: City cross-department social (~40 edges, stride 4)
INSERT INTO {{zone_name}}.storage_modes.edges_flat
SELECT
    1000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.2 + 0.3 * ((CAST(src * 11 + dst * 17 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst * 2 AS BIGINT) % 3)
        WHEN 0 THEN 'city-social' WHEN 1 THEN 'lunch-buddy' WHEN 2 THEN 'gym-partner'
    END AS relationship_type,
    2020 + CAST((src + dst) % 5 AS INT) AS since_year,
    CASE (CAST(src + dst AS BIGINT) % 2)
        WHEN 0 THEN 'weekly' WHEN 1 THEN 'monthly'
    END AS frequency,
    'social' AS context,
    2 + CAST((src + dst) % 3 AS INT) AS rating
FROM (
    SELECT gs AS src, ((gs - 1 + 4) % 50) + 1 AS dst
    FROM generate_series(1, 25) AS t(gs)
    UNION ALL
    SELECT gs AS src, ((gs - 1 + 8) % 50) + 1 AS dst
    FROM generate_series(1, 15) AS t(gs)
) sub
WHERE src != dst
  AND (src % 5) != (dst % 5);

-- Batch 3: Hierarchical mentorship (~25 edges)
INSERT INTO {{zone_name}}.storage_modes.edges_flat
SELECT
    2000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
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
INSERT INTO {{zone_name}}.storage_modes.edges_flat
SELECT
    3000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.3 + 0.3 * ((CAST(src * 19 + dst * 23 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'liaison' WHEN 1 THEN 'cross-dept-bridge' WHEN 2 THEN 'inter-team-link'
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
        SELECT 13 AS bridge_id UNION ALL SELECT 26
    ) bridges
    CROSS JOIN (
        SELECT gs AS offset FROM generate_series(1, 12) AS t(gs) WHERE gs % 5 != 0
    ) offsets
) sub
WHERE src != dst
  AND dst BETWEEN 1 AND 50;

-- Batch 5: Weak ties — pseudo-random long-range (~24 edges)
INSERT INTO {{zone_name}}.storage_modes.edges_flat
SELECT
    4000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.1 + 0.15 * ((CAST(src * 43 + dst * 47 AS DOUBLE) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src * 7 + dst * 3 AS BIGINT) % 4)
        WHEN 0 THEN 'acquaintance' WHEN 1 THEN 'conference-contact'
        WHEN 2 THEN 'alumni' WHEN 3 THEN 'referral'
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

-- ============================================================================
-- PHYSICAL LAYOUT — Z-ORDER for fast data skipping
-- ============================================================================
-- The data was inserted in id-generation order, which has reasonable locality
-- for `id` but scatters frequent filter columns (department, city) across
-- files.  Z-ORDER rewrites files so rows with similar values on the ordering
-- keys co-locate, giving Parquet min/max statistics much tighter ranges per
-- file.  This benefits three hot paths:
--
--   1. CSR build from the edges table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `id` co-location lets the Parquet reader skip
--      almost every row group for targeted person lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE p.department = 'Engineering' AND p.city = 'SF'` skip entire
--      files instead of reading the whole person table.
--
-- The same three-benefits reasoning applies to the HYBRID and JSON variants
-- below; only the ordering keys differ because JSON/extras blobs are opaque
-- to Parquet statistics.  One-time cost at setup; every subsequent query
-- benefits.
OPTIMIZE {{zone_name}}.storage_modes.persons_flat
    ZORDER BY (id, department, city);

OPTIMIZE {{zone_name}}.storage_modes.edges_flat
    ZORDER BY (src, dst);

-- Graph definition: FLATTENED (default — no PROPERTIES clause needed)
CREATE GRAPH IF NOT EXISTS {{zone_name}}.storage_modes.storage_flat
    VERTEX TABLE {{zone_name}}.storage_modes.persons_flat ID COLUMN id NODE TYPE COLUMN department NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.storage_modes.edges_flat SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN relationship_type
    DIRECTED;

-- Warm CSR cache for FLATTENED graph (see WARM CSR notes at end of file)
CREATE GRAPHCSR {{zone_name}}.storage_modes.storage_flat;


-- ############################################################################
--  MODE 2: HYBRID — Core columns + JSON extras
-- ############################################################################

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.storage_modes.persons_hybrid (
    id      BIGINT,
    name    STRING,
    age     INT,
    label   STRING,
    extras  STRING
) LOCATION 'graph-storage-modes/persons_hybrid';


INSERT INTO {{zone_name}}.storage_modes.persons_hybrid
SELECT
    id, name, age, department AS label,
    '{"department": "' || department ||
    '", "city": "' || city ||
    '", "title": "' || title ||
    '", "level": "' || level ||
    '", "active": ' || CASE WHEN active THEN 'true' ELSE 'false' END ||
    '}' AS extras
FROM {{zone_name}}.storage_modes.persons_flat;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.storage_modes.edges_hybrid (
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    relationship_type   STRING,
    extras              STRING
) LOCATION 'graph-storage-modes/edges_hybrid';


INSERT INTO {{zone_name}}.storage_modes.edges_hybrid
SELECT
    src, dst, weight, relationship_type,
    '{"since_year": ' || CAST(since_year AS VARCHAR) ||
    ', "frequency": "' || frequency ||
    '", "context": "' || context ||
    '", "rating": ' || CAST(rating AS VARCHAR) ||
    '}' AS extras
FROM {{zone_name}}.storage_modes.edges_flat;

-- Physical layout — hybrid tables ZORDER on (id, label) / (src, dst).
-- Only promoted columns can be ordering keys; JSON extras are opaque.
OPTIMIZE {{zone_name}}.storage_modes.persons_hybrid
    ZORDER BY (id, label);

OPTIMIZE {{zone_name}}.storage_modes.edges_hybrid
    ZORDER BY (src, dst);

-- Graph definition: HYBRID
CREATE GRAPH IF NOT EXISTS {{zone_name}}.storage_modes.storage_hybrid
    VERTEX TABLE {{zone_name}}.storage_modes.persons_hybrid ID COLUMN id NODE TYPE COLUMN label NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.storage_modes.edges_hybrid SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN relationship_type
    DIRECTED
    VERTEX PROPERTIES HYBRID COLUMNS (name, age) JSON COLUMN extras
    EDGE PROPERTIES HYBRID COLUMNS (weight, relationship_type) JSON COLUMN extras;

-- Warm CSR cache for HYBRID graph (see WARM CSR notes at end of file)
CREATE GRAPHCSR {{zone_name}}.storage_modes.storage_hybrid;


-- ############################################################################
--  MODE 3: JSON — Single JSON blob per vertex/edge
-- ############################################################################

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.storage_modes.persons_json (
    id      BIGINT,
    label   STRING,
    props   STRING
) LOCATION 'graph-storage-modes/persons_json';


INSERT INTO {{zone_name}}.storage_modes.persons_json
SELECT
    id, department AS label,
    '{"name": "' || name ||
    '", "age": ' || CAST(age AS VARCHAR) ||
    ', "department": "' || department ||
    '", "city": "' || city ||
    '", "title": "' || title ||
    '", "level": "' || level ||
    '", "active": ' || CASE WHEN active THEN 'true' ELSE 'false' END ||
    '}' AS props
FROM {{zone_name}}.storage_modes.persons_flat;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.storage_modes.edges_json (
    src                BIGINT,
    dst                BIGINT,
    weight             DOUBLE,
    relationship_type  VARCHAR,
    props              STRING
) LOCATION 'graph-storage-modes/edges_json';


INSERT INTO {{zone_name}}.storage_modes.edges_json
SELECT
    src, dst, weight, relationship_type,
    '{"since_year": ' || CAST(since_year AS VARCHAR) ||
    ', "frequency": "' || frequency ||
    '", "context": "' || context ||
    '", "rating": ' || CAST(rating AS VARCHAR) ||
    '}' AS props
FROM {{zone_name}}.storage_modes.edges_flat;

-- Physical layout — JSON tables ZORDER on (id, label) / (src, dst).
-- Ordering keys must be top-level columns; props blob is opaque.
OPTIMIZE {{zone_name}}.storage_modes.persons_json
    ZORDER BY (id, label);

OPTIMIZE {{zone_name}}.storage_modes.edges_json
    ZORDER BY (src, dst);

-- Graph definition: JSON
CREATE GRAPH IF NOT EXISTS {{zone_name}}.storage_modes.storage_json
    VERTEX TABLE {{zone_name}}.storage_modes.persons_json ID COLUMN id NODE TYPE COLUMN label
    EDGE TABLE {{zone_name}}.storage_modes.edges_json SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN relationship_type
    DIRECTED
    VERTEX PROPERTIES JSON COLUMN props
    EDGE PROPERTIES JSON COLUMN props;

-- ############################################################################
-- WARM CSR CACHE — All three storage modes
-- ############################################################################
-- CREATE GRAPHCSR pre-builds the Compressed Sparse Row topology and writes
-- it to disk as a .dcsr file. The first Cypher query then loads in ~200 ms
-- instead of rebuilding from Delta tables. Safe to re-run after bulk edge
-- loads to refresh the cache. Each graph has its own cache file keyed to the
-- edge-table version, so FLAT/HYBRID/JSON are warmed independently.

CREATE GRAPHCSR {{zone_name}}.storage_modes.storage_json;
