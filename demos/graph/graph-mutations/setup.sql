-- ============================================================================
-- Graph Mutations — Hospital Referral Network Setup
-- ============================================================================
-- Creates a hospital referral network for testing graph DML operations.
--
--   1. physicians — 30 vertex nodes (doctors across 6 specialties, 3 hospitals)
--   2. referrals  — 75 directed edges (consult, transfer, second-opinion, follow-up)
--
-- Edge generation uses three deterministic batches:
--   Batch 1: Intra-hospital referrals (stride 3, ~30 edges)
--   Batch 2: Cross-specialty consults (stride 7, ~30 edges)
--   Batch 3: Emergency transfers (prime scatter, ~15 edges)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.hospital_referrals
    COMMENT 'Hospital referral network — physicians and referral edges across specialties';

-- ============================================================================
-- TABLE 1: physicians — 30 vertex nodes
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.hospital_referrals.physicians (
    id                  BIGINT,
    name                STRING,
    specialty           STRING,
    hospital            STRING,
    years_exp           INT,
    accepting_referrals BOOLEAN
) LOCATION 'graph-mutations/physicians';


INSERT INTO {{zone_name}}.hospital_referrals.physicians
SELECT
    id,
    'Dr. ' || CASE (id % 10)
        WHEN 1 THEN 'Chen'       WHEN 2 THEN 'Patel'     WHEN 3 THEN 'Okafor'
        WHEN 4 THEN 'Martinez'   WHEN 5 THEN 'Kim'       WHEN 6 THEN 'Johansson'
        WHEN 7 THEN 'Nakamura'   WHEN 8 THEN 'Schmidt'   WHEN 9 THEN 'Dubois'
        WHEN 0 THEN 'Rao'
    END || '_' || CAST(id AS VARCHAR) AS name,
    CASE (id % 6)
        WHEN 0 THEN 'Cardiology'   WHEN 1 THEN 'Neurology'
        WHEN 2 THEN 'Orthopedics'  WHEN 3 THEN 'Oncology'
        WHEN 4 THEN 'Pediatrics'   WHEN 5 THEN 'Radiology'
    END AS specialty,
    CASE (id % 3)
        WHEN 0 THEN 'Memorial'  WHEN 1 THEN 'General'  WHEN 2 THEN 'University'
    END AS hospital,
    5 + CAST((id * 3) % 25 AS INT) AS years_exp,
    (id % 8 != 0) AS accepting_referrals
FROM generate_series(1, 30) AS t(id);

-- ============================================================================
-- TABLE 2: referrals — 75 directed edges
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.hospital_referrals.referrals (
    id              BIGINT,
    src             BIGINT,
    dst             BIGINT,
    weight          DOUBLE,
    referral_type   STRING,
    referral_date   STRING,
    status          STRING
) LOCATION 'graph-mutations/referrals';


-- Batch 1: Intra-hospital referrals (stride 3, same hospital) — 30 edges
INSERT INTO {{zone_name}}.hospital_referrals.referrals
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.1 + 0.9 * (((CAST(src * 7 + dst * 13 AS DOUBLE)) * 0.618033988749895) % 1.0), 2) AS weight,
    CASE (CAST(src * 2 + dst * 5 AS BIGINT) % 4)
        WHEN 0 THEN 'consult'  WHEN 1 THEN 'transfer'
        WHEN 2 THEN 'second-opinion'  WHEN 3 THEN 'follow-up'
    END AS referral_type,
    CAST(2024 + (src + dst) % 2 AS VARCHAR) || '-' ||
        LPAD(CAST(1 + (src + dst) % 12 AS VARCHAR), 2, '0') || '-' ||
        LPAD(CAST(1 + (src * dst) % 28 AS VARCHAR), 2, '0') AS referral_date,
    CASE (CAST(src + dst * 2 AS BIGINT) % 3)
        WHEN 0 THEN 'active'  WHEN 1 THEN 'completed'  WHEN 2 THEN 'pending'
    END AS status
FROM (
    SELECT gs AS src, ((gs - 1 + 3) % 30) + 1 AS dst
    FROM generate_series(1, 30) AS t(gs)
) sub
WHERE src != dst
  AND (src % 3) = (dst % 3);

-- Batch 2: Cross-specialty consults (stride 7) — 30 edges
INSERT INTO {{zone_name}}.hospital_referrals.referrals
SELECT
    1000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.1 + 0.9 * (((CAST(src * 7 + dst * 13 AS DOUBLE)) * 0.618033988749895) % 1.0), 2) AS weight,
    'consult' AS referral_type,
    CAST(2024 + (src + dst) % 2 AS VARCHAR) || '-' ||
        LPAD(CAST(1 + (src + dst) % 12 AS VARCHAR), 2, '0') || '-' ||
        LPAD(CAST(1 + (src * dst) % 28 AS VARCHAR), 2, '0') AS referral_date,
    CASE (CAST(src * 3 + dst AS BIGINT) % 3)
        WHEN 0 THEN 'active'  WHEN 1 THEN 'completed'  WHEN 2 THEN 'pending'
    END AS status
FROM (
    SELECT gs AS src, ((gs - 1 + 7) % 30) + 1 AS dst
    FROM generate_series(1, 30) AS t(gs)
) sub
WHERE src != dst
  AND (src % 6) != (dst % 6)
  AND NOT EXISTS (
      SELECT 1 FROM {{zone_name}}.hospital_referrals.referrals r
      WHERE r.src = sub.src AND r.dst = sub.dst
  );

-- Batch 3: Emergency transfers (prime scatter) — 15 edges
INSERT INTO {{zone_name}}.hospital_referrals.referrals
SELECT
    2000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.1 + 0.9 * (((CAST(src * 7 + dst * 13 AS DOUBLE)) * 0.618033988749895) % 1.0), 2) AS weight,
    'transfer' AS referral_type,
    CAST(2024 + (src + dst) % 2 AS VARCHAR) || '-' ||
        LPAD(CAST(1 + (src + dst) % 12 AS VARCHAR), 2, '0') || '-' ||
        LPAD(CAST(1 + (src * dst) % 28 AS VARCHAR), 2, '0') AS referral_date,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'active'  WHEN 1 THEN 'completed'  WHEN 2 THEN 'pending'
    END AS status
FROM (
    SELECT
        ((i * 17 + 3) % 30) + 1 AS src,
        ((i * 23 + 11) % 30) + 1 AS dst
    FROM generate_series(1, 15) AS t(i)
) sub
WHERE src != dst
  AND NOT EXISTS (
      SELECT 1 FROM {{zone_name}}.hospital_referrals.referrals r
      WHERE r.src = sub.src AND r.dst = sub.dst
  );

-- ============================================================================
-- PHYSICAL LAYOUT — Z-ORDER for fast data skipping
-- ============================================================================
-- The data was inserted in id-generation order, which has reasonable locality
-- for `id` but scatters frequent filter columns (specialty, hospital) across
-- files.  Z-ORDER rewrites files so rows with similar values on the ordering
-- keys co-locate, giving Parquet min/max statistics much tighter ranges per
-- file.  This benefits three hot paths:
--
--   1. CSR build from the referrals table — sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups — `id` co-location lets the Parquet reader skip
--      almost every row group for targeted physician lookups.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE p.specialty = 'Cardiology' AND p.hospital = 'General'` skip
--      entire files instead of reading the whole physicians table.
--
-- One-time cost at setup; every subsequent query benefits.  These OPTIMIZE
-- statements also compact small files written by the three-batch edge load.
OPTIMIZE {{zone_name}}.hospital_referrals.physicians
    ZORDER BY (id, specialty, hospital);

OPTIMIZE {{zone_name}}.hospital_referrals.referrals
    ZORDER BY (src, dst);

-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
-- AUTO REFRESH CSR: this demo validates DML visibility (INSERT of a new
-- physician, UPDATE of referral priorities, DELETE of completed referrals)
-- through Cypher MATCH after each mutation. That test requires the CSR
-- to rebuild whenever the backing tables advance their Delta version —
-- exactly what AUTO REFRESH CSR enables. Default (NO AUTO REFRESH) is
-- geared toward large read-heavy graphs and would keep a stale CSR.
CREATE GRAPH IF NOT EXISTS {{zone_name}}.hospital_referrals.hospital_referrals
    VERTEX TABLE {{zone_name}}.hospital_referrals.physicians ID COLUMN id NODE TYPE COLUMN specialty NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.hospital_referrals.referrals SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN referral_type
    DIRECTED
    AUTO REFRESH CSR;

-- ============================================================================
-- WARM CSR CACHE — Pre-build the Compressed Sparse Row topology
-- ============================================================================
-- CREATE GRAPHCSR writes the binary .dcsr file to disk, so the first Cypher
-- query loads in ~200 ms instead of rebuilding from Delta tables (6-14 s for
-- large graphs). Safe to re-run after bulk edge loads to refresh the cache.
CREATE GRAPHCSR {{zone_name}}.hospital_referrals.hospital_referrals;
