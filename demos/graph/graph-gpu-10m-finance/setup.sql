-- ============================================================================
-- GPU Global Banking Network — Setup Script (10M Scale)
-- ============================================================================
-- Creates a 10,000,000-account global banking network with ~48,000,000
-- directed transaction edges for GPU-accelerated fraud detection analytics.
--
-- Topology features (financial domain):
--   * Dense intra-bank payment clusters (stride-20 arithmetic)
--   * Nested sector-based payment corridors (stride-200)
--   * Cross-bank city clearing routes (stride-15, stride-45)
--   * Hierarchical advisory trees (relationship managers → clients)
--   * Compliance bridge nodes (2% of accounts) connecting banks
--   * High-volume institutional hubs with power-law degree
--   * Weak ties: pseudo-random P2P and merchant settlements
--
-- Tables:
--   1. gfn_banks        — 30 bank lookup records
--   2. gfn_accounts     — 10,000,000 vertex nodes (deterministic generation)
--   3. gfn_transactions — ~48,000,000 directed edges (7 batches, deterministic)
--
-- ┌────────────────────────────────────────────────────────────────────┐
-- │ RESOURCE WARNING — READ BEFORE RUNNING                             │
-- ├────────────────────────────────────────────────────────────────────┤
-- │ * Setup takes 10-30 minutes depending on hardware.                 │
-- │ * Peak compute-node memory: ~10 GB RSS during the heavy Cypher     │
-- │   algorithms (PageRank, Louvain, Triangle Count, and especially    │
-- │   betweenness sampling).  The CSR topology build itself is much    │
-- │   cheaper — memory pressure lives in per-node / per-edge score     │
-- │   arrays and traversal queues inside the algorithm executors.      │
-- │ * Recommended host size: 24 GB+ addressable memory.  On smaller    │
-- │   hosts, prefer graph-gpu-stress-test (1M/5M).                     │
-- │ * CLI:  Run with DF_HTTP_TIMEOUT_SECS=0 (or >=1800) so the first   │
-- │   cold-path Cypher query doesn't trip the default HTTP timeout.    │
-- └────────────────────────────────────────────────────────────────────┘
-- ============================================================================


-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.gpu_finance_network
    COMMENT '10M-account GPU-accelerated global banking network for fraud analytics';


-- ============================================================================
-- TABLE 1: gfn_banks — 30 bank lookup records
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.gpu_finance_network.gfn_banks (
    bank_id     INT,
    bank_name   STRING,
    country     STRING,
    region      STRING,
    tier        STRING
) LOCATION 'gfn_banks';


INSERT INTO {{zone_name}}.gpu_finance_network.gfn_banks VALUES
    (0,  'JPMorgan',           'US',          'Americas',  'Tier-1'),
    (1,  'Goldman Sachs',      'US',          'Americas',  'Tier-1'),
    (2,  'Morgan Stanley',     'US',          'Americas',  'Tier-1'),
    (3,  'Citibank',           'US',          'Americas',  'Tier-1'),
    (4,  'HSBC',               'UK',          'EMEA',      'Tier-1'),
    (5,  'Barclays',           'UK',          'EMEA',      'Tier-1'),
    (6,  'Deutsche Bank',      'Germany',     'EMEA',      'Tier-1'),
    (7,  'BNP Paribas',        'France',      'EMEA',      'Tier-1'),
    (8,  'Credit Suisse',      'Switzerland', 'EMEA',      'Tier-1'),
    (9,  'UBS',                'Switzerland', 'EMEA',      'Tier-1'),
    (10, 'Santander',          'Spain',       'EMEA',      'Tier-2'),
    (11, 'BBVA',               'Spain',       'EMEA',      'Tier-2'),
    (12, 'ING',                'Netherlands', 'EMEA',      'Tier-2'),
    (13, 'Rabobank',           'Netherlands', 'EMEA',      'Tier-2'),
    (14, 'Nordea',             'Finland',     'EMEA',      'Tier-2'),
    (15, 'DBS',                'Singapore',   'APAC',      'Tier-2'),
    (16, 'ANZ',                'Australia',   'APAC',      'Tier-2'),
    (17, 'Westpac',            'Australia',   'APAC',      'Tier-2'),
    (18, 'MUFG',               'Japan',       'APAC',      'Tier-1'),
    (19, 'Sumitomo Mitsui',    'Japan',       'APAC',      'Tier-1'),
    (20, 'Mizuho',             'Japan',       'APAC',      'Tier-2'),
    (21, 'ICBC',               'China',       'APAC',      'Tier-1'),
    (22, 'Bank of China',      'China',       'APAC',      'Tier-1'),
    (23, 'Standard Chartered', 'UK',          'EMEA',      'Tier-2'),
    (24, 'Itau',               'Brazil',      'Americas',  'Tier-2'),
    (25, 'Bradesco',           'Brazil',      'Americas',  'Tier-2'),
    (26, 'OCBC',               'Singapore',   'APAC',      'Tier-2'),
    (27, 'Siam Commercial',    'Thailand',    'APAC',      'Tier-2'),
    (28, 'SEB',                'Sweden',      'EMEA',      'Tier-2'),
    (29, 'Danske Bank',        'Denmark',     'EMEA',      'Tier-2');


-- ============================================================================
-- TABLE 2: gfn_accounts — 10,000,000 vertex nodes
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.gpu_finance_network.gfn_accounts (
    id              BIGINT,
    name            STRING,
    bank            STRING,
    city            STRING,
    account_type    STRING,
    risk_tier       STRING,
    balance_band    STRING,
    kyc_level       STRING,
    open_year       INT,
    active          BOOLEAN
) LOCATION 'gfn_accounts';


INSERT INTO {{zone_name}}.gpu_finance_network.gfn_accounts
SELECT
    id,
    CASE (id % 40)
        WHEN 0  THEN 'Acct_A' WHEN 1  THEN 'Acct_B' WHEN 2  THEN 'Acct_C'
        WHEN 3  THEN 'Acct_D' WHEN 4  THEN 'Acct_E' WHEN 5  THEN 'Acct_F'
        WHEN 6  THEN 'Acct_G' WHEN 7  THEN 'Acct_H' WHEN 8  THEN 'Acct_I'
        WHEN 9  THEN 'Acct_J' WHEN 10 THEN 'Acct_K' WHEN 11 THEN 'Acct_L'
        WHEN 12 THEN 'Acct_M' WHEN 13 THEN 'Acct_N' WHEN 14 THEN 'Acct_O'
        WHEN 15 THEN 'Acct_P' WHEN 16 THEN 'Acct_Q' WHEN 17 THEN 'Acct_R'
        WHEN 18 THEN 'Acct_S' WHEN 19 THEN 'Acct_T' WHEN 20 THEN 'Acct_U'
        WHEN 21 THEN 'Acct_V' WHEN 22 THEN 'Acct_W' WHEN 23 THEN 'Acct_X'
        WHEN 24 THEN 'Acct_Y' WHEN 25 THEN 'Acct_Z' WHEN 26 THEN 'Acct_AA'
        WHEN 27 THEN 'Acct_AB' WHEN 28 THEN 'Acct_AC' WHEN 29 THEN 'Acct_AD'
        WHEN 30 THEN 'Acct_AE' WHEN 31 THEN 'Acct_AF' WHEN 32 THEN 'Acct_AG'
        WHEN 33 THEN 'Acct_AH' WHEN 34 THEN 'Acct_AI' WHEN 35 THEN 'Acct_AJ'
        WHEN 36 THEN 'Acct_AK' WHEN 37 THEN 'Acct_AL' WHEN 38 THEN 'Acct_AM'
        WHEN 39 THEN 'Acct_AN'
    END || '_' || CAST(id AS VARCHAR) AS name,
    CASE (id % 30)
        WHEN 0  THEN 'JPMorgan'           WHEN 1  THEN 'Goldman Sachs'
        WHEN 2  THEN 'Morgan Stanley'     WHEN 3  THEN 'Citibank'
        WHEN 4  THEN 'HSBC'               WHEN 5  THEN 'Barclays'
        WHEN 6  THEN 'Deutsche Bank'      WHEN 7  THEN 'BNP Paribas'
        WHEN 8  THEN 'Credit Suisse'      WHEN 9  THEN 'UBS'
        WHEN 10 THEN 'Santander'          WHEN 11 THEN 'BBVA'
        WHEN 12 THEN 'ING'                WHEN 13 THEN 'Rabobank'
        WHEN 14 THEN 'Nordea'             WHEN 15 THEN 'DBS'
        WHEN 16 THEN 'ANZ'                WHEN 17 THEN 'Westpac'
        WHEN 18 THEN 'MUFG'               WHEN 19 THEN 'Sumitomo Mitsui'
        WHEN 20 THEN 'Mizuho'             WHEN 21 THEN 'ICBC'
        WHEN 22 THEN 'Bank of China'      WHEN 23 THEN 'Standard Chartered'
        WHEN 24 THEN 'Itau'               WHEN 25 THEN 'Bradesco'
        WHEN 26 THEN 'OCBC'               WHEN 27 THEN 'Siam Commercial'
        WHEN 28 THEN 'SEB'                WHEN 29 THEN 'Danske Bank'
    END AS bank,
    CASE (id % 25)
        WHEN 0  THEN 'NYC'        WHEN 1  THEN 'London'
        WHEN 2  THEN 'Tokyo'      WHEN 3  THEN 'Shanghai'
        WHEN 4  THEN 'Singapore'  WHEN 5  THEN 'Hong Kong'
        WHEN 6  THEN 'Frankfurt'  WHEN 7  THEN 'Paris'
        WHEN 8  THEN 'Zurich'     WHEN 9  THEN 'Sydney'
        WHEN 10 THEN 'Toronto'    WHEN 11 THEN 'Mumbai'
        WHEN 12 THEN 'Seoul'      WHEN 13 THEN 'Sao Paulo'
        WHEN 14 THEN 'Dubai'      WHEN 15 THEN 'Amsterdam'
        WHEN 16 THEN 'Stockholm'  WHEN 17 THEN 'Dublin'
        WHEN 18 THEN 'Chicago'    WHEN 19 THEN 'San Francisco'
        WHEN 20 THEN 'Boston'     WHEN 21 THEN 'Geneva'
        WHEN 22 THEN 'Luxembourg' WHEN 23 THEN 'Taipei'
        WHEN 24 THEN 'Jakarta'
    END AS city,
    CASE
        WHEN id % 20 <= 15 THEN 'retail'
        WHEN id % 20 <= 18 THEN 'corporate'
        ELSE 'institutional'
    END AS account_type,
    CASE
        WHEN id % 1000 = 0 THEN 'critical'
        WHEN id % 200  = 0 THEN 'high'
        WHEN id % 50   = 0 THEN 'medium'
        ELSE 'low'
    END AS risk_tier,
    CASE
        WHEN id % 1000 = 0 THEN 'Ultra-High'
        WHEN id % 500  = 0 THEN 'High'
        WHEN id % 100  = 0 THEN 'Premium'
        WHEN id % 50   = 0 THEN 'Standard-Plus'
        WHEN id % 10   = 0 THEN 'Standard'
        ELSE 'Basic'
    END AS balance_band,
    CASE
        WHEN id % 1000 = 0 THEN 'Enhanced'
        WHEN id % 100  = 0 THEN 'Standard'
        ELSE 'Basic'
    END AS kyc_level,
    2010 + CAST(id % 16 AS INT) AS open_year,
    (id % 21 != 0) AS active
FROM generate_series(1, 10000000) AS t(id);


-- ============================================================================
-- TABLE 3: gfn_transactions — ~48,000,000 directed edges (7 batches)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.gpu_finance_network.gfn_transactions (
    id                  BIGINT,
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    transaction_type    STRING,
    tx_year             INT
) LOCATION 'gfn_transactions';


-- ============================================================================
-- Batch 1: Intra-bank payments (~15M edges)
-- ============================================================================
-- Accounts within the same bank transact frequently via wire and ACH.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 7 + dst * 13 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 4)
        WHEN 0 THEN 'wire-transfer'  WHEN 1 THEN 'ach-payment'
        WHEN 2 THEN 'card-payment'   WHEN 3 THEN 'direct-debit'
    END AS transaction_type,
    2015 + CAST((src + dst) % 11 AS INT) AS tx_year
FROM (
    SELECT ((gs - 1) % 10000000) + 1 AS src, (((gs - 1) % 10000000 + 20) % 10000000) + 1 AS dst
    FROM generate_series(1, 10000000) AS t(gs)
    UNION ALL
    SELECT ((gs - 1) % 10000000) + 1 AS src, (((gs - 1) % 10000000 + 40) % 10000000) + 1 AS dst
    FROM generate_series(1, 5000000) AS t(gs)
) sub
WHERE src != dst AND src BETWEEN 1 AND 10000000 AND dst BETWEEN 1 AND 10000000;


-- ============================================================================
-- Batch 2: Sector payment corridors (~10M edges)
-- ============================================================================
-- Cross-sector payments between accounts in similar industry segments.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    100000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.5 + 0.4 * ((CAST(src * 11 + dst * 17 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src * 3 + dst AS BIGINT) % 3)
        WHEN 0 THEN 'loan-payment'       WHEN 1 THEN 'mortgage-payment'
        WHEN 2 THEN 'investment-transfer'
    END AS transaction_type,
    2018 + CAST((src + dst) % 8 AS INT) AS tx_year
FROM (
    SELECT ((gs - 1) % 10000000) + 1 AS src, (((gs - 1) % 10000000 + 200) % 10000000) + 1 AS dst
    FROM generate_series(1, 7000000) AS t(gs)
    UNION ALL
    SELECT ((gs - 1) % 10000000) + 1 AS src, (((gs - 1) % 10000000 + 400) % 10000000) + 1 AS dst
    FROM generate_series(1, 3000000) AS t(gs)
) sub
WHERE src != dst AND src BETWEEN 1 AND 10000000 AND dst BETWEEN 1 AND 10000000;


-- ============================================================================
-- Batch 3: Cross-bank clearing routes (~5.5M edges)
-- ============================================================================
-- Interbank clearing and settlement between accounts at different banks.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    200000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.2 + 0.3 * ((CAST(src * 23 + dst * 29 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst * 2 AS BIGINT) % 4)
        WHEN 0 THEN 'interbank-clearing'      WHEN 1 THEN 'correspondent-banking'
        WHEN 2 THEN 'cross-border-transfer'    WHEN 3 THEN 'fx-settlement'
    END AS transaction_type,
    2019 + CAST((src + dst) % 7 AS INT) AS tx_year
FROM (
    SELECT ((gs - 1) % 10000000) + 1 AS src, (((gs - 1) % 10000000 + 15) % 10000000) + 1 AS dst
    FROM generate_series(1, 4000000) AS t(gs)
    UNION ALL
    SELECT ((gs - 1) % 10000000) + 1 AS src, (((gs - 1) % 10000000 + 30) % 10000000) + 1 AS dst
    FROM generate_series(1, 2500000) AS t(gs)
    UNION ALL
    SELECT ((gs - 1) % 10000000) + 1 AS src, (((gs - 1) % 10000000 + 45) % 10000000) + 1 AS dst
    FROM generate_series(1, 1500000) AS t(gs)
) sub
WHERE src != dst AND src BETWEEN 1 AND 10000000 AND dst BETWEEN 1 AND 10000000
  AND (src % 30) != (dst % 30);


-- ============================================================================
-- Batch 4: Advisory hierarchy (~5.5M edges)
-- ============================================================================
-- Relationship managers advise client accounts in a hierarchical structure.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    300000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 3 + dst * 7 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    'advisory' AS transaction_type,
    2016 + CAST((src + dst) % 10 AS INT) AS tx_year
FROM (
    SELECT manager_id AS src, ((manager_id - 1 + k * 20) % 10000000) + 1 AS dst
    FROM (
        SELECT m.manager_id, o.k
        FROM (SELECT gs * 50 AS manager_id FROM generate_series(1, 200000) AS t(gs)) m
        CROSS JOIN (SELECT gs AS k FROM generate_series(1, 100) AS t(gs)) o
        WHERE (m.manager_id % 1000 = 0 AND o.k <= 100)
           OR (m.manager_id % 1000 != 0 AND m.manager_id % 500 = 0 AND o.k <= 60)
           OR (m.manager_id % 500 != 0 AND m.manager_id % 100 = 0 AND o.k <= 30)
           OR (m.manager_id % 100 != 0 AND o.k <= 15)
    ) pairs
) sub
WHERE src != dst AND dst BETWEEN 1 AND 10000000;


-- ============================================================================
-- Batch 5: Compliance bridge connections (~4M edges)
-- ============================================================================
-- Compliance nodes (2% of accounts) monitor cross-bank activity.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    400000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.3 + 0.3 * ((CAST(src * 19 + dst * 23 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'compliance-referral'  WHEN 1 THEN 'aml-flag'
        WHEN 2 THEN 'regulatory-report'
    END AS transaction_type,
    2017 + CAST((src + dst) % 9 AS INT) AS tx_year
FROM (
    SELECT bridge_id AS src, ((bridge_id - 1 + offset) % 10000000) + 1 AS dst
    FROM (SELECT gs AS bridge_id FROM generate_series(1, 10000000) AS t(gs) WHERE gs % 100 < 2) bridges
    CROSS JOIN (SELECT gs AS offset FROM generate_series(1, 21) AS t(gs) WHERE gs != 20) offsets
) sub
WHERE src != dst AND dst BETWEEN 1 AND 10000000;


-- ============================================================================
-- Batch 6: Institutional hub connections (~4.9M edges)
-- ============================================================================
-- High-volume institutional accounts with power-law degree distribution.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    500000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.4 + 0.4 * ((CAST(src * 31 + dst * 37 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'institutional-transfer'  WHEN 1 THEN 'syndicate-payment'
        WHEN 2 THEN 'treasury-sweep'
    END AS transaction_type,
    2014 + CAST((src + dst) % 12 AS INT) AS tx_year
FROM (
    SELECT hub_id AS src, ((hub_id - 1 + k * 7) % 10000000) + 1 AS dst
    FROM (
        SELECT m.hub_id, o.k
        FROM (SELECT gs * 20 AS hub_id FROM generate_series(1, 500000) AS t(gs)) m
        CROSS JOIN (SELECT gs AS k FROM generate_series(1, 50) AS t(gs)) o
        WHERE (m.hub_id % 1000 = 0 AND o.k <= 50)
           OR (m.hub_id % 1000 != 0 AND m.hub_id % 500 = 0 AND o.k <= 40)
           OR (m.hub_id % 500 != 0 AND m.hub_id % 100 = 0 AND o.k <= 25)
           OR (m.hub_id % 100 != 0 AND o.k <= 5)
    ) pairs
) sub
WHERE src != dst AND dst BETWEEN 1 AND 10000000;


-- ============================================================================
-- Batch 7: P2P and merchant settlements (~3.2M edges)
-- ============================================================================
-- Pseudo-random weak ties: peer-to-peer transfers, merchant payments.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    600000000 + ROW_NUMBER() OVER (ORDER BY src, dst) AS id,
    src, dst,
    ROUND(0.05 + 0.15 * ((CAST(src * 43 + dst * 47 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src * 7 + dst * 3 AS BIGINT) % 4)
        WHEN 0 THEN 'p2p-transfer'          WHEN 1 THEN 'merchant-settlement'
        WHEN 2 THEN 'atm-withdrawal'         WHEN 3 THEN 'pos-purchase'
    END AS transaction_type,
    2022 + CAST((src + dst) % 4 AS INT) AS tx_year
FROM (
    SELECT
        ((i * 104729 + 56891) % 10000000) + 1 AS src,
        ((i * 224737 + 31547) % 10000000) + 1 AS dst
    FROM generate_series(1, 3200000) AS t(i)
) sub
WHERE src != dst;


-- ============================================================================
-- PHYSICAL LAYOUT — Z-ORDER for fast data skipping
-- ============================================================================
-- The data was inserted in id-generation order, which has reasonable locality
-- for `id` but scatters frequent filter columns (bank, account_type, etc.)
-- across files.  Z-ORDER rewrites files so rows with similar values on the
-- ordering keys co-locate, giving Parquet min/max statistics much tighter
-- ranges per file.  This benefits three hot paths:
--
--   1. CSR build from the edge table — sequential I/O on `(src, dst)` ordering
--      cuts read time on the first cold load.
--   2. Reverse-index lookups — `id` co-location lets the Parquet reader skip
--      almost every row group for targeted id scans.
--   3. Cypher→SQL translator seed queries — selective filters like
--      `WHERE a.account_type = 'retail' AND a.risk_tier = 'HIGH'` skip
--      entire files instead of reading the whole table.
--
-- One-time cost at setup; every subsequent query benefits.  These OPTIMIZE
-- statements also compact small files written by the seven-batch edge load.

OPTIMIZE {{zone_name}}.gpu_finance_network.gfn_accounts
    ZORDER BY (id, account_type, risk_tier);

OPTIMIZE {{zone_name}}.gpu_finance_network.gfn_transactions
    ZORDER BY (src, dst);


-- ============================================================================
-- GRAPH DEFINITION
-- ============================================================================
CREATE GRAPH IF NOT EXISTS {{zone_name}}.gpu_finance_network.gpu_finance_network
    VERTEX TABLE {{zone_name}}.gpu_finance_network.gfn_accounts ID COLUMN id NODE TYPE COLUMN bank NODE NAME COLUMN name
    EDGE TABLE {{zone_name}}.gpu_finance_network.gfn_transactions SOURCE COLUMN src TARGET COLUMN dst
    WEIGHT COLUMN weight
    EDGE TYPE COLUMN transaction_type
    DIRECTED;

-- ============================================================================
-- WARM CSR CACHE — Pre-build the Compressed Sparse Row topology
-- ============================================================================
-- At 10M nodes and 48M edges, CSR construction is expensive. Building it once
-- upfront writes a .dcsr sidecar to disk so the first Cypher query loads in
-- ~200 ms instead of rebuilding from Delta tables. Safe to re-run after bulk
-- edge loads to refresh the cache.  The ZORDER step above ensures CSR build
-- reads edges in `(src, dst)` order — roughly sequential I/O.
CREATE GRAPHCSR {{zone_name}}.gpu_finance_network.gpu_finance_network;
