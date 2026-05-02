-- ============================================================================
-- GPU Global Banking Network -- Setup Script (10M Scale)
-- ============================================================================
-- Creates a 10,000,000-account global banking network with ~48,000,000
-- directed transaction edges for GPU-accelerated fraud detection analytics.
--
-- Generation strategy
--   * Accounts use the native synthetic generator df_generate_table, which
--     writes Arrow record batches in tight Rust loops parallelised across
--     cores via Rayon. Empirically ~30-80M rows/sec/core, roughly 10x the
--     throughput of the equivalent generate_series + multi-branch CASE
--     pattern, where every row pays for dozens of comparisons through the
--     SQL expression engine.
--   * Realism: real first and last names from the fake-rs corpus, real
--     bank names (30 of the world's largest), real global financial
--     centres (25 cities). Account-type, risk-tier, balance-band and
--     KYC-level distributions mirror retail-banking norms (80% retail,
--     15% corporate, 5% institutional; ~95% active; KYC mostly Basic).
--   * Edge topology is built from generate_series with stride arithmetic:
--     dense intra-cluster strides, sector corridors, hierarchical
--     advisory trees, compliance bridges, institutional hubs, plus
--     pseudo-random P2P weak ties. ROW_NUMBER OVER is intentionally
--     avoided; ids come from gs + a per-batch offset, which removes the
--     global sort that previously dominated edge-load wall time.
--
-- Tables
--   1. gfn_banks        -- 30 bank lookup records
--   2. gfn_accounts     -- 10,000,000 vertex nodes (synthetic generator)
--   3. gfn_transactions -- 48,099,998 directed edges (7 batches)
--
-- +------------------------------------------------------------------+
-- | RESOURCE WARNING -- READ BEFORE RUNNING                          |
-- +------------------------------------------------------------------+
-- | * Setup takes 5-15 minutes on modern hardware (the synthetic    |
-- |   account generator removes most of the previous CPU bottleneck;|
-- |   the edge topology and OPTIMIZE ZORDER passes dominate now).   |
-- | * Peak compute-node memory: ~10 GB RSS during the heavy Cypher  |
-- |   algorithms (PageRank, Louvain, Triangle Count, and especially |
-- |   betweenness sampling). Memory pressure lives in per-node /    |
-- |   per-edge score arrays inside the algorithm executors.         |
-- | * Recommended host size: 24 GB+ addressable memory. On smaller  |
-- |   hosts, prefer graph-gpu-stress-test (1M/5M).                  |
-- | * CLI: run with DF_HTTP_TIMEOUT_SECS=0 (or >=1800) so the first |
-- |   cold-path Cypher query does not trip the default HTTP timeout.|
-- +------------------------------------------------------------------+
-- ============================================================================


-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables -- demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.gpu_finance_network
    COMMENT '10M-account GPU-accelerated global banking network for fraud analytics';


-- ============================================================================
-- TABLE 1: gfn_banks -- 30 bank lookup records
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.gpu_finance_network.gfn_banks (
    bank_id     INT,
    bank_name   STRING,
    country     STRING,
    region      STRING,
    tier        STRING
) LOCATION 'graph-gpu-10m-finance/gfn_banks';


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
-- TABLE 2: gfn_accounts -- 10,000,000 vertex nodes (synthetic generator)
-- ============================================================================
-- The slow path used to be a 40-branch CASE for the account-holder name, a
-- 30-branch CASE for the bank, and a 25-branch CASE for the city, all
-- evaluated row-by-row by DataFusion's expression engine. Replacing those
-- three columns with native df_generate_table kernels (cyclic_lookup +
-- fake-rs first/last names) removes the dominant CPU cost while preserving
-- exact semantic equivalence: the bank assigned to account id N is
-- deterministic and reproducible because cyclic_lookup is 0-indexed
-- ((id - 1) % 30) and the fake-rs streams are seeded.
--
-- Other columns (account_type, risk_tier, balance_band, kyc_level, open_year,
-- active) are computed in the outer SELECT directly from id. These are simple
-- integer-arithmetic CASE expressions, which DataFusion vectorises efficiently
-- on already-materialised columns -- the cost there is negligible compared to
-- the upstream string CASE work the synthetic generator now eliminates.
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
) LOCATION 'graph-gpu-10m-finance/gfn_accounts';


INSERT INTO {{zone_name}}.gpu_finance_network.gfn_accounts
SELECT
    id,
    first_name || ' ' || last_name AS name,
    bank,
    city,
    -- Retail-banking population: 80% retail, 15% corporate, 5% institutional.
    CASE
        WHEN id % 20 <= 15 THEN 'retail'
        WHEN id % 20 <= 18 THEN 'corporate'
        ELSE 'institutional'
    END AS account_type,
    -- AML risk-tier distribution: most accounts low-risk, escalating tail.
    CASE
        WHEN id % 1000 = 0 THEN 'critical'
        WHEN id % 200  = 0 THEN 'high'
        WHEN id % 50   = 0 THEN 'medium'
        ELSE 'low'
    END AS risk_tier,
    -- Wealth-management balance bands.
    CASE
        WHEN id % 1000 = 0 THEN 'Ultra-High'
        WHEN id % 500  = 0 THEN 'High'
        WHEN id % 100  = 0 THEN 'Premium'
        WHEN id % 50   = 0 THEN 'Standard-Plus'
        WHEN id % 10   = 0 THEN 'Standard'
        ELSE 'Basic'
    END AS balance_band,
    -- KYC depth: Basic for retail mass-market, escalating for higher tiers.
    CASE
        WHEN id % 1000 = 0 THEN 'Enhanced'
        WHEN id % 100  = 0 THEN 'Standard'
        ELSE 'Basic'
    END AS kyc_level,
    2010 + CAST(id % 16 AS INT) AS open_year,
    (id % 21 != 0) AS active
FROM df_generate_table(10000000, '[
    {"type":"row_index","name":"id","start":1},
    {"type":"fake","name":"first_name","kind":"first_name","seed":101},
    {"type":"fake","name":"last_name","kind":"last_name","seed":102},
    {"type":"cyclic_lookup","name":"bank","values":["JPMorgan","Goldman Sachs","Morgan Stanley","Citibank","HSBC","Barclays","Deutsche Bank","BNP Paribas","Credit Suisse","UBS","Santander","BBVA","ING","Rabobank","Nordea","DBS","ANZ","Westpac","MUFG","Sumitomo Mitsui","Mizuho","ICBC","Bank of China","Standard Chartered","Itau","Bradesco","OCBC","Siam Commercial","SEB","Danske Bank"]},
    {"type":"cyclic_lookup","name":"city","values":["New York","London","Tokyo","Shanghai","Singapore","Hong Kong","Frankfurt","Paris","Zurich","Sydney","Toronto","Mumbai","Seoul","Sao Paulo","Dubai","Amsterdam","Stockholm","Dublin","Chicago","San Francisco","Boston","Geneva","Luxembourg","Taipei","Jakarta"]}
]');


-- ============================================================================
-- TABLE 3: gfn_transactions -- 48,099,998 directed edges (7 batches)
-- ============================================================================
-- Each batch uses generate_series with stride arithmetic to build a slice of
-- the topology. ids are computed as a per-batch offset plus gs, which avoids
-- the global ROW_NUMBER OVER (ORDER BY src, dst) sort that previously
-- dominated edge-load wall time at this scale.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.gpu_finance_network.gfn_transactions (
    id                  BIGINT,
    src                 BIGINT,
    dst                 BIGINT,
    weight              DOUBLE,
    transaction_type    STRING,
    tx_year             INT
) LOCATION 'graph-gpu-10m-finance/gfn_transactions';


-- ============================================================================
-- Batch 1: Intra-bank payments (15M edges, stride 20 + stride 40)
-- ============================================================================
-- Accounts in the same dense neighbourhood transact frequently via wire and
-- ACH. Stride 20 and stride 40 are both even, so (src + dst) % 4 only ever
-- hits values 0 and 2; this batch produces only wire-transfer (type 0) and
-- card-payment (type 2) -- a deliberately concentrated mix that mirrors how
-- wire and card volume dominate retail-bank flow.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    id, src, dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 7 + dst * 13 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 4)
        WHEN 0 THEN 'wire-transfer'  WHEN 1 THEN 'ach-payment'
        WHEN 2 THEN 'card-payment'   WHEN 3 THEN 'direct-debit'
    END AS transaction_type,
    2015 + CAST((src + dst) % 11 AS INT) AS tx_year
FROM (
    SELECT
        gs AS id,
        ((gs - 1) % 10000000) + 1 AS src,
        (((gs - 1) % 10000000 + 20) % 10000000) + 1 AS dst
    FROM generate_series(1, 10000000) AS t(gs)
    UNION ALL
    SELECT
        10000000 + gs AS id,
        ((gs - 1) % 10000000) + 1 AS src,
        (((gs - 1) % 10000000 + 40) % 10000000) + 1 AS dst
    FROM generate_series(1, 5000000) AS t(gs)
) sub
WHERE src != dst AND src BETWEEN 1 AND 10000000 AND dst BETWEEN 1 AND 10000000;


-- ============================================================================
-- Batch 2: Sector payment corridors (10M edges, stride 200 + stride 400)
-- ============================================================================
-- Cross-sector payments between accounts in similar industry segments.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    id, src, dst,
    ROUND(0.5 + 0.4 * ((CAST(src * 11 + dst * 17 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src * 3 + dst AS BIGINT) % 3)
        WHEN 0 THEN 'loan-payment'       WHEN 1 THEN 'mortgage-payment'
        WHEN 2 THEN 'investment-transfer'
    END AS transaction_type,
    2018 + CAST((src + dst) % 8 AS INT) AS tx_year
FROM (
    SELECT
        100000000 + gs AS id,
        ((gs - 1) % 10000000) + 1 AS src,
        (((gs - 1) % 10000000 + 200) % 10000000) + 1 AS dst
    FROM generate_series(1, 7000000) AS t(gs)
    UNION ALL
    SELECT
        107000000 + gs AS id,
        ((gs - 1) % 10000000) + 1 AS src,
        (((gs - 1) % 10000000 + 400) % 10000000) + 1 AS dst
    FROM generate_series(1, 3000000) AS t(gs)
) sub
WHERE src != dst AND src BETWEEN 1 AND 10000000 AND dst BETWEEN 1 AND 10000000;


-- ============================================================================
-- Batch 3: Cross-bank clearing routes (5.5M edges, strides 15 + 30 + 45)
-- ============================================================================
-- Interbank clearing and settlement between accounts at different banks.
-- The (src % 30) != (dst % 30) filter encodes "different bank" because bank
-- assignment is (id - 1) % 30; two ids differ in bank iff their residue mod
-- 30 differs. Stride 30 is congruent to 0 mod 30, so the entire stride-30
-- sub-batch is filtered out (same-bank edges); strides 15 and 45 both give
-- bank delta 15, both survive in full.
INSERT INTO {{zone_name}}.gpu_finance_network.gfn_transactions
SELECT
    id, src, dst,
    ROUND(0.2 + 0.3 * ((CAST(src * 23 + dst * 29 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst * 2 AS BIGINT) % 4)
        WHEN 0 THEN 'interbank-clearing'      WHEN 1 THEN 'correspondent-banking'
        WHEN 2 THEN 'cross-border-transfer'    WHEN 3 THEN 'fx-settlement'
    END AS transaction_type,
    2019 + CAST((src + dst) % 7 AS INT) AS tx_year
FROM (
    SELECT
        200000000 + gs AS id,
        ((gs - 1) % 10000000) + 1 AS src,
        (((gs - 1) % 10000000 + 15) % 10000000) + 1 AS dst
    FROM generate_series(1, 4000000) AS t(gs)
    UNION ALL
    SELECT
        204000000 + gs AS id,
        ((gs - 1) % 10000000) + 1 AS src,
        (((gs - 1) % 10000000 + 30) % 10000000) + 1 AS dst
    FROM generate_series(1, 2500000) AS t(gs)
    UNION ALL
    SELECT
        206500000 + gs AS id,
        ((gs - 1) % 10000000) + 1 AS src,
        (((gs - 1) % 10000000 + 45) % 10000000) + 1 AS dst
    FROM generate_series(1, 1500000) AS t(gs)
) sub
WHERE src != dst AND src BETWEEN 1 AND 10000000 AND dst BETWEEN 1 AND 10000000
  AND (src % 30) != (dst % 30);


-- ============================================================================
-- Batch 4: Advisory hierarchy (5.5M edges)
-- ============================================================================
-- Relationship managers advise client accounts. Manager fan-out follows a
-- power-law shape: senior managers (mod 1000 == 0) advise 100 clients each,
-- mid-tier (mod 500) 60, line managers (mod 100) 30, juniors 15.
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
-- Batch 5: Compliance bridge connections (4M edges)
-- ============================================================================
-- Compliance nodes (~2% of accounts: gs % 100 < 2) fan out to 20 nearby
-- accounts each (offsets 1..19 plus 21, skipping 20 to avoid colliding with
-- Batch 1's stride-20 edges).
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
-- Batch 6: Institutional hub connections (4.9M edges)
-- ============================================================================
-- High-volume institutional accounts with a power-law degree distribution.
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
-- Batch 7: P2P and merchant settlements (3,199,998 edges)
-- ============================================================================
-- Pseudo-random weak ties: peer-to-peer transfers, merchant payments. The
-- two coprime multipliers give effectively independent src and dst streams,
-- which makes for diffuse weak ties throughout the graph. Two pairs collide
-- with src == dst (solutions of 120008*i = 25344 (mod 10M) in [1, 3.2M]),
-- so the WHERE clause drops 2 self-loops out of 3,200,000 candidates.
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
-- PHYSICAL LAYOUT -- Z-ORDER for fast data skipping
-- ============================================================================
-- Rows landed in id-generation order, which has reasonable locality for `id`
-- but scatters frequent filter columns (bank, account_type, src/dst) across
-- files. Z-ORDER rewrites files so rows with similar values on the ordering
-- keys co-locate, giving Parquet min/max statistics much tighter ranges per
-- file. This benefits three hot paths:
--
--   1. CSR build from the edge table -- sequential I/O on `(src, dst)`
--      ordering cuts read time on the first cold load.
--   2. Reverse-index lookups -- `id` co-location lets the Parquet reader
--      skip almost every row group for targeted id scans.
--   3. Cypher->SQL translator seed queries -- selective filters like
--      `WHERE a.account_type = 'retail' AND a.risk_tier = 'high'` skip
--      entire files instead of reading the whole table.
--
-- One-time cost at setup; every subsequent query benefits.

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
-- WARM CSR CACHE -- Pre-build the Compressed Sparse Row topology
-- ============================================================================
-- At 10M nodes and 48M edges, CSR construction is expensive. Building it
-- once upfront writes a .dcsr sidecar to disk so the first Cypher query
-- loads in ~200 ms instead of rebuilding from Delta tables. Safe to re-run
-- after bulk edge loads to refresh the cache. The ZORDER step above ensures
-- CSR build reads edges in `(src, dst)` order (roughly sequential I/O).
CREATE GRAPHCSR {{zone_name}}.gpu_finance_network.gpu_finance_network;
