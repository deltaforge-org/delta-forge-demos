-- ============================================================================
-- Fleet Dispatch Network — AUTO REFRESH CSR: Live vs Batched Graph Views
-- ============================================================================
-- Teaches the AUTO REFRESH CSR graph property through paired graph views
-- over the same vertex/edge tables:
--
--   1. dispatch_live  — declared AUTO REFRESH CSR.  Every new Delta
--                       table version advances invalidates the cached
--                       CSR on the next read, so Cypher queries reflect
--                       post-DML state immediately.
--   2. dispatch_batch — declared NO AUTO REFRESH CSR (the default).
--                       The cached CSR keeps serving the last-built
--                       topology + properties across DML. Users run
--                       CREATE GRAPHCSR explicitly when they're ready
--                       to pay the rebuild cost — batching N writes
--                       into one rebuild.
--
-- Scenario: a logistics provider manages 30 distribution hubs and 100
-- directed delivery routes. Dispatch analysts need live visibility into
-- pricing/status changes for real-time routing, but nightly planners
-- prefer a stable snapshot that only refreshes once per batch window.
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────
-- STEP 1: Zone & Schema
-- ────────────────────────────────────────────────────────────────────
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.fleet_dispatch
    COMMENT 'Fleet dispatch network — hubs, delivery routes, paired graph views for AUTO REFRESH CSR';

-- ────────────────────────────────────────────────────────────────────
-- STEP 2: Vertex table — 30 distribution hubs across 6 regions
-- ────────────────────────────────────────────────────────────────────
-- Deterministic generation: hub_id 1..30, hub_name = 'Hub_NN',
-- region = 'Region_((id-1)/5)' giving 6 regions of 5 hubs each,
-- population = 50000 + (id * 7919) % 450000 (stable per id).

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.fleet_dispatch.hubs (
    hub_id     BIGINT,
    hub_name   VARCHAR,
    region     VARCHAR,
    population BIGINT
) LOCATION 'graph-auto-refresh-csr/hubs';


INSERT INTO {{zone_name}}.fleet_dispatch.hubs
SELECT
    hub_id,
    'Hub_' || LPAD(CAST(hub_id AS VARCHAR), 2, '0') AS hub_name,
    'Region_' || CAST((hub_id - 1) / 5 AS VARCHAR) AS region,
    CAST(50000 + (hub_id * 7919) % 450000 AS BIGINT) AS population
FROM generate_series(1, 30) AS t(hub_id);

-- ────────────────────────────────────────────────────────────────────
-- STEP 3: Edge table — 100 directed delivery routes
-- ────────────────────────────────────────────────────────────────────
-- Each route carries id, src_hub, dst_hub, distance_km, eta_hours,
-- price_usd, status.  Deterministic formulas (see proof.py):
--
--   src         = ((i - 1) mod 30) + 1
--   dst         = ((i*7 - 1) mod 30) + 1    (+1 if collides with src)
--   distance_km = 50  + (i*11) mod 500
--   eta_hours   = 2   + (i mod 24)
--   price_usd   = 100 + (i*3)  mod 200
--   status      = 'suspended' when i mod 10 = 0, else 'active'
--
-- So exactly 10 routes carry 'suspended' (ids 10, 20, 30, …, 100).

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.fleet_dispatch.routes (
    route_id    BIGINT,
    src_hub     BIGINT,
    dst_hub     BIGINT,
    distance_km BIGINT,
    eta_hours   BIGINT,
    price_usd   BIGINT,
    status      VARCHAR
) LOCATION 'graph-auto-refresh-csr/routes';


INSERT INTO {{zone_name}}.fleet_dispatch.routes
SELECT
    route_id,
    CAST(((route_id - 1) % 30) + 1 AS BIGINT) AS src_hub,
    CAST(
        CASE
            WHEN ((route_id * 7 - 1) % 30) + 1 = ((route_id - 1) % 30) + 1
                THEN (((route_id * 7 - 1) % 30) + 1) % 30 + 1
            ELSE ((route_id * 7 - 1) % 30) + 1
        END AS BIGINT
    ) AS dst_hub,
    CAST(50  + (route_id * 11) % 500 AS BIGINT) AS distance_km,
    CAST(2   + (route_id % 24)        AS BIGINT) AS eta_hours,
    CAST(100 + (route_id * 3)  % 200  AS BIGINT) AS price_usd,
    CASE WHEN route_id % 10 = 0 THEN 'suspended' ELSE 'active' END AS status
FROM generate_series(1, 100) AS t(route_id);

-- ────────────────────────────────────────────────────────────────────
-- STEP 4: Paired graphs over the same vertex + edge tables
-- ────────────────────────────────────────────────────────────────────
-- dispatch_live  — opts in to AUTO REFRESH CSR. First Cypher query
-- after a DML commit sees the new version, evicts the cached CSR,
-- and serves the rebuild. Good for ad-hoc analytical queries that
-- need to reflect operational writes in real time.

CREATE GRAPH IF NOT EXISTS {{zone_name}}.fleet_dispatch.dispatch_live
    VERTEX TABLE {{zone_name}}.fleet_dispatch.hubs
        ID COLUMN hub_id
        NODE TYPE COLUMN region
        NODE NAME COLUMN hub_name
    EDGE TABLE {{zone_name}}.fleet_dispatch.routes
        SOURCE COLUMN src_hub
        TARGET COLUMN dst_hub
        WEIGHT COLUMN distance_km
        EDGE TYPE COLUMN status
    DIRECTED
    AUTO REFRESH CSR;

-- dispatch_batch — default behaviour (NO AUTO REFRESH CSR). The cache
-- persists across DML until the user explicitly runs CREATE GRAPHCSR.
-- Good for nightly/batch workloads where a stable snapshot is wanted
-- and rebuild cost should be amortised over many writes.

CREATE GRAPH IF NOT EXISTS {{zone_name}}.fleet_dispatch.dispatch_batch
    VERTEX TABLE {{zone_name}}.fleet_dispatch.hubs
        ID COLUMN hub_id
        NODE TYPE COLUMN region
        NODE NAME COLUMN hub_name
    EDGE TABLE {{zone_name}}.fleet_dispatch.routes
        SOURCE COLUMN src_hub
        TARGET COLUMN dst_hub
        WEIGHT COLUMN distance_km
        EDGE TYPE COLUMN status
    DIRECTED
    NO AUTO REFRESH CSR;

-- ────────────────────────────────────────────────────────────────────
-- STEP 5: Warm both disk CSR caches so the first query path is
-- memory-hit, not disk-read. Also fixes the "baseline" version — any
-- subsequent DML will advance Delta's log while these CSRs remain
-- pinned at the current version until refreshed.
-- ────────────────────────────────────────────────────────────────────
CREATE GRAPHCSR {{zone_name}}.fleet_dispatch.dispatch_live;
CREATE GRAPHCSR {{zone_name}}.fleet_dispatch.dispatch_batch;
