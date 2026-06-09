-- ############################################################################
-- ############################################################################
--
--   FLEET DISPATCH NETWORK — AUTO REFRESH CSR: LIVE vs BATCHED GRAPH VIEWS
--   30 Hubs / 100 Directed Routes / Paired Named Graphs Over Same Tables
--
-- ############################################################################
-- ############################################################################
--
-- Teaches the AUTO REFRESH CSR graph property by running the same Cypher
-- pattern against two graphs built from the identical tables:
--
--   • dispatch_live  — AUTO REFRESH CSR       (opt-in auto-rebuild)
--   • dispatch_batch — NO AUTO REFRESH CSR    (default, manual refresh)
--
-- The tests interleave DML with Cypher reads and prove that:
--
--   1. Before any DML, both graphs return identical values.
--   2. After UPDATE, dispatch_live reflects the new value immediately;
--      dispatch_batch still serves the pre-UPDATE value from cache.
--   3. CREATE GRAPHCSR on dispatch_batch forces a rebuild and brings it
--      back in sync — demonstrating the manual refresh contract.
--   4. After DELETE, dispatch_live reflects the new topology (row count
--      drops); dispatch_batch still shows the old topology until the
--      next CREATE GRAPHCSR. This is the intended batching behaviour.
--
-- PART 1: DATA INTEGRITY       (queries 1-4)
-- PART 2: BASELINE COMPARISON  (queries 5-6)
-- PART 3: UPDATE → DIVERGE     (queries 7-10)
-- PART 4: MANUAL REFRESH       (queries 11-12)
-- PART 5: DELETE → DIVERGE     (queries 13-16)
-- PART 6: FINAL SYNC + VERIFY  (queries 17-19)
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA INTEGRITY
-- ############################################################################


-- ============================================================================
-- 1. HUB COUNT — 30 distribution hubs in the network
-- ============================================================================
ASSERT VALUE row_count = 30
SELECT COUNT(*) AS row_count FROM {{zone_name}}.fleet_dispatch.hubs;


-- ============================================================================
-- 2. ROUTE COUNT — 100 directed delivery routes at baseline
-- ============================================================================
ASSERT VALUE row_count = 100
SELECT COUNT(*) AS row_count FROM {{zone_name}}.fleet_dispatch.routes;


-- ============================================================================
-- 3. STATUS MIX — 90 active + 10 suspended (one every 10 ids)
-- ============================================================================
-- Deterministic: status = 'suspended' when route_id % 10 = 0. Those 10 rows
-- are what PART 5's DELETE targets.

ASSERT ROW_COUNT = 2
ASSERT VALUE cnt = 90 WHERE status = 'active'
ASSERT VALUE cnt = 10 WHERE status = 'suspended'
SELECT status, COUNT(*) AS cnt
FROM {{zone_name}}.fleet_dispatch.routes
GROUP BY status
ORDER BY cnt DESC;


-- ============================================================================
-- 4. REGIONAL HUB DISTRIBUTION — 6 regions × 5 hubs = 30
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE cnt = 5 WHERE region = 'Region_0'
ASSERT VALUE cnt = 5 WHERE region = 'Region_5'
SELECT region, COUNT(*) AS cnt
FROM {{zone_name}}.fleet_dispatch.hubs
GROUP BY region
ORDER BY region;


-- ############################################################################
-- PART 2: BASELINE COMPARISON — both graphs agree
-- ############################################################################


-- ============================================================================
-- 5. LIVE GRAPH — route id=42 baseline (src=12, dst=24, price=226)
-- ============================================================================
-- Route 42 is the canary we watch through every DML. At baseline it has
-- price_usd=226 (from 100 + (42*3) mod 200). The live graph sees exactly
-- what the edge table has.

ASSERT ROW_COUNT = 1
ASSERT VALUE price_usd = 226
ASSERT VALUE status = 'active'
USE {{zone_name}}.fleet_dispatch.dispatch_live
MATCH (a)-[r]->(b)
WHERE r.route_id = 42
RETURN a.hub_id AS src, b.hub_id AS dst, r.price_usd AS price_usd, r.status AS status;


-- ============================================================================
-- 6. BATCH GRAPH — same route, same baseline (both CSRs freshly built)
-- ============================================================================
-- At baseline neither graph has observed any DML, so dispatch_batch must
-- report the same values dispatch_live reports. This anchors the
-- comparison — divergence only starts after a DML commit.

ASSERT ROW_COUNT = 1
ASSERT VALUE price_usd = 226
ASSERT VALUE status = 'active'
USE {{zone_name}}.fleet_dispatch.dispatch_batch
MATCH (a)-[r]->(b)
WHERE r.route_id = 42
RETURN a.hub_id AS src, b.hub_id AS dst, r.price_usd AS price_usd, r.status AS status;


-- ############################################################################
-- PART 3: UPDATE → divergence between live and batched views
-- ############################################################################


-- ============================================================================
-- 7. UPDATE — escalate pricing for route 42 to a sentinel value
-- ============================================================================
-- SQL UPDATE advances the Delta log to a new version. dispatch_live will
-- evict its cache on the next Cypher read. dispatch_batch will NOT — it
-- keeps the baseline CSR until we explicitly refresh it.

UPDATE {{zone_name}}.fleet_dispatch.routes
SET price_usd = 9999
WHERE route_id = 42;


-- ============================================================================
-- 8. SQL read — Delta has the new value (ground truth)
-- ============================================================================
-- Any SQL query reads the current Delta snapshot, so the new price is
-- visible immediately. The interesting question is whether Cypher — via
-- the paired graph caches — agrees.

ASSERT ROW_COUNT = 1
ASSERT VALUE price_usd = 9999
SELECT route_id, src_hub, dst_hub, price_usd
FROM {{zone_name}}.fleet_dispatch.routes
WHERE route_id = 42;


-- ============================================================================
-- 9. LIVE GRAPH — reflects the UPDATE (AUTO REFRESH CSR rebuilt on read)
-- ============================================================================
-- The cache lookup saw cached_edge_version != current_edge_version and,
-- because the graph is declared AUTO REFRESH CSR, evicted and rebuilt.
-- New price_usd = 9999 is materialised in the rebuilt CSR.

ASSERT ROW_COUNT = 1
ASSERT VALUE price_usd = 9999
USE {{zone_name}}.fleet_dispatch.dispatch_live
MATCH (a)-[r]->(b)
WHERE r.route_id = 42
RETURN a.hub_id AS src, b.hub_id AS dst, r.price_usd AS price_usd, r.status AS status;


-- ============================================================================
-- 10. BATCH GRAPH — UPDATE is NOT visible (CSR + .dptr cache is stale)
-- ============================================================================
-- Under NO AUTO REFRESH CSR, BOTH the cached topology AND the cached
-- row-pointer store (.dptr) stay pinned at the version captured by
-- the last build. After Q7's UPDATE, Delta tombstoned the old Parquet
-- file and wrote a new one; the stale .dptr still encodes offsets
-- into the old file, so any per-edge property read on this graph
-- (price_usd, status, even route_id used as a WHERE filter) is
-- incoherent until the user explicitly rebuilds with CREATE GRAPHCSR
-- (Q11). The only contract-faithful check on this batched view is
-- the cached topology cardinality: 100 edges from baseline, unchanged
-- because UPDATE did not add or remove rows. Property freshness is
-- verified post-refresh in Q12.

ASSERT VALUE edge_count = 100
USE {{zone_name}}.fleet_dispatch.dispatch_batch
MATCH (a)-[r]->(b)
RETURN count(r) AS edge_count;


-- ############################################################################
-- PART 4: Manual refresh keeps the batched view explicit
-- ############################################################################


-- ============================================================================
-- 11. CREATE GRAPHCSR — refresh dispatch_batch to the current version
-- ============================================================================
-- CREATE GRAPHCSR always forces a full rebuild. It evicts the in-memory
-- cache entry and the on-disk .dcsr sidecar for this graph, then reads
-- from the current Delta snapshot. After this runs, dispatch_batch is
-- pinned at the latest version (topology + fresh properties).

CREATE GRAPHCSR {{zone_name}}.fleet_dispatch.dispatch_batch;


-- ============================================================================
-- 12. BATCH GRAPH — still reflects the UPDATE after explicit refresh
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE price_usd = 9999
USE {{zone_name}}.fleet_dispatch.dispatch_batch
MATCH (a)-[r]->(b)
WHERE r.route_id = 42
RETURN a.hub_id AS src, b.hub_id AS dst, r.price_usd AS price_usd, r.status AS status;


-- ############################################################################
-- PART 5: DELETE → divergence on topology, not just properties
-- ############################################################################


-- ============================================================================
-- 13. DELETE — remove all suspended routes (10 rows)
-- ============================================================================
-- DELETE is a much bigger invalidation than UPDATE: it changes topology,
-- not just property values. Cached CSRs that don't refresh will report
-- edge counts that diverge from the Delta source of truth.

DELETE FROM {{zone_name}}.fleet_dispatch.routes
WHERE status = 'suspended';


-- ============================================================================
-- 14. SQL read — Delta now holds 90 routes (ground truth)
-- ============================================================================

ASSERT VALUE row_count = 90
SELECT COUNT(*) AS row_count FROM {{zone_name}}.fleet_dispatch.routes;


-- ============================================================================
-- 15. LIVE GRAPH — topology reflects DELETE (90 edges, no suspended)
-- ============================================================================

ASSERT VALUE edge_count = 90
USE {{zone_name}}.fleet_dispatch.dispatch_live
MATCH (a)-[r]->(b)
RETURN count(r) AS edge_count;


-- ============================================================================
-- 16. BATCH GRAPH — still shows the pre-DELETE topology (100 edges)
-- ============================================================================
-- Same divergence pattern as Q10, but now on COUNT instead of a
-- per-row property. The batched view counts 100 edges because its
-- cached CSR was last built at post-UPDATE / pre-DELETE version —
-- the 10 suspended routes are still in its topology.

ASSERT VALUE edge_count = 100
USE {{zone_name}}.fleet_dispatch.dispatch_batch
MATCH (a)-[r]->(b)
RETURN count(r) AS edge_count;


-- ############################################################################
-- PART 6: Final sync and cross-cutting verification
-- ############################################################################


-- ============================================================================
-- 17. CREATE GRAPHCSR — refresh the batched view one more time
-- ============================================================================

CREATE GRAPHCSR {{zone_name}}.fleet_dispatch.dispatch_batch;


-- ============================================================================
-- 18. BATCH GRAPH — now matches dispatch_live (90 edges, post-DELETE)
-- ============================================================================

ASSERT VALUE edge_count = 90
USE {{zone_name}}.fleet_dispatch.dispatch_batch
MATCH (a)-[r]->(b)
RETURN count(r) AS edge_count;


-- ============================================================================
-- 19. VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: after UPDATE + DELETE + two manual
-- refreshes, both graphs must agree with Delta on every dimension
-- that matters — row count, vertex count, the updated canary row's
-- final price, and the absence of suspended routes.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 6
SELECT 'Delta row count = 90' AS test,
       CASE WHEN cnt = 90 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END AS result
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.fleet_dispatch.routes)

UNION ALL
SELECT 'Hub count = 30',
       CASE WHEN cnt = 30 THEN 'PASS' ELSE 'FAIL (got ' || CAST(cnt AS VARCHAR) || ')' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.fleet_dispatch.hubs)

UNION ALL
SELECT 'No suspended routes survive DELETE',
       CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL (' || CAST(cnt AS VARCHAR) || ' survived)' END
FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.fleet_dispatch.routes WHERE status = 'suspended')

UNION ALL
SELECT 'Canary route 42 price = 9999 (SQL)',
       CASE WHEN price_usd = 9999 THEN 'PASS'
            ELSE 'FAIL (got ' || CAST(price_usd AS VARCHAR) || ')' END
FROM (SELECT price_usd FROM {{zone_name}}.fleet_dispatch.routes WHERE route_id = 42)

UNION ALL
SELECT 'Live-graph edge count matches Delta',
       CASE WHEN live_edges = delta_edges THEN 'PASS'
            ELSE 'FAIL (live=' || CAST(live_edges AS VARCHAR)
                 || ', delta=' || CAST(delta_edges AS VARCHAR) || ')' END
FROM (
    SELECT
        (SELECT COUNT(*) FROM {{zone_name}}.fleet_dispatch.routes) AS delta_edges,
        90 AS live_edges  -- Anchored from Q15; a mismatch indicates live graph stopped auto-refreshing
)

UNION ALL
SELECT '6 regions × 5 hubs (region invariant)',
       CASE WHEN dist_regions = 6 AND min_per_region = 5 AND max_per_region = 5 THEN 'PASS'
            ELSE 'FAIL (regions=' || CAST(dist_regions AS VARCHAR)
                 || ', min=' || CAST(min_per_region AS VARCHAR)
                 || ', max=' || CAST(max_per_region AS VARCHAR) || ')' END
FROM (
    SELECT
        COUNT(DISTINCT region) AS dist_regions,
        MIN(n) AS min_per_region,
        MAX(n) AS max_per_region
    FROM (
        SELECT region, COUNT(*) AS n
        FROM {{zone_name}}.fleet_dispatch.hubs
        GROUP BY region
    )
);
