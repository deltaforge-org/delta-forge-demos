-- ############################################################################
-- ############################################################################
--
--   SALES TERRITORY OPTIMIZATION — SQL + CYPHER INTEROPERABILITY
--   40 Customers / 96 Referral Edges / 120 Orders / 8 Sales Reps
--
-- ############################################################################
-- ############################################################################
--
-- Demonstrates Delta Forge's unique ability to mix SQL and Cypher in the
-- same interface, sharing the same Delta tables:
--
--   1. cypher() table function — embed Cypher results inside SQL queries
--   2. INSERT INTO delta_table SELECT FROM cypher() — persist graph analysis
--   3. SQL JOINs on Cypher-populated tables — combine graph + relational data
--   4. CTE mixing — Cypher CTE + SQL CTE in the same query
--
-- PART 1: PURE CYPHER BASELINE (queries 1–2)
-- PART 2: CYPHER() TABLE FUNCTION (query 3)
-- PART 3: INSERT CYPHER RESULTS INTO DELTA TABLES (queries 4–7)
-- PART 4: SQL JOINS ON CYPHER-POPULATED TABLES (queries 8–10)
-- PART 5: MIXED CTE — CYPHER + SQL IN ONE QUERY (query 11)
-- PART 6: VERIFICATION (query 12)
--
-- ############################################################################


-- ############################################################################
-- PART 1: PURE CYPHER BASELINE
-- ############################################################################


-- ============================================================================
-- 1. Customer Network Overview — Verify graph vertices
-- ============================================================================
-- The graph has 40 customer vertices. Each vertex carries region, industry,
-- tier, and annual_contract as properties inherited from the Delta table.
-- Acme_Corp (id=20) should be in region=North, industry=Tech.

ASSERT ROW_COUNT = 40
ASSERT VALUE region = 'North' WHERE name = 'Acme_Corp' AND id = 20
ASSERT VALUE industry = 'Tech' WHERE name = 'Acme_Corp' AND id = 20
ASSERT VALUE tier = 'Enterprise' WHERE name = 'Acme_Corp' AND id = 20
USE {{zone_name}}.customer_network.customer_network
MATCH (n)
RETURN n.id AS id, n.name AS name, n.region AS region,
       n.industry AS industry, n.tier AS tier,
       n.annual_contract AS annual_contract
ORDER BY n.id;


-- ============================================================================
-- 2. Referral Map — Verify graph edges
-- ============================================================================
-- 96 directed referral edges with weight and referral_type properties.
-- First edge: Bolt_Inc (1) → Forge_Inc (5), type=partner, weight=0.6, year=2019

ASSERT ROW_COUNT = 96
ASSERT VALUE referral_type = 'partner' WHERE src_name = 'Bolt_Inc' AND dst_name = 'Forge_Inc' AND edge_id = 1
ASSERT VALUE weight = 0.6 WHERE edge_id = 1
ASSERT VALUE year_established = 2019 WHERE edge_id = 1
USE {{zone_name}}.customer_network.customer_network
MATCH (a)-[r]->(b)
RETURN r.id AS edge_id, a.name AS src_name, b.name AS dst_name,
       r.weight AS weight, r.referral_type AS referral_type,
       r.year_established AS year_established
ORDER BY r.id;


-- ############################################################################
-- PART 2: CYPHER() TABLE FUNCTION
-- ############################################################################


-- ============================================================================
-- 3. Cypher Inside SQL — PageRank via cypher() JOINed with customers table
-- ============================================================================
-- The cypher() table function executes a Cypher query and returns the result
-- as a relational table. Here we run PageRank inside cypher() and JOIN the
-- scores with the customers Delta table in a single SQL query.
-- Every customer gets a positive influence_score from PageRank.

ASSERT ROW_COUNT = 40
-- Non-deterministic: PageRank score magnitudes vary with dampingFactor and
-- floating-point iteration order; only the > 0 invariant is stable.
ASSERT WARNING VALUE influence_score > 0 WHERE name = 'Acme_Corp' AND id = 20
SELECT c.id, c.name, c.region, c.industry, pr.score AS influence_score
FROM cypher('{{zone_name}}.customer_network.customer_network', $$
    CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
    YIELD node_id, score
    RETURN node_id AS customer_id, score
$$) AS (customer_id BIGINT, score DOUBLE) pr
JOIN {{zone_name}}.customer_network.customers c ON pr.customer_id = c.id
ORDER BY pr.score DESC;


-- ############################################################################
-- PART 3: INSERT CYPHER RESULTS INTO DELTA TABLES
-- ############################################################################


-- ============================================================================
-- 4. Persist PageRank — INSERT INTO influence_scores SELECT FROM cypher()
-- ============================================================================
-- Runs PageRank via Cypher and persists all 40 scores directly into the
-- influence_scores Delta table. This is the key interop feature: graph
-- algorithm output flows into a Delta table for subsequent SQL analysis.

INSERT INTO {{zone_name}}.customer_network.influence_scores
SELECT * FROM cypher('{{zone_name}}.customer_network.customer_network', $$
    CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
    YIELD node_id, score, rank
    RETURN node_id AS customer_id, score AS influence_score, rank AS influence_rank
$$) AS (customer_id BIGINT, influence_score DOUBLE, influence_rank BIGINT);


-- ============================================================================
-- 5. Verify Persisted PageRank Scores
-- ============================================================================
-- Plain SQL SELECT on the influence_scores table populated by Cypher.
-- All 40 customers should have a score. The top-ranked customer has rank=1.

ASSERT ROW_COUNT = 40
-- Non-deterministic: the exact rank assigned to Acme_Corp depends on
-- PageRank score ordering, but rank is always in [1, 40].
ASSERT WARNING VALUE influence_rank >= 1 WHERE name = 'Acme_Corp'
ASSERT WARNING VALUE influence_rank <= 40 WHERE name = 'Acme_Corp'
-- Non-deterministic: top-ranked score magnitude varies, but must be > 0.
ASSERT WARNING VALUE influence_score > 0 WHERE influence_rank = 1
SELECT i.customer_id, c.name, c.region, i.influence_score, i.influence_rank
FROM {{zone_name}}.customer_network.influence_scores i
JOIN {{zone_name}}.customer_network.customers c ON i.customer_id = c.id
ORDER BY i.influence_rank;


-- ============================================================================
-- 6. Persist Communities — INSERT INTO community_assignments SELECT FROM cypher()
-- ============================================================================
-- Louvain community detection identifies clusters of densely connected
-- customers. Results are persisted into the community_assignments Delta table.

INSERT INTO {{zone_name}}.customer_network.community_assignments
SELECT * FROM cypher('{{zone_name}}.customer_network.customer_network', $$
    CALL algo.louvain()
    YIELD node_id, community_id
    RETURN node_id AS customer_id, community_id
$$) AS (customer_id BIGINT, community_id BIGINT);


-- ============================================================================
-- 7. Verify Communities
-- ============================================================================
-- All 40 customers should be assigned to a community.
-- Louvain should detect at least 2 distinct communities in this dataset.

ASSERT ROW_COUNT = 1
-- Non-deterministic: Louvain modularity optimization can terminate at
-- different partitions depending on traversal order; the count is bounded
-- by [1, 40] but should be >= 2 for this connected graph.
ASSERT WARNING VALUE community_count >= 2
ASSERT WARNING VALUE community_count <= 40
SELECT COUNT(DISTINCT community_id) AS community_count
FROM {{zone_name}}.customer_network.community_assignments;


-- ############################################################################
-- PART 4: SQL JOINS ON CYPHER-POPULATED TABLES
-- ############################################################################


-- ============================================================================
-- 8. Revenue-Weighted Influence — JOIN influence_scores + orders + customers
-- ============================================================================
-- Combines graph-derived influence scores with relational order revenue.
-- weighted_influence = influence_score * total_revenue shows which customers
-- are both influential in the referral network AND high-revenue.
-- Customer 20 (Acme_Corp) has total_revenue = 60896.

ASSERT ROW_COUNT = 40
ASSERT VALUE total_revenue = 60896.0 WHERE name = 'Acme_Corp' AND customer_id = 20
SELECT c.id AS customer_id, c.name, c.region, i.influence_score,
       SUM(o.amount) AS total_revenue,
       ROUND(i.influence_score * SUM(o.amount), 2) AS weighted_influence
FROM {{zone_name}}.customer_network.influence_scores i
JOIN {{zone_name}}.customer_network.customers c ON i.customer_id = c.id
JOIN {{zone_name}}.customer_network.orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.region, i.influence_score
ORDER BY weighted_influence DESC;


-- ============================================================================
-- 9. Community Revenue Analysis — JOIN community_assignments + orders
-- ============================================================================
-- Aggregates revenue by community. Shows which community clusters drive
-- the most revenue — useful for territory planning.
-- All community members have orders, so total community members = 40.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_members = 40
SELECT SUM(members) AS total_members
FROM (
    SELECT ca.community_id, COUNT(DISTINCT ca.customer_id) AS members,
           SUM(o.amount) AS community_revenue
    FROM {{zone_name}}.customer_network.community_assignments ca
    JOIN {{zone_name}}.customer_network.orders o ON ca.customer_id = o.customer_id
    GROUP BY ca.community_id
) sub;


-- ============================================================================
-- 10. Territory Planning — Full 4-way JOIN with sales reps
-- ============================================================================
-- Joins influence_scores + community_assignments + customers + sales_reps
-- to show each rep's territory influence profile: how many customers,
-- their combined influence, and how many communities they span.
-- 8 reps, each covering one of 4 regions. 40 customers split evenly:
-- 10 per region, so each rep sees customer_count = 10.

ASSERT ROW_COUNT = 8
ASSERT VALUE territory = 'North' WHERE rep_name = 'Alice_Chen'
ASSERT VALUE quota = 200000 WHERE rep_name = 'Alice_Chen'
ASSERT VALUE customer_count = 10 WHERE rep_name = 'Alice_Chen'
SELECT sr.rep_id, sr.rep_name, sr.territory, sr.quota,
       COUNT(DISTINCT c.id) AS customer_count,
       ROUND(SUM(i.influence_score), 4) AS total_influence,
       COUNT(DISTINCT ca.community_id) AS communities_covered
FROM {{zone_name}}.customer_network.sales_reps sr
JOIN {{zone_name}}.customer_network.customers c ON sr.territory = c.region
JOIN {{zone_name}}.customer_network.influence_scores i ON c.id = i.customer_id
JOIN {{zone_name}}.customer_network.community_assignments ca ON c.id = ca.customer_id
GROUP BY sr.rep_id, sr.rep_name, sr.territory, sr.quota
ORDER BY total_influence DESC;


-- ############################################################################
-- PART 5: MIXED CTE — CYPHER + SQL IN ONE QUERY
-- ############################################################################


-- ============================================================================
-- 11. Single-Query Mashup — Degree centrality (Cypher CTE) + Revenue (SQL CTE)
-- ============================================================================
-- This is the pinnacle of SQL/Cypher interoperability: a single query that
-- uses a CTE powered by cypher() for graph degree centrality alongside a
-- standard SQL CTE for revenue aggregation, then JOINs them together.
-- All 40 customers appear. Acme_Corp (id=20) has out-degree 3 (edges 20,56,91)
-- and in-degree 3 (edges 16,51,88), so total_degree = 6.
-- Acme_Corp total_revenue = 12460 + 35299 + 13137 = 60896.

ASSERT ROW_COUNT = 40
ASSERT VALUE total_degree = 6 WHERE name = 'Acme_Corp' AND id = 20
ASSERT VALUE total_revenue = 60896.0 WHERE name = 'Acme_Corp' AND id = 20
WITH hub_scores AS (
    SELECT * FROM cypher('{{zone_name}}.customer_network.customer_network', $$
        CALL algo.degree()
        YIELD node_id, total_degree
        RETURN node_id AS customer_id, total_degree
    $$) AS (customer_id BIGINT, total_degree BIGINT)
),
revenue AS (
    SELECT customer_id, SUM(amount) AS total_revenue
    FROM {{zone_name}}.customer_network.orders
    GROUP BY customer_id
)
SELECT c.id, c.name, c.region, h.total_degree, r.total_revenue
FROM hub_scores h
JOIN {{zone_name}}.customer_network.customers c ON h.customer_id = c.id
JOIN revenue r ON c.id = r.customer_id
ORDER BY h.total_degree DESC, r.total_revenue DESC;


-- ############################################################################
-- PART 6: VERIFICATION
-- ############################################################################


-- ============================================================================
-- 12. Cross-Cutting Verification — All counts in one query
-- ============================================================================
-- 40 customers, 96 referrals, 120 orders, 40 influence scores persisted,
-- 40 community assignments persisted, 8 sales reps.

ASSERT VALUE row_count = 40 WHERE entity = 'customers'
ASSERT VALUE row_count = 96 WHERE entity = 'referrals'
ASSERT VALUE row_count = 120 WHERE entity = 'orders'
ASSERT VALUE row_count = 8 WHERE entity = 'sales_reps'
ASSERT VALUE row_count = 40 WHERE entity = 'influence_scores'
ASSERT VALUE row_count = 40 WHERE entity = 'community_assignments'
ASSERT ROW_COUNT = 6
SELECT 'customers' AS entity, COUNT(*) AS row_count FROM {{zone_name}}.customer_network.customers
UNION ALL SELECT 'referrals', COUNT(*) FROM {{zone_name}}.customer_network.referrals
UNION ALL SELECT 'orders', COUNT(*) FROM {{zone_name}}.customer_network.orders
UNION ALL SELECT 'sales_reps', COUNT(*) FROM {{zone_name}}.customer_network.sales_reps
UNION ALL SELECT 'influence_scores', COUNT(*) FROM {{zone_name}}.customer_network.influence_scores
UNION ALL SELECT 'community_assignments', COUNT(*) FROM {{zone_name}}.customer_network.community_assignments
ORDER BY entity;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting trust anchor covering the key invariants end-to-end:
--  * Customer catalogue intact (40 rows, 4 regions x 10 customers).
--  * Total order revenue matches the sum of the 120 seeded amounts
--    (pass1 + pass2 + pass3 = 2,458,220).
--  * Graph tables share the same customer key cardinality (40 each).
--  * Acme_Corp (id=20) revenue proves SQL↔Cypher tables stay in sync.

ASSERT ROW_COUNT = 1
ASSERT VALUE customer_total = 40
ASSERT VALUE referral_total = 96
ASSERT VALUE order_total = 120
ASSERT VALUE revenue_total = 2458220.0
ASSERT VALUE acme_revenue = 60896.0
ASSERT VALUE influence_total = 40
ASSERT VALUE community_total = 40
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.customer_network.customers)            AS customer_total,
    (SELECT COUNT(*) FROM {{zone_name}}.customer_network.referrals)            AS referral_total,
    (SELECT COUNT(*) FROM {{zone_name}}.customer_network.orders)               AS order_total,
    (SELECT SUM(amount) FROM {{zone_name}}.customer_network.orders)            AS revenue_total,
    (SELECT SUM(amount) FROM {{zone_name}}.customer_network.orders
         WHERE customer_id = 20)                                               AS acme_revenue,
    (SELECT COUNT(*) FROM {{zone_name}}.customer_network.influence_scores)     AS influence_total,
    (SELECT COUNT(*) FROM {{zone_name}}.customer_network.community_assignments) AS community_total;
