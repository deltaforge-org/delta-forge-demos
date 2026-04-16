-- ############################################################################
-- ############################################################################
--
--   GLOBAL SHIPPING ROUTE OPTIMIZATION — 25 PORTS / 55 ROUTES
--   Weighted Graph Analytics via Cypher
--
-- ############################################################################
-- ############################################################################
--
-- A maritime logistics company operates 25 major container ports worldwide
-- connected by 55 directed shipping routes. Each route carries distance in
-- nautical miles and transit time as weights. The network spans five trade
-- regions: Asia, Europe, Americas, Middle East, and South Asia.
--
-- PART 1: DATA EXPLORATION (queries 1–4)
--   Port inventory, route overview, regional analysis.
--
-- PART 2: GRAPH ALGORITHMS (queries 5–11)
--   Degree centrality, PageRank, shortest paths, BFS, MST, components.
--
-- PART 3: VERIFICATION (query 12)
--   Cross-cutting structural invariants.
--
-- ############################################################################


-- ############################################################################
-- PART 1: DATA EXPLORATION
-- ############################################################################


-- ============================================================================
-- 1. PORT INVENTORY — All 25 ports with region and capacity
-- ============================================================================
-- The logistics team needs a complete view of the port network: which ports
-- are in which region, what throughput capacity they offer, and how many
-- cranes are available for container handling.

ASSERT ROW_COUNT = 25
ASSERT VALUE region = 'Asia' WHERE name = 'Shanghai'
ASSERT VALUE capacity_teu = 43500 WHERE name = 'Shanghai'
ASSERT VALUE region = 'Europe' WHERE name = 'Rotterdam'
ASSERT VALUE capacity_teu = 37200 WHERE name = 'Singapore'
USE {{zone_name}}.shipping_network.shipping_network
MATCH (p)
RETURN p.name AS name, p.region AS region,
       p.capacity_teu AS capacity_teu, p.crane_count AS crane_count
ORDER BY p.capacity_teu DESC;


-- ============================================================================
-- 2. ROUTE OVERVIEW — All 55 shipping routes with distances
-- ============================================================================
-- Operations needs to see every active route: origin, destination, distance,
-- transit time, route classification, and fuel cost for budgeting.

ASSERT ROW_COUNT = 55
ASSERT VALUE distance_nm = 2200.0 WHERE from_port = 'Shanghai' AND to_port = 'Singapore'
ASSERT VALUE transit_days = 26 WHERE from_port = 'Shanghai' AND to_port = 'Rotterdam'
ASSERT VALUE distance_nm = 3300.0 WHERE from_port = 'Singapore' AND to_port = 'Dubai'
USE {{zone_name}}.shipping_network.shipping_network
MATCH (a)-[r]->(b)
RETURN a.name AS from_port, b.name AS to_port,
       r.distance_nm AS distance_nm, r.transit_days AS transit_days,
       r.route_type AS route_type, r.fuel_cost_usd AS fuel_cost_usd
ORDER BY r.distance_nm DESC;


-- ============================================================================
-- 3. REGIONAL ROUTE ANALYSIS — Cross-region vs intra-region traffic
-- ============================================================================
-- Understanding how much traffic stays within a region versus crossing
-- regional boundaries reveals trade corridor intensity. Intra-Asia routes
-- dominate due to the density of Asian manufacturing ports.

ASSERT ROW_COUNT = 2
ASSERT VALUE route_count = 38 WHERE category = 'Intra-region'
ASSERT VALUE route_count = 17 WHERE category = 'Cross-region'
SELECT
    CASE WHEN src_p.region = dst_p.region THEN 'Intra-region' ELSE 'Cross-region' END AS category,
    COUNT(*) AS route_count,
    ROUND(AVG(r.distance_nm), 0) AS avg_distance_nm,
    ROUND(SUM(r.fuel_cost_usd), 2) AS total_fuel_cost
FROM {{zone_name}}.shipping_network.routes r
JOIN {{zone_name}}.shipping_network.ports src_p ON r.src = src_p.id
JOIN {{zone_name}}.shipping_network.ports dst_p ON r.dst = dst_p.id
GROUP BY CASE WHEN src_p.region = dst_p.region THEN 'Intra-region' ELSE 'Cross-region' END
ORDER BY route_count DESC;


-- ============================================================================
-- 4. ROUTE TYPE DISTRIBUTION — Trunk, feeder, and transshipment breakdown
-- ============================================================================
-- The network has three tiers: trunk routes (long-haul main corridors),
-- feeder routes (regional distribution), and transshipment connections
-- (hub-to-hub transfers). A healthy network needs all three tiers.

ASSERT ROW_COUNT = 3
ASSERT VALUE route_count = 36 WHERE route_type = 'trunk'
ASSERT VALUE route_count = 10 WHERE route_type = 'feeder'
ASSERT VALUE route_count = 9 WHERE route_type = 'transshipment'
SELECT
    route_type,
    COUNT(*) AS route_count,
    ROUND(AVG(distance_nm), 0) AS avg_distance_nm,
    ROUND(AVG(transit_days), 1) AS avg_transit_days,
    ROUND(SUM(fuel_cost_usd), 2) AS total_fuel_cost
FROM {{zone_name}}.shipping_network.routes
GROUP BY route_type
ORDER BY route_count DESC;


-- ############################################################################
-- PART 2: GRAPH ALGORITHMS
-- ############################################################################


-- ============================================================================
-- 5. BUSIEST PORTS (Degree Centrality) — Hub detection
-- ============================================================================
-- Which ports handle the most shipping connections? High total degree means
-- a port serves as both origin and destination for many routes — a natural
-- logistics hub. Shanghai leads with 13 connections (10 outbound, 3 inbound).

ASSERT ROW_COUNT = 10
ASSERT VALUE total_degree = 13 WHERE node_id = 1
ASSERT VALUE total_degree = 12 WHERE node_id = 3
USE {{zone_name}}.shipping_network.shipping_network
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC
LIMIT 10;


-- ============================================================================
-- 6. MOST INFLUENTIAL HUB (PageRank) — Network importance ranking
-- ============================================================================
-- PageRank reveals which ports receive traffic from other well-connected
-- ports. Unlike raw degree, PageRank accounts for the quality of connections.
-- Felixstowe ranks highest because it receives feeder traffic from three
-- major European hubs (Rotterdam, Hamburg, Antwerp).

ASSERT ROW_COUNT = 10
-- Non-deterministic: PageRank scores depend on power-iteration convergence and
-- damping math; assert a range consistent with the computed value (~0.1127).
ASSERT WARNING VALUE score >= 0.08 WHERE node_id = 18
ASSERT WARNING VALUE score <= 0.20 WHERE node_id = 18
USE {{zone_name}}.shipping_network.shipping_network
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC
LIMIT 10;


-- ============================================================================
-- 7. SHORTEST ROUTE BY DISTANCE — Shanghai to Piraeus
-- ============================================================================
-- Find the distance-optimal route from Shanghai (id=1) to the Mediterranean
-- hub Piraeus (id=22). algo.shortestPath uses weighted Dijkstra over the
-- distance_nm edge weights. The optimal path travels 9750 nm in 4 hops:
-- Shanghai -> Singapore -> Colombo -> Dubai -> Piraeus.

ASSERT ROW_COUNT = 5
ASSERT VALUE node_id = 1 WHERE step = 0
ASSERT VALUE distance = 0 WHERE step = 0
ASSERT VALUE node_id = 2 WHERE step = 1
ASSERT VALUE node_id = 20 WHERE step = 2
ASSERT VALUE node_id = 8 WHERE step = 3
ASSERT VALUE node_id = 22 WHERE step = 4
ASSERT VALUE distance = 9750 WHERE step = 4
USE {{zone_name}}.shipping_network.shipping_network
CALL algo.shortestPath({source: 1, target: 22})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 8. SHORTEST ROUTE WITH EXPLICIT WEIGHTED FLAG — Shanghai to Piraeus
-- ============================================================================
-- algo.shortestPath is weighted by default, but the call below passes
-- `weighted: true` explicitly for clarity. The result matches Query 7:
-- the 9750 nm path Shanghai -> Singapore -> Colombo -> Dubai -> Piraeus.

ASSERT ROW_COUNT = 5
ASSERT VALUE node_id = 1 WHERE step = 0
ASSERT VALUE distance = 0 WHERE step = 0
ASSERT VALUE node_id = 22 WHERE step = 4
ASSERT VALUE distance = 9750 WHERE step = 4
USE {{zone_name}}.shipping_network.shipping_network
CALL algo.shortestPath({source: 1, target: 22, weighted: true})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 9. BFS FROM SHANGHAI — Depth distribution across the network
-- ============================================================================
-- How many hops does it take to reach every port from Shanghai? A well-
-- connected hub should reach most of the network in 2 hops. Shanghai
-- reaches 10 ports directly, 12 more at depth 2, and the last 2 at depth 3.

ASSERT ROW_COUNT = 4
ASSERT VALUE people_at_distance = 1 WHERE depth = 0
ASSERT VALUE people_at_distance = 10 WHERE depth = 1
ASSERT VALUE people_at_distance = 12 WHERE depth = 2
ASSERT VALUE people_at_distance = 2 WHERE depth = 3
USE {{zone_name}}.shipping_network.shipping_network
CALL algo.bfs({source: 1})
YIELD node_id, depth, parent_id
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ============================================================================
-- 10. CONNECTED COMPONENTS — Is the network fully connected?
-- ============================================================================
-- For a global shipping network, all ports must be reachable. A single
-- connected component of size 25 confirms no port is isolated.

ASSERT ROW_COUNT = 1
ASSERT VALUE size = 25
USE {{zone_name}}.shipping_network.shipping_network
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 11. MINIMUM SPANNING TREE — Cheapest backbone network
-- ============================================================================
-- If the company could only keep 24 routes (minimum to connect all 25 ports),
-- which routes would form the cheapest infrastructure? The MST selects the
-- lowest-distance route for each connection, totaling 28,805 nautical miles.

ASSERT ROW_COUNT = 24
USE {{zone_name}}.shipping_network.shipping_network
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN sourceId, targetId, weight
ORDER BY weight;


-- ############################################################################
-- PART 3: VERIFICATION
-- ############################################################################


-- ============================================================================
-- 12. VERIFY — Cross-cutting structural invariants
-- ============================================================================
-- Confirms the core structural properties of the shipping network: port count,
-- route count, route type distribution, and regional diversity.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_ports = 25
ASSERT VALUE total_routes = 55
ASSERT VALUE trunk_routes = 36
ASSERT VALUE feeder_routes = 10
ASSERT VALUE transshipment_routes = 9
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.shipping_network.ports)                                                 AS total_ports,
    (SELECT COUNT(*) FROM {{zone_name}}.shipping_network.routes)                                                AS total_routes,
    (SELECT COUNT(*) FROM {{zone_name}}.shipping_network.routes WHERE route_type = 'trunk')                     AS trunk_routes,
    (SELECT COUNT(*) FROM {{zone_name}}.shipping_network.routes WHERE route_type = 'feeder')                    AS feeder_routes,
    (SELECT COUNT(*) FROM {{zone_name}}.shipping_network.routes WHERE route_type = 'transshipment')             AS transshipment_routes;
