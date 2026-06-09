-- ============================================================================
-- Graph Storage Modes — Cypher Queries
-- ============================================================================
-- Proves that FLATTENED, HYBRID, and JSON storage modes produce identical
-- Cypher results. Part 1 runs the same queries against all three modes.
-- Parts 2-5 exercise the full algorithm suite on the flattened graph.
-- ============================================================================


-- ############################################################################
-- PART 1: STORAGE TRANSPARENCY PROOF
-- ############################################################################
-- The same Cypher query, the same data, three different storage backends.
-- Every VALUE assertion must match across all three modes.


-- ============================================================================
-- 1A. FLATTENED — Meet the team (50 employees)
-- ============================================================================

ASSERT ROW_COUNT = 50
ASSERT VALUE city = 'SF' WHERE name = 'Priya_1'
ASSERT VALUE dept = 'Marketing' WHERE name = 'Priya_1'
ASSERT VALUE age = 43 WHERE name = 'Priya_1'
ASSERT VALUE dept = 'HR' WHERE name = 'Marcus_2'
ASSERT VALUE city = 'Chicago' WHERE name = 'Marcus_2'
ASSERT VALUE age = 32 WHERE name = 'Marcus_2'
ASSERT VALUE dept = 'Engineering' WHERE name = 'Wei_5'
ASSERT VALUE age = 27 WHERE name = 'Wei_5'
USE {{zone_name}}.storage_modes.storage_flat
MATCH (n)
RETURN n.name AS name, n.department AS dept, n.city AS city,
       n.age AS age, n.level AS level
ORDER BY n.name;


-- ============================================================================
-- 1B. HYBRID — Meet the team (same 50 employees)
-- ============================================================================

ASSERT ROW_COUNT = 50
ASSERT VALUE city = 'SF' WHERE name = 'Priya_1'
ASSERT VALUE dept = 'Marketing' WHERE name = 'Priya_1'
ASSERT VALUE age = 43 WHERE name = 'Priya_1'
ASSERT VALUE dept = 'HR' WHERE name = 'Marcus_2'
ASSERT VALUE city = 'Chicago' WHERE name = 'Marcus_2'
ASSERT VALUE age = 32 WHERE name = 'Marcus_2'
ASSERT VALUE dept = 'Engineering' WHERE name = 'Wei_5'
ASSERT VALUE age = 27 WHERE name = 'Wei_5'
USE {{zone_name}}.storage_modes.storage_hybrid
MATCH (n)
RETURN n.name AS name, n.department AS dept, n.city AS city,
       n.age AS age, n.level AS level
ORDER BY n.name;


-- ============================================================================
-- 1C. JSON — Meet the team (same 50 employees)
-- ============================================================================

ASSERT ROW_COUNT = 50
ASSERT VALUE city = 'SF' WHERE name = 'Priya_1'
ASSERT VALUE dept = 'Marketing' WHERE name = 'Priya_1'
ASSERT VALUE age = 43 WHERE name = 'Priya_1'
ASSERT VALUE dept = 'HR' WHERE name = 'Marcus_2'
ASSERT VALUE city = 'Chicago' WHERE name = 'Marcus_2'
ASSERT VALUE age = 32 WHERE name = 'Marcus_2'
ASSERT VALUE dept = 'Engineering' WHERE name = 'Wei_5'
ASSERT VALUE age = 27 WHERE name = 'Wei_5'
USE {{zone_name}}.storage_modes.storage_json
MATCH (n)
RETURN n.name AS name, n.department AS dept, n.city AS city,
       n.age AS age, n.level AS level
ORDER BY n.name;


-- ============================================================================
-- 2A. FLATTENED — Edge count and mentor bond data
-- ============================================================================

ASSERT ROW_COUNT = 189
ASSERT VALUE type = 'mentor' WHERE src_name = 'Luca_50' AND dst_name = 'Luca_10'
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r]->(b)
RETURN a.name AS src_name, b.name AS dst_name,
       r.relationship_type AS type, r.weight AS weight
ORDER BY a.name, b.name;


-- ============================================================================
-- 2B. HYBRID — Edge count and mentor bond data
-- ============================================================================

ASSERT ROW_COUNT = 189
ASSERT VALUE type = 'mentor' WHERE src_name = 'Luca_50' AND dst_name = 'Luca_10'
USE {{zone_name}}.storage_modes.storage_hybrid
MATCH (a)-[r]->(b)
RETURN a.name AS src_name, b.name AS dst_name,
       r.relationship_type AS type, r.weight AS weight
ORDER BY a.name, b.name;


-- ============================================================================
-- 2C. JSON — Edge count and mentor bond data
-- ============================================================================

ASSERT ROW_COUNT = 189
ASSERT VALUE type = 'mentor' WHERE src_name = 'Luca_50' AND dst_name = 'Luca_10'
USE {{zone_name}}.storage_modes.storage_json
MATCH (a)-[r]->(b)
RETURN a.name AS src_name, b.name AS dst_name,
       r.relationship_type AS type, r.weight AS weight
ORDER BY a.name, b.name;


-- ============================================================================
-- 3A. FLATTENED — Department headcount
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 10 WHERE department = 'Engineering'
ASSERT VALUE headcount = 10 WHERE department = 'Marketing'
ASSERT VALUE headcount = 10 WHERE department = 'HR'
ASSERT VALUE headcount = 10 WHERE department = 'Finance'
ASSERT VALUE headcount = 10 WHERE department = 'Sales'
USE {{zone_name}}.storage_modes.storage_flat
MATCH (n)
RETURN n.department AS department, count(n) AS headcount
ORDER BY department;


-- ============================================================================
-- 3B. HYBRID — Department headcount
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 10 WHERE department = 'Engineering'
ASSERT VALUE headcount = 10 WHERE department = 'Marketing'
ASSERT VALUE headcount = 10 WHERE department = 'HR'
ASSERT VALUE headcount = 10 WHERE department = 'Finance'
ASSERT VALUE headcount = 10 WHERE department = 'Sales'
USE {{zone_name}}.storage_modes.storage_hybrid
MATCH (n)
RETURN n.department AS department, count(n) AS headcount
ORDER BY department;


-- ============================================================================
-- 3C. JSON — Department headcount
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 10 WHERE department = 'Engineering'
ASSERT VALUE headcount = 10 WHERE department = 'Marketing'
ASSERT VALUE headcount = 10 WHERE department = 'HR'
ASSERT VALUE headcount = 10 WHERE department = 'Finance'
ASSERT VALUE headcount = 10 WHERE department = 'Sales'
USE {{zone_name}}.storage_modes.storage_json
MATCH (n)
RETURN n.department AS department, count(n) AS headcount
ORDER BY department;


-- ============================================================================
-- 4A. FLATTENED — Connected components
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE size = 50
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 4B. HYBRID — Connected components
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE size = 50
USE {{zone_name}}.storage_modes.storage_hybrid
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 4C. JSON — Connected components
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE size = 50
USE {{zone_name}}.storage_modes.storage_json
CALL algo.connectedComponents()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ############################################################################
-- PART 2: DEEP EXPLORATION (flattened mode)
-- ############################################################################


-- ============================================================================
-- 5. SENIOR STAFF — Active L3+ employees ready to mentor
-- ============================================================================

ASSERT ROW_COUNT = 20
ASSERT VALUE level = 'L5' WHERE name = 'Luca_10'
ASSERT VALUE dept = 'Engineering' WHERE name = 'Luca_10'
ASSERT VALUE level = 'L4' WHERE name = 'Wei_5'
ASSERT VALUE dept = 'Engineering' WHERE name = 'Wei_5'
USE {{zone_name}}.storage_modes.storage_flat
MATCH (n)
WHERE n.active = true AND n.level IN ['L3', 'L4', 'L5']
RETURN n.name AS name, n.department AS dept, n.city AS city,
       n.title AS title, n.level AS level
ORDER BY n.level DESC, n.name;


-- ============================================================================
-- 6. MENTORSHIP MAP — Who is coaching whom?
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.storage_modes.storage_flat
MATCH (mentor)-[r:mentor]->(mentee)
RETURN mentor.name AS mentor, mentor.department AS dept,
       mentee.name AS mentee, mentee.department AS mentee_dept,
       r.weight AS bond_strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 7. STRONGEST BONDS — Backbone relationships (weight > 0.8)
-- ============================================================================

ASSERT ROW_COUNT = 53
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r]->(b)
WHERE r.weight > 0.8
RETURN a.name AS person_a, b.name AS person_b,
       r.relationship_type AS type, r.weight AS strength
ORDER BY r.weight DESC;


-- ============================================================================
-- 8. CROSS-DEPARTMENT BRIDGES — Preventing organizational silos
-- ============================================================================

ASSERT ROW_COUNT = 80
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.name AS src, a.department AS src_dept,
       b.name AS dst, b.department AS dst_dept,
       r.relationship_type AS type
ORDER BY a.department, b.department;


-- ============================================================================
-- 9. DEPARTMENT CONNECTIVITY — Which teams talk to each other?
-- ============================================================================

ASSERT ROW_COUNT = 14
ASSERT VALUE connections = 10 WHERE from_dept = 'HR' AND to_dept = 'Marketing'
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a.department AS from_dept, b.department AS to_dept,
       count(r) AS connections, avg(r.weight) AS avg_strength
ORDER BY connections DESC;


-- ============================================================================
-- 10. RELATIONSHIP TYPE MIX — Organizational health check
-- ============================================================================

ASSERT ROW_COUNT = 11
ASSERT VALUE count = 25 WHERE type = 'mentor'
ASSERT VALUE count = 27 WHERE type = 'teammate'
ASSERT VALUE count = 26 WHERE type = 'colleague'
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r]->(b)
RETURN r.relationship_type AS type, r.context AS context,
       count(r) AS count, avg(r.weight) AS avg_weight
ORDER BY count DESC;


-- ============================================================================
-- 11. FRIENDS OF FRIENDS — 2-hop information flow from person #1
-- ============================================================================

ASSERT ROW_COUNT = 21
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[]->(b)-[]->(c)
WHERE a.id = 1 AND a <> c
RETURN a.name AS source, b.name AS relay, c.name AS reached,
       b.department AS relay_dept, c.department AS reached_dept;


-- ============================================================================
-- 12. REACHABILITY — Who can person #1 reach within 3 hops?
-- ============================================================================

ASSERT ROW_COUNT = 27
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[*1..3]->(b)
WHERE a.id = 1 AND a <> b
RETURN DISTINCT b.name AS reachable, b.department AS dept
ORDER BY b.name;


-- ============================================================================
-- 13. ENGINEERING SUBGRAPH — Internal team collaboration
-- ============================================================================

ASSERT ROW_COUNT = 45
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r]->(b)
WHERE a.department = 'Engineering' AND b.department = 'Engineering'
RETURN a.name AS src, b.name AS dst,
       r.relationship_type AS type, r.weight AS weight
ORDER BY r.weight DESC;


-- ============================================================================
-- 14. RECIPROCAL BONDS — Mutual relationships
-- ============================================================================

ASSERT ROW_COUNT = 2
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r1]->(b)-[r2]->(a)
WHERE a.id < b.id
RETURN a.name AS person_a, b.name AS person_b,
       r1.relationship_type AS a_to_b, r2.relationship_type AS b_to_a,
       r1.weight AS a_to_b_weight, r2.weight AS b_to_a_weight
ORDER BY r1.weight + r2.weight DESC;


-- ############################################################################
-- PART 3: GRAPH ALGORITHMS (flattened mode)
-- ############################################################################


-- ============================================================================
-- 15. PAGERANK — Informal influencers
-- ============================================================================

ASSERT ROW_COUNT = 50
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 16. DEGREE CENTRALITY — Connection counts
-- ============================================================================

ASSERT ROW_COUNT = 50
ASSERT VALUE out_degree = 4 WHERE node_id = 1
ASSERT VALUE in_degree = 2 WHERE node_id = 1
ASSERT VALUE total_degree = 6 WHERE node_id = 1
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC;


-- ============================================================================
-- 17. BETWEENNESS CENTRALITY — Information flow gatekeepers
-- ============================================================================

ASSERT ROW_COUNT = 50
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.betweenness()
YIELD node_id, centrality, rank
RETURN node_id, centrality, rank
ORDER BY centrality DESC;


-- ============================================================================
-- 18. CLOSENESS CENTRALITY — Fastest broadcast reach
-- ============================================================================

ASSERT ROW_COUNT = 50
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.closeness()
YIELD node_id, closeness, rank
RETURN node_id, closeness, rank
ORDER BY closeness DESC;


-- ============================================================================
-- 19. LOUVAIN COMMUNITIES — Natural team detection
-- ============================================================================

ASSERT WARNING ROW_COUNT >= 2
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.louvain({resolution: 1.0})
YIELD node_id, community_id
RETURN community_id, collect(node_id) AS members, count(*) AS size
ORDER BY size DESC;


-- ============================================================================
-- 20. TRIANGLE COUNT — Tight-knit group density
-- ============================================================================

ASSERT ROW_COUNT = 50
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.triangle_count()
YIELD node_id, triangle_count
RETURN node_id, triangle_count
ORDER BY triangle_count DESC;


-- ============================================================================
-- 21. STRONGLY CONNECTED COMPONENTS — Bidirectional reachability
-- ============================================================================

ASSERT ROW_COUNT >= 1
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.scc()
YIELD node_id, component_id
RETURN component_id, count(*) AS size
ORDER BY size DESC;


-- ############################################################################
-- PART 4: PATHFINDING & SIMILARITY (flattened mode)
-- ############################################################################


-- ============================================================================
-- 22. SHORTEST PATH — Route from person #1 to person #42
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE node_id = 1 WHERE step = 0
ASSERT VALUE distance = 0 WHERE step = 0
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.shortestPath({source: 1, target: 42})
YIELD node_id, step, distance
RETURN node_id, step, distance
ORDER BY step;


-- ============================================================================
-- 23. ALL DISTANCES — How far is person #1 from everyone?
-- ============================================================================

ASSERT ROW_COUNT = 49
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.allShortestPaths({source: 1})
YIELD node_id, distance, path
RETURN node_id, distance, path
ORDER BY distance;


-- ============================================================================
-- 24. BFS — News propagation from person #1
-- ============================================================================

ASSERT ROW_COUNT = 8
ASSERT VALUE people_at_distance = 1 WHERE depth = 0
ASSERT VALUE people_at_distance = 4 WHERE depth = 1
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.bfs({source: 1})
YIELD node_id, depth, parent_id
RETURN depth, count(*) AS people_at_distance
ORDER BY depth;


-- ============================================================================
-- 25. DFS — Deep exploration tree from person #1
-- ============================================================================

ASSERT ROW_COUNT = 50
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.dfs({source: 1})
YIELD node_id, discovery_time, finish_time, parent_id
RETURN node_id, discovery_time, finish_time, parent_id
ORDER BY discovery_time;


-- ============================================================================
-- 26. MINIMUM SPANNING TREE — Essential backbone connections
-- ============================================================================

ASSERT ROW_COUNT = 49
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.mst()
YIELD sourceId, targetId, weight
RETURN sourceId, targetId, weight
ORDER BY weight;


-- ============================================================================
-- 27. K-NEAREST NEIGHBORS — Who is most like person #1?
-- ============================================================================

ASSERT ROW_COUNT = 5
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.knn({node: 1, k: 5})
YIELD neighbor_id, similarity, rank
RETURN neighbor_id, similarity, rank
ORDER BY rank;


-- ============================================================================
-- 28. JACCARD SIMILARITY — How alike are two specific employees?
-- ============================================================================

ASSERT ROW_COUNT = 1
USE {{zone_name}}.storage_modes.storage_flat
CALL algo.similarity({node1: 1, node2: 13, metric: 'jaccard'})
YIELD node1Id, node2Id, score
RETURN node1Id, node2Id, score;


-- ############################################################################
-- PART 5: VISUALIZATION
-- ############################################################################


-- ============================================================================
-- 29. FULL COMPANY GRAPH — All 50 people and connections
-- ============================================================================

ASSERT ROW_COUNT = 189
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r]->(b)
RETURN a, r, b;


-- ============================================================================
-- 30. MENTORSHIP HIERARCHY — Coach-to-mentee structure
-- ============================================================================

ASSERT ROW_COUNT = 25
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r:mentor]->(b)
RETURN a, r, b;


-- ============================================================================
-- 31. CROSS-DEPARTMENT BRIDGES ONLY — Silo-prevention network
-- ============================================================================

ASSERT ROW_COUNT = 80
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r]->(b)
WHERE a.department <> b.department
RETURN a, r, b;


-- ############################################################################
-- VERIFY: All Checks
-- ############################################################################
-- Cross-cutting sanity checks across all three storage modes.

-- Verify flattened totals
ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 10 WHERE department = 'Marketing'
ASSERT VALUE headcount = 10 WHERE department = 'Engineering'
USE {{zone_name}}.storage_modes.storage_flat
MATCH (n)
RETURN n.department AS department, count(n) AS headcount
ORDER BY department;

ASSERT ROW_COUNT = 189
USE {{zone_name}}.storage_modes.storage_flat
MATCH (a)-[r]->(b)
RETURN a, r, b;

-- Verify hybrid totals match flattened
ASSERT ROW_COUNT = 50
ASSERT VALUE dept = 'Finance' WHERE name = 'Sofia_3'
ASSERT VALUE city = 'London' WHERE name = 'Sofia_3'
USE {{zone_name}}.storage_modes.storage_hybrid
MATCH (n)
RETURN n.name AS name, n.department AS dept, n.city AS city
ORDER BY n.name;

ASSERT ROW_COUNT = 189
USE {{zone_name}}.storage_modes.storage_hybrid
MATCH (a)-[r]->(b)
RETURN a, r, b;

-- Verify JSON totals match flattened
ASSERT ROW_COUNT = 50
ASSERT VALUE dept = 'Finance' WHERE name = 'Sofia_3'
ASSERT VALUE city = 'London' WHERE name = 'Sofia_3'
USE {{zone_name}}.storage_modes.storage_json
MATCH (n)
RETURN n.name AS name, n.department AS dept, n.city AS city
ORDER BY n.name;

ASSERT ROW_COUNT = 189
USE {{zone_name}}.storage_modes.storage_json
MATCH (a)-[r]->(b)
RETURN a, r, b;
