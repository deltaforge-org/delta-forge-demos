-- ============================================================================
-- Graph Mutations — Hospital Referral Network Queries
-- ============================================================================
-- Tests graph DML operations: INSERT, UPDATE, DELETE on a hospital referral
-- network, then verifies each mutation via Cypher queries. Finishes with
-- graph algorithms on the modified graph.
--
-- Mutation sequence:
--   1. Verify initial state (30 physicians, 75 referrals)
--   2. INSERT new physician (Dr. Rivera_31, Emergency)
--   3. INSERT new referral edge (31 -> 1)
--   4. UPDATE referral priorities (src=1 edges -> weight=1.0, urgent)
--   5. DELETE completed referrals
--   6. Run PageRank and degree centrality on modified graph
-- ============================================================================


-- ============================================================================
-- PART 1: VERIFY INITIAL STATE
-- ============================================================================


-- ============================================================================
-- 1. PHYSICIAN COUNT — Confirm 30 physicians loaded
-- ============================================================================
-- The hospital system has 30 physicians spread across 6 specialties and
-- 3 hospitals. This baseline count is essential before we start mutating.

ASSERT ROW_COUNT = 30
ASSERT VALUE specialty = 'Neurology' WHERE name = 'Dr. Chen_1'
ASSERT VALUE hospital = 'General' WHERE name = 'Dr. Chen_1'
ASSERT VALUE years_exp = 8 WHERE name = 'Dr. Chen_1'
ASSERT VALUE specialty = 'Orthopedics' WHERE name = 'Dr. Patel_2'
ASSERT VALUE hospital = 'University' WHERE name = 'Dr. Patel_2'
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (n)
RETURN n.name AS name, n.specialty AS specialty, n.hospital AS hospital,
       n.years_exp AS years_exp
ORDER BY n.name;


-- ============================================================================
-- 2. REFERRAL COUNT — Confirm 75 referral edges loaded
-- ============================================================================
-- 75 directed referral edges connect physicians across the network. Three
-- batches: intra-hospital (30), cross-specialty consults (30), and
-- emergency transfers (15).

ASSERT ROW_COUNT = 75
ASSERT VALUE referral_type = 'second-opinion' WHERE src = 1 AND dst = 4
ASSERT VALUE referral_type = 'consult' WHERE src = 1 AND dst = 8
ASSERT VALUE weight = 0.52 WHERE src = 1 AND dst = 4
ASSERT VALUE weight = 0.64 WHERE src = 1 AND dst = 8
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (a)-[r]->(b)
RETURN a.id AS src, b.id AS dst, r.referral_type AS referral_type,
       r.weight AS weight, r.status AS status
ORDER BY a.id, b.id;


-- ============================================================================
-- 3. SPECIALTY DISTRIBUTION — 5 physicians per specialty
-- ============================================================================
-- Each of the 6 specialties should have exactly 5 physicians.
-- Verifying uniform distribution before we add an Emergency physician.

ASSERT ROW_COUNT = 6
ASSERT VALUE cnt = 5 WHERE specialty = 'Cardiology'
ASSERT VALUE cnt = 5 WHERE specialty = 'Neurology'
ASSERT VALUE cnt = 5 WHERE specialty = 'Oncology'
ASSERT VALUE cnt = 5 WHERE specialty = 'Orthopedics'
ASSERT VALUE cnt = 5 WHERE specialty = 'Pediatrics'
ASSERT VALUE cnt = 5 WHERE specialty = 'Radiology'
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (n)
RETURN n.specialty AS specialty, count(*) AS cnt
ORDER BY specialty;


-- ============================================================================
-- PART 2: INSERT MUTATIONS
-- ============================================================================


-- ============================================================================
-- 4. INSERT NEW PHYSICIAN — Dr. Rivera joins Emergency Medicine
-- ============================================================================
-- The hospital system hires Dr. Rivera_31, the first Emergency Medicine
-- physician. This tests vertex insertion into an active graph.

INSERT INTO {{zone_name}}.hospital_referrals.physicians
VALUES (31, 'Dr. Rivera_31', 'Emergency', 'Memorial', 15, true);


-- ============================================================================
-- 5. VERIFY INSERT VIA CYPHER — Confirm Dr. Rivera is in the graph
-- ============================================================================
-- After the DML INSERT, the graph should immediately reflect the new node.
-- Cypher pattern matching should find physician 31 with all properties.

ASSERT ROW_COUNT = 1
ASSERT VALUE name = 'Dr. Rivera_31' WHERE id = 31
ASSERT VALUE specialty = 'Emergency' WHERE id = 31
ASSERT VALUE hospital = 'Memorial' WHERE id = 31
ASSERT VALUE years_exp = 15 WHERE id = 31
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (n)
WHERE n.id = 31
RETURN n.id AS id, n.name AS name, n.specialty AS specialty,
       n.hospital AS hospital, n.years_exp AS years_exp;


-- ============================================================================
-- 6. INSERT NEW REFERRAL — Dr. Rivera refers a patient to Dr. Chen
-- ============================================================================
-- Dr. Rivera_31 sends an urgent consult referral to Dr. Chen_1 (Neurology).
-- This tests edge insertion connecting the new vertex to the existing graph.

INSERT INTO {{zone_name}}.hospital_referrals.referrals
VALUES (9999, 31, 1, 0.9, 'consult', '2025-06-15', 'active');


-- ============================================================================
-- 7. VERIFY NEW REFERRAL VIA CYPHER — Confirm the edge exists
-- ============================================================================
-- The new referral edge from 31 to 1 should appear in Cypher traversals
-- with the correct weight, type, and status properties.

ASSERT ROW_COUNT = 1
ASSERT VALUE src_name = 'Dr. Rivera_31' WHERE dst_name = 'Dr. Chen_1'
ASSERT VALUE weight = 0.9 WHERE dst_name = 'Dr. Chen_1'
ASSERT VALUE referral_type = 'consult' WHERE dst_name = 'Dr. Chen_1'
ASSERT VALUE status = 'active' WHERE dst_name = 'Dr. Chen_1'
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (a)-[r]->(b)
WHERE a.id = 31
RETURN a.name AS src_name, b.name AS dst_name, r.weight AS weight,
       r.referral_type AS referral_type, r.status AS status;


-- ============================================================================
-- PART 3: UPDATE MUTATIONS
-- ============================================================================


-- ============================================================================
-- 8. UPDATE REFERRAL PRIORITY — Escalate Dr. Chen's outgoing referrals
-- ============================================================================
-- Dr. Chen_1's referrals are upgraded to urgent priority. This changes
-- weight to 1.0 (maximum priority) and status to 'urgent' for all
-- outgoing edges from physician 1.

UPDATE {{zone_name}}.hospital_referrals.referrals
SET weight = 1.0, status = 'urgent'
WHERE src = 1;


-- ============================================================================
-- 9. VERIFY UPDATE VIA CYPHER — Confirm priority escalation
-- ============================================================================
-- Both outgoing edges from Dr. Chen_1 should now show weight=1.0 and
-- status='urgent'. The edge to physician 4 was a second-opinion and the
-- edge to physician 8 was a consult — both should retain their types.

ASSERT ROW_COUNT = 2
ASSERT VALUE weight = 1.0 WHERE dst_id = 4
ASSERT VALUE status = 'urgent' WHERE dst_id = 4
ASSERT VALUE referral_type = 'second-opinion' WHERE dst_id = 4
ASSERT VALUE weight = 1.0 WHERE dst_id = 8
ASSERT VALUE status = 'urgent' WHERE dst_id = 8
ASSERT VALUE referral_type = 'consult' WHERE dst_id = 8
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (a)-[r]->(b)
WHERE a.id = 1
RETURN a.name AS src_name, b.id AS dst_id, b.name AS dst_name,
       r.weight AS weight, r.referral_type AS referral_type,
       r.status AS status
ORDER BY b.id;


-- ============================================================================
-- PART 4: DELETE MUTATIONS
-- ============================================================================


-- ============================================================================
-- 10. DELETE COMPLETED REFERRALS — Remove discharged patients
-- ============================================================================
-- Completed referrals are no longer active and should be purged from the
-- graph. Note: Dr. Chen's edges were updated to 'urgent' in step 8, so
-- none of his edges will be deleted even if they were originally completed.

DELETE FROM {{zone_name}}.hospital_referrals.referrals
WHERE status = 'completed';


-- ============================================================================
-- 11. VERIFY DELETION — No completed referrals remain
-- ============================================================================
-- After deletion, the graph should have 61 edges (75 original + 1 inserted
-- - 15 completed deleted). Zero edges should have status='completed'.

ASSERT ROW_COUNT = 61
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (a)-[r]->(b)
RETURN a.id AS src, b.id AS dst, r.status AS status
ORDER BY a.id, b.id;


-- ============================================================================
-- 12. VERIFY ZERO COMPLETED — Cross-check deletion completeness
-- ============================================================================
-- Explicitly filter for completed status to confirm none survive.

ASSERT ROW_COUNT = 0
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (a)-[r]->(b)
WHERE r.status = 'completed'
RETURN a.id AS src, b.id AS dst;


-- ============================================================================
-- PART 5: GRAPH ALGORITHMS ON MODIFIED GRAPH
-- ============================================================================


-- ============================================================================
-- 13. PAGERANK AFTER MUTATIONS — Who are the top referral targets?
-- ============================================================================
-- PageRank on the mutated graph reveals which physicians receive the most
-- (and most important) referrals. Dr. Chen's edges now carry maximum weight
-- (1.0), boosting his referral targets' PageRank scores.

ASSERT ROW_COUNT = 31
USE {{zone_name}}.hospital_referrals.hospital_referrals
CALL algo.pageRank({dampingFactor: 0.85, iterations: 20})
YIELD node_id, score, rank
RETURN node_id, score, rank
ORDER BY score DESC;


-- ============================================================================
-- 14. DEGREE CENTRALITY — Verify Dr. Rivera's connectivity
-- ============================================================================
-- Dr. Rivera_31 should have total_degree=1 (one outgoing referral to
-- Dr. Chen_1, zero incoming since she just joined). This confirms the
-- INSERT propagated correctly to the graph's degree computation.

ASSERT ROW_COUNT = 31
ASSERT VALUE out_degree = 1 WHERE node_id = 31
ASSERT VALUE in_degree = 0 WHERE node_id = 31
ASSERT VALUE total_degree = 1 WHERE node_id = 31
USE {{zone_name}}.hospital_referrals.hospital_referrals
CALL algo.degree()
YIELD node_id, in_degree, out_degree, total_degree
RETURN node_id, in_degree, out_degree, total_degree
ORDER BY total_degree DESC;


-- ============================================================================
-- 15. VERIFY: All Checks — Final cross-cutting assertions
-- ============================================================================
-- After all mutations: 31 physicians, 61 referral edges, 0 completed,
-- Dr. Rivera_31 exists with Emergency specialty, Dr. Chen_1's outgoing
-- edges all have weight=1.0 and status='urgent'.

ASSERT ROW_COUNT = 31
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (n)
RETURN n.id AS id;

ASSERT ROW_COUNT = 61
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (a)-[r]->(b)
RETURN a.id AS src, b.id AS dst;

ASSERT ROW_COUNT = 0
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (a)-[r]->(b)
WHERE r.status = 'completed'
RETURN a.id AS src;

ASSERT ROW_COUNT = 2
ASSERT VALUE weight = 1.0 WHERE src = 1 AND dst = 4
ASSERT VALUE weight = 1.0 WHERE src = 1 AND dst = 8
ASSERT VALUE status = 'urgent' WHERE src = 1 AND dst = 4
ASSERT VALUE status = 'urgent' WHERE src = 1 AND dst = 8
USE {{zone_name}}.hospital_referrals.hospital_referrals
MATCH (a)-[r]->(b)
WHERE a.id = 1
RETURN a.id AS src, b.id AS dst, r.weight AS weight, r.status AS status
ORDER BY b.id;
