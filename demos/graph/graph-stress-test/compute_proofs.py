#!/usr/bin/env python3
"""
Independent proof-value computation for graph-stress-test demo.
Uses DuckDB to replicate the same deterministic data generation as setup.sql,
then computes all expected assertion values.
"""
import duckdb
import json
import time

db = duckdb.connect(":memory:")
t0 = time.time()

print("=== Generating 1M people ===")
db.execute("""
CREATE TABLE st_people AS
SELECT
    id,
    CASE (id % 40)
        WHEN 0  THEN 'Priya'    WHEN 1  THEN 'Marcus'   WHEN 2  THEN 'Sofia'
        WHEN 3  THEN 'James'    WHEN 4  THEN 'Wei'      WHEN 5  THEN 'Elena'
        WHEN 6  THEN 'Raj'      WHEN 7  THEN 'Kenji'    WHEN 8  THEN 'Amara'
        WHEN 9  THEN 'Luca'     WHEN 10 THEN 'Fatima'   WHEN 11 THEN 'Carlos'
        WHEN 12 THEN 'Yuki'     WHEN 13 THEN 'Nadia'    WHEN 14 THEN 'Omar'
        WHEN 15 THEN 'Ingrid'   WHEN 16 THEN 'Dmitri'   WHEN 17 THEN 'Aisha'
        WHEN 18 THEN 'Tomas'    WHEN 19 THEN 'Mei'      WHEN 20 THEN 'Henrik'
        WHEN 21 THEN 'Zara'     WHEN 22 THEN 'Mateo'    WHEN 23 THEN 'Suki'
        WHEN 24 THEN 'Andre'    WHEN 25 THEN 'Leila'    WHEN 26 THEN 'Chen'
        WHEN 27 THEN 'Rosa'     WHEN 28 THEN 'Vikram'   WHEN 29 THEN 'Astrid'
        WHEN 30 THEN 'Felix'    WHEN 31 THEN 'Naomi'    WHEN 32 THEN 'Pavel'
        WHEN 33 THEN 'Lucia'    WHEN 34 THEN 'Tariq'    WHEN 35 THEN 'Elin'
        WHEN 36 THEN 'Kofi'     WHEN 37 THEN 'Maren'    WHEN 38 THEN 'Dante'
        WHEN 39 THEN 'Isla'
    END || '_' || CAST(id AS VARCHAR) AS name,
    22 + CAST(FLOOR(((CAST(id AS DOUBLE) * 0.618033988749895) % 1.0) * 38.0) AS INT) AS age,
    CASE (id % 20)
        WHEN 0  THEN 'Engineering'      WHEN 1  THEN 'Marketing'
        WHEN 2  THEN 'HR'               WHEN 3  THEN 'Finance'
        WHEN 4  THEN 'Sales'            WHEN 5  THEN 'Operations'
        WHEN 6  THEN 'Legal'            WHEN 7  THEN 'Product'
        WHEN 8  THEN 'Data Science'     WHEN 9  THEN 'DevOps'
        WHEN 10 THEN 'Security'         WHEN 11 THEN 'Customer Support'
        WHEN 12 THEN 'Research'         WHEN 13 THEN 'Design'
        WHEN 14 THEN 'QA'              WHEN 15 THEN 'Platform'
        WHEN 16 THEN 'Infrastructure'   WHEN 17 THEN 'Analytics'
        WHEN 18 THEN 'Mobile'           WHEN 19 THEN 'AI/ML'
    END AS department,
    CASE (id % 15)
        WHEN 0  THEN 'NYC'         WHEN 1  THEN 'SF'
        WHEN 2  THEN 'Chicago'     WHEN 3  THEN 'London'
        WHEN 4  THEN 'Berlin'      WHEN 5  THEN 'Tokyo'
        WHEN 6  THEN 'Sydney'      WHEN 7  THEN 'Toronto'
        WHEN 8  THEN 'Singapore'   WHEN 9  THEN 'Dublin'
        WHEN 10 THEN 'Seattle'     WHEN 11 THEN 'Austin'
        WHEN 12 THEN 'Amsterdam'   WHEN 13 THEN 'Mumbai'
        WHEN 14 THEN 'Paris'
    END AS city,
    'Team_' || CAST((id % 200) + 1 AS VARCHAR) AS project_team,
    CASE
        WHEN id % 1000 = 0 THEN 'VP'
        WHEN id % 500  = 0 THEN 'Director'
        WHEN id % 100  = 0 THEN 'Senior Manager'
        WHEN id % 50   = 0 THEN 'Manager'
        WHEN id % 20   = 0 THEN 'Senior Engineer'
        WHEN id % 5    = 0 THEN 'Engineer'
        ELSE 'Associate'
    END AS title,
    2010 + CAST(id % 16 AS INT) AS hire_year,
    CASE
        WHEN id % 1000 = 0 THEN 'L8'
        WHEN id % 500  = 0 THEN 'L7'
        WHEN id % 100  = 0 THEN 'L6'
        WHEN id % 50   = 0 THEN 'L5'
        WHEN id % 20   = 0 THEN 'L4'
        WHEN id % 5    = 0 THEN 'L3'
        WHEN id % 3    = 0 THEN 'L2'
        ELSE 'L1'
    END AS level,
    CASE
        WHEN id % 1000 = 0 THEN 'Executive'
        WHEN id % 500  = 0 THEN 'Band-5'
        WHEN id % 100  = 0 THEN 'Band-4'
        WHEN id % 50   = 0 THEN 'Band-3'
        WHEN id % 20   = 0 THEN 'Band-2'
        ELSE 'Band-1'
    END AS salary_band,
    (id % 21 != 0) AS active
FROM generate_series(1, 1000000) AS t(id)
""")
print(f"  People: {db.execute('SELECT count(*) FROM st_people').fetchone()[0]}")

# ======================================================================
# Generate all 7 edge batches — same logic as setup.sql
# ======================================================================
print("=== Generating ~5M edges (7 batches) ===")

# Batch 1: Intra-department neighborhood
db.execute("""
CREATE TABLE st_edges AS
SELECT
    ROW_NUMBER() OVER (ORDER BY src, dst) AS id, src, dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 7 + dst * 13 AS DOUBLE) * 0.618033988749895) % 1.0), 3) AS weight,
    CASE (CAST(src + dst AS BIGINT) % 4)
        WHEN 0 THEN 'colleague' WHEN 1 THEN 'desk-neighbor'
        WHEN 2 THEN 'teammate'  WHEN 3 THEN 'collaborator'
    END AS relationship_type,
    2015 + CAST((src + dst) % 11 AS INT) AS since_year
FROM (
    SELECT ((gs - 1) % 1000000) + 1 AS src, (((gs - 1) % 1000000 + 20) % 1000000) + 1 AS dst
    FROM generate_series(1, 1000000) AS t(gs)
    UNION ALL
    SELECT ((gs - 1) % 1000000) + 1 AS src, (((gs - 1) % 1000000 + 40) % 1000000) + 1 AS dst
    FROM generate_series(1, 500000) AS t(gs)
) sub WHERE src != dst AND src BETWEEN 1 AND 1000000 AND dst BETWEEN 1 AND 1000000
""")
b1 = db.execute("SELECT count(*) FROM st_edges").fetchone()[0]
print(f"  Batch 1 (dept neighborhood): {b1}")

# Batch 2: Intra-team project connections
db.execute("""
INSERT INTO st_edges
SELECT 10000000 + ROW_NUMBER() OVER (ORDER BY src, dst), src, dst,
    ROUND(0.5 + 0.4 * ((CAST(src * 11 + dst * 17 AS DOUBLE) * 0.618033988749895) % 1.0), 3),
    CASE (CAST(src * 3 + dst AS BIGINT) % 3)
        WHEN 0 THEN 'project-mate' WHEN 1 THEN 'sprint-partner' WHEN 2 THEN 'code-reviewer'
    END, 2018 + CAST((src + dst) % 8 AS INT)
FROM (
    SELECT ((gs-1)%1000000)+1 AS src, (((gs-1)%1000000+200)%1000000)+1 AS dst FROM generate_series(1,700000) AS t(gs)
    UNION ALL
    SELECT ((gs-1)%1000000)+1 AS src, (((gs-1)%1000000+400)%1000000)+1 AS dst FROM generate_series(1,300000) AS t(gs)
) sub WHERE src!=dst AND src BETWEEN 1 AND 1000000 AND dst BETWEEN 1 AND 1000000
""")
b2 = db.execute("SELECT count(*) FROM st_edges").fetchone()[0] - b1
print(f"  Batch 2 (team connections): {b2}")

# Batch 3: City cross-department social
prev = db.execute("SELECT count(*) FROM st_edges").fetchone()[0]
db.execute("""
INSERT INTO st_edges
SELECT 20000000 + ROW_NUMBER() OVER (ORDER BY src, dst), src, dst,
    ROUND(0.2 + 0.3 * ((CAST(src * 23 + dst * 29 AS DOUBLE) * 0.618033988749895) % 1.0), 3),
    CASE (CAST(src + dst * 2 AS BIGINT) % 4)
        WHEN 0 THEN 'city-social' WHEN 1 THEN 'lunch-buddy' WHEN 2 THEN 'commute-buddy' WHEN 3 THEN 'gym-partner'
    END, 2019 + CAST((src + dst) % 7 AS INT)
FROM (
    SELECT ((gs-1)%1000000)+1 AS src, (((gs-1)%1000000+15)%1000000)+1 AS dst FROM generate_series(1,400000) AS t(gs)
    UNION ALL
    SELECT ((gs-1)%1000000)+1 AS src, (((gs-1)%1000000+30)%1000000)+1 AS dst FROM generate_series(1,250000) AS t(gs)
    UNION ALL
    SELECT ((gs-1)%1000000)+1 AS src, (((gs-1)%1000000+45)%1000000)+1 AS dst FROM generate_series(1,150000) AS t(gs)
) sub WHERE src!=dst AND src BETWEEN 1 AND 1000000 AND dst BETWEEN 1 AND 1000000
    AND (src % 20) != (dst % 20)
""")
b3 = db.execute("SELECT count(*) FROM st_edges").fetchone()[0] - prev
print(f"  Batch 3 (city social): {b3}")

# Batch 4: Hierarchical mentorship
prev = db.execute("SELECT count(*) FROM st_edges").fetchone()[0]
db.execute("""
INSERT INTO st_edges
SELECT 30000000 + ROW_NUMBER() OVER (ORDER BY src, dst), src, dst,
    ROUND(0.6 + 0.4 * ((CAST(src * 3 + dst * 7 AS DOUBLE) * 0.618033988749895) % 1.0), 3),
    'mentor', 2016 + CAST((src + dst) % 10 AS INT)
FROM (
    SELECT mentor_id AS src, ((mentor_id - 1 + k * 20) % 1000000) + 1 AS dst
    FROM (
        SELECT m.mentor_id, o.k
        FROM (SELECT gs * 50 AS mentor_id FROM generate_series(1, 20000) AS t(gs)) m
        CROSS JOIN (SELECT gs AS k FROM generate_series(1, 100) AS t(gs)) o
        WHERE (m.mentor_id % 1000 = 0 AND o.k <= 100)
           OR (m.mentor_id % 1000 != 0 AND m.mentor_id % 500 = 0 AND o.k <= 60)
           OR (m.mentor_id % 500 != 0 AND m.mentor_id % 100 = 0 AND o.k <= 30)
           OR (m.mentor_id % 100 != 0 AND o.k <= 15)
    ) pairs
) sub WHERE src != dst AND dst BETWEEN 1 AND 1000000
""")
b4 = db.execute("SELECT count(*) FROM st_edges").fetchone()[0] - prev
print(f"  Batch 4 (mentorship): {b4}")

# Batch 5: Bridge node cross-department
prev = db.execute("SELECT count(*) FROM st_edges").fetchone()[0]
db.execute("""
INSERT INTO st_edges
SELECT 40000000 + ROW_NUMBER() OVER (ORDER BY src, dst), src, dst,
    ROUND(0.3 + 0.3 * ((CAST(src * 19 + dst * 23 AS DOUBLE) * 0.618033988749895) % 1.0), 3),
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'liaison' WHEN 1 THEN 'cross-dept-bridge' WHEN 2 THEN 'inter-team-link'
    END, 2017 + CAST((src + dst) % 9 AS INT)
FROM (
    SELECT bridge_id AS src, ((bridge_id - 1 + off) % 1000000) + 1 AS dst
    FROM (SELECT gs AS bridge_id FROM generate_series(1, 1000000) AS t(gs) WHERE gs % 100 < 2) bridges
    CROSS JOIN (SELECT gs AS off FROM generate_series(1, 21) AS t(gs) WHERE gs != 20) offsets
) sub WHERE src != dst AND dst BETWEEN 1 AND 1000000
""")
b5 = db.execute("SELECT count(*) FROM st_edges").fetchone()[0] - prev
print(f"  Batch 5 (bridge nodes): {b5}")

# Batch 6: Hub node extra connections
prev = db.execute("SELECT count(*) FROM st_edges").fetchone()[0]
db.execute("""
INSERT INTO st_edges
SELECT 50000000 + ROW_NUMBER() OVER (ORDER BY src, dst), src, dst,
    ROUND(0.4 + 0.4 * ((CAST(src * 31 + dst * 37 AS DOUBLE) * 0.618033988749895) % 1.0), 3),
    CASE (CAST(src + dst AS BIGINT) % 3)
        WHEN 0 THEN 'leadership-network' WHEN 1 THEN 'executive-link' WHEN 2 THEN 'strategic-partner'
    END, 2014 + CAST((src + dst) % 12 AS INT)
FROM (
    SELECT hub_id AS src, ((hub_id - 1 + k * 7) % 1000000) + 1 AS dst
    FROM (
        SELECT m.hub_id, o.k
        FROM (SELECT gs * 20 AS hub_id FROM generate_series(1, 50000) AS t(gs)) m
        CROSS JOIN (SELECT gs AS k FROM generate_series(1, 50) AS t(gs)) o
        WHERE (m.hub_id % 1000 = 0 AND o.k <= 50)
           OR (m.hub_id % 1000 != 0 AND m.hub_id % 500 = 0 AND o.k <= 40)
           OR (m.hub_id % 500 != 0 AND m.hub_id % 100 = 0 AND o.k <= 25)
           OR (m.hub_id % 100 != 0 AND o.k <= 5)
    ) pairs
) sub WHERE src != dst AND dst BETWEEN 1 AND 1000000
""")
b6 = db.execute("SELECT count(*) FROM st_edges").fetchone()[0] - prev
print(f"  Batch 6 (hub nodes): {b6}")

# Batch 7: Weak ties
prev = db.execute("SELECT count(*) FROM st_edges").fetchone()[0]
db.execute("""
INSERT INTO st_edges
SELECT 60000000 + ROW_NUMBER() OVER (ORDER BY src, dst), src, dst,
    ROUND(0.05 + 0.15 * ((CAST(src * 43 + dst * 47 AS DOUBLE) * 0.618033988749895) % 1.0), 3),
    CASE (CAST(src * 7 + dst * 3 AS BIGINT) % 4)
        WHEN 0 THEN 'acquaintance' WHEN 1 THEN 'conference-contact'
        WHEN 2 THEN 'alumni-connection' WHEN 3 THEN 'referral'
    END, 2022 + CAST((src + dst) % 4 AS INT)
FROM (
    SELECT ((i * 104729 + 56891) % 1000000) + 1 AS src, ((i * 224737 + 31547) % 1000000) + 1 AS dst
    FROM generate_series(1, 320000) AS t(i)
) sub WHERE src != dst
""")
b7 = db.execute("SELECT count(*) FROM st_edges").fetchone()[0] - prev
print(f"  Batch 7 (weak ties): {b7}")

total_edges = db.execute("SELECT count(*) FROM st_edges").fetchone()[0]
print(f"  TOTAL EDGES: {total_edges}")
print(f"  Generation time: {time.time()-t0:.1f}s")

# ======================================================================
# Compute all proof values
# ======================================================================
print("\n=== Computing proof values ===\n")
results = {}

# Q1: Total employees
r = db.execute("SELECT count(*) FROM st_people").fetchone()
results['q1_total_employees'] = r[0]
print(f"Q1  total_employees = {r[0]}")

# Q2: Total connections
results['q2_total_connections'] = total_edges
print(f"Q2  total_connections = {total_edges}")

# Q3: Workforce by department (top 3 + Engineering avg_age)
rows = db.execute("""
    SELECT department, count(*) as headcount, avg(age) as avg_age
    FROM st_people GROUP BY department ORDER BY headcount DESC
""").fetchall()
results['q3_dept_count'] = len(rows)
results['q3_eng_headcount'] = [r for r in rows if r[0]=='Engineering'][0][1]
results['q3_sales_headcount'] = [r for r in rows if r[0]=='Sales'][0][1]
eng_avg = [r for r in rows if r[0]=='Engineering'][0][2]
results['q3_eng_avg_age'] = round(eng_avg, 5)
print(f"Q3  departments = {len(rows)}, Engineering headcount = {results['q3_eng_headcount']}, avg_age = {results['q3_eng_avg_age']}")

# Q4: Global footprint — city headcounts
rows = db.execute("SELECT city, count(*) as headcount FROM st_people GROUP BY city ORDER BY headcount DESC").fetchall()
results['q4_city_count'] = len(rows)
results['q4_cities'] = {r[0]: r[1] for r in rows}
print(f"Q4  cities = {len(rows)}, top: {rows[0][0]}={rows[0][1]}, {rows[1][0]}={rows[1][1]}, NYC={results['q4_cities']['NYC']}")

# Q5: Relationship type counts
rows = db.execute("""
    SELECT relationship_type, count(*) as cnt
    FROM st_edges GROUP BY relationship_type ORDER BY cnt DESC
""").fetchall()
results['q5_rel_types'] = len(rows)
results['q5_counts'] = {r[0]: r[1] for r in rows}
print(f"Q5  relationship_types = {len(rows)}")
for r in rows[:6]:
    print(f"     {r[0]} = {r[1]}")

# Q6: Engineering veterans over 50 — max age
r = db.execute("""
    SELECT name, age, city FROM st_people
    WHERE department = 'Engineering' AND age > 50
    ORDER BY age DESC LIMIT 5
""").fetchall()
results['q6_max_age'] = r[0][1]
results['q6_top_name'] = r[0][0]
results['q6_top_city'] = r[0][2]
results['q6_count'] = db.execute("SELECT count(*) FROM st_people WHERE department='Engineering' AND age > 50").fetchone()[0]
print(f"Q6  max_age = {r[0][1]}, top = {r[0][0]} in {r[0][2]}, total_over_50 = {results['q6_count']}")

# Q13: Mentorship level flow — count mentor edges by level pairs
rows = db.execute("""
    SELECT m.level as mentor_level, e.level as mentee_level, count(*) as cnt
    FROM st_edges ed
    JOIN st_people m ON ed.src = m.id
    JOIN st_people e ON ed.dst = e.id
    WHERE ed.relationship_type = 'mentor'
    GROUP BY m.level, e.level
    ORDER BY cnt DESC
""").fetchall()
results['q13_level_pairs'] = len(rows)
results['q13_top_pairs'] = [(r[0], r[1], r[2]) for r in rows[:5]]
print(f"Q13 mentorship level pairs = {len(rows)}")
for r in rows[:5]:
    print(f"     {r[0]} -> {r[1]} = {r[2]}")

# Q15: Within-department connections
r = db.execute("""
    SELECT count(*) as connections
    FROM st_edges e
    JOIN st_people s ON e.src = s.id
    JOIN st_people d ON e.dst = d.id
    WHERE s.department = d.department
""").fetchone()
results['q15_within_dept'] = r[0]
print(f"Q15 within_department connections = {r[0]}")

# Q16: Cross-department connections
r = db.execute("""
    SELECT count(*) as connections
    FROM st_edges e
    JOIN st_people s ON e.src = s.id
    JOIN st_people d ON e.dst = d.id
    WHERE s.department != d.department
""").fetchone()
results['q16_cross_dept'] = r[0]
print(f"Q16 cross_department connections = {r[0]}")

# Degree centrality (computed from edge table)
print("\n=== Degree Centrality ===")
rows = db.execute("""
    WITH out_d AS (SELECT src as node_id, count(*) as out_degree FROM st_edges GROUP BY src),
         in_d AS (SELECT dst as node_id, count(*) as in_degree FROM st_edges GROUP BY dst)
    SELECT COALESCE(o.node_id, i.node_id) as node_id,
           COALESCE(i.in_degree, 0) as in_degree,
           COALESCE(o.out_degree, 0) as out_degree,
           COALESCE(i.in_degree, 0) + COALESCE(o.out_degree, 0) as total_degree
    FROM out_d o FULL OUTER JOIN in_d i ON o.node_id = i.node_id
    ORDER BY total_degree DESC
    LIMIT 10
""").fetchall()
results['q17_top_nodes'] = [(r[0], r[1], r[2], r[3]) for r in rows[:5]]
print(f"Q17 Top degree nodes:")
for r in rows[:5]:
    print(f"     node_id={r[0]}: in={r[1]}, out={r[2]}, total={r[3]}")

# Edge count per batch verification (relationship types as proxy)
print("\n=== Edge breakdown by relationship ===")
rows = db.execute("""
    SELECT relationship_type, count(*) as cnt, round(avg(weight),4) as avg_w
    FROM st_edges GROUP BY relationship_type ORDER BY cnt DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]:25s} = {r[1]:>8d}  avg_weight={r[2]}")

# Specific employee verification (deterministic)
print("\n=== Specific Employee Verification ===")
rows = db.execute("""
    SELECT id, name, department, city, title, level, project_team
    FROM st_people WHERE id IN (1, 1000, 500000, 999999) ORDER BY id
""").fetchall()
for r in rows:
    print(f"  id={r[0]}: name={r[1]}, dept={r[2]}, city={r[3]}, title={r[4]}, level={r[5]}, team={r[6]}")
results['emp_1'] = rows[0]
results['emp_1000'] = rows[1]
results['emp_500000'] = rows[2]
results['emp_999999'] = rows[3]

# VIZ query row counts (edges within id ranges)
print("\n=== VIZ Edge Counts ===")
for limit in [100, 500, 1000, 5000, 10000, 50000, 100000]:
    r = db.execute(f"SELECT count(*) FROM st_edges WHERE src <= {limit} AND dst <= {limit}").fetchone()
    print(f"  edges where src,dst <= {limit:>7d}: {r[0]}")
    results[f'viz_{limit}'] = r[0]

# Title distribution
print("\n=== Title Distribution ===")
rows = db.execute("""
    SELECT title, count(*) as cnt FROM st_people GROUP BY title ORDER BY cnt DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]:20s} = {r[1]:>8d}")
results['titles'] = {r[0]: r[1] for r in rows}

# Level distribution
print("\n=== Level Distribution ===")
rows = db.execute("""
    SELECT level, count(*) as cnt FROM st_people GROUP BY level ORDER BY level
""").fetchall()
for r in rows:
    print(f"  {r[0]:5s} = {r[1]:>8d}")
results['levels'] = {r[0]: r[1] for r in rows}

# Active/inactive counts
r = db.execute("SELECT count(*) FROM st_people WHERE active = true").fetchone()
results['active_count'] = r[0]
r2 = db.execute("SELECT count(*) FROM st_people WHERE active = false").fetchone()
results['inactive_count'] = r2[0]
print(f"\nActive = {results['active_count']}, Inactive = {results['inactive_count']}")

# Hire year range
rows = db.execute("SELECT hire_year, count(*) FROM st_people GROUP BY hire_year ORDER BY hire_year").fetchall()
results['hire_years'] = {r[0]: r[1] for r in rows}
print(f"Hire years: {rows[0][0]}-{rows[-1][0]}, {len(rows)} distinct years")

# Per-batch edge counts
results['batch_counts'] = {
    'dept_neighborhood': b1,
    'team_connections': b2,
    'city_social': b3,
    'mentorship': b4,
    'bridge_nodes': b5,
    'hub_nodes': b6,
    'weak_ties': b7,
}
print(f"\n=== Batch Summary ===")
for k,v in results['batch_counts'].items():
    print(f"  {k:25s} = {v}")

# Top cross-department pairs
print("\n=== Top Cross-Department Connections ===")
rows = db.execute("""
    SELECT s.department as from_dept, d.department as to_dept, count(*) as connections
    FROM st_edges e
    JOIN st_people s ON e.src = s.id
    JOIN st_people d ON e.dst = d.id
    WHERE s.department != d.department
    GROUP BY s.department, d.department
    ORDER BY connections DESC
    LIMIT 5
""").fetchall()
for r in rows:
    print(f"  {r[0]:20s} -> {r[1]:20s} = {r[2]}")
results['q8_top_cross'] = [(r[0], r[1], r[2]) for r in rows]

# Mentor relationship count
r = db.execute("SELECT count(*) FROM st_edges WHERE relationship_type = 'mentor'").fetchone()
results['mentor_count'] = r[0]
print(f"\nTotal mentor edges: {r[0]}")

# Bridge node count
r = db.execute("SELECT count(DISTINCT src) FROM st_edges WHERE relationship_type IN ('liaison','cross-dept-bridge','inter-team-link')").fetchone()
results['bridge_node_count'] = r[0]
print(f"Distinct bridge sources: {r[0]}")

elapsed = time.time() - t0
print(f"\n=== Done in {elapsed:.1f}s ===")

# Save results as JSON for reference
with open('/home/chess/delta-forge/delta-forge-demos/demos/graph/graph-stress-test/proof_values.json', 'w') as f:
    # Convert tuples to lists for JSON serialization
    json_safe = {}
    for k, v in results.items():
        if isinstance(v, list) and v and isinstance(v[0], tuple):
            json_safe[k] = [list(t) for t in v]
        elif isinstance(v, tuple):
            json_safe[k] = list(v)
        else:
            json_safe[k] = v
    json.dump(json_safe, f, indent=2, default=str)

print("\nProof values saved to proof_values.json")
