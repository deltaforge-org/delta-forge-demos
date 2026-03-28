-- ============================================================================
-- Retail Sales Rep Leaderboard — Educational Queries
-- ============================================================================
-- WHAT: Advanced window functions for sales leaderboard analytics
-- WHY:  Ranking, tiering, and relative-standing queries are essential for
--       performance dashboards and compensation planning
-- HOW:  DENSE_RANK, NTILE, LEAD, PERCENT_RANK, CUME_DIST, and window frames
--       operate over partitions and orderings without self-joins
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Q4 Raw Data
-- ============================================================================
-- Show all 10 reps' Q4 performance as the foundation for subsequent analysis.
-- Note the deliberate ties: Alice, David, and James all posted $175K in Q4;
-- Bob and Irene both posted $130K.

ASSERT ROW_COUNT = 10
ASSERT VALUE revenue = 310000.00 WHERE rep_name = 'Carol Wu'
ASSERT VALUE revenue = 80000.00 WHERE rep_name = 'Hank Davis'
SELECT rep_id, rep_name, region, quarter, revenue, deals_closed, quota
FROM {{zone_name}}.delta_demos.sales_reps
WHERE quarter = 'Q4'
ORDER BY revenue DESC;


-- ============================================================================
-- LEARN: DENSE_RANK — Regional Leaderboard Positions
-- ============================================================================
-- DENSE_RANK() assigns the same rank to ties and does NOT skip values.
-- Unlike RANK(), after two reps tied at rank 2, the next rep gets rank 3
-- (not rank 4). This is ideal for leaderboard displays where gaps look wrong.
--
-- In the North region: Alice=175K (rank 1), Bob & Irene both=130K (rank 2).
-- In the South region: Carol=310K (rank 1), David & James both=175K (rank 2).

ASSERT ROW_COUNT = 10
ASSERT VALUE region_rank = 1 WHERE rep_name = 'Alice Chen'
ASSERT VALUE region_rank = 2 WHERE rep_name = 'Bob Martinez'
ASSERT VALUE region_rank = 2 WHERE rep_name = 'Irene Novak'
ASSERT VALUE region_rank = 1 WHERE rep_name = 'Carol Wu'
ASSERT VALUE region_rank = 2 WHERE rep_name = 'David Kim'
SELECT rep_name, region, revenue,
       DENSE_RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS region_rank
FROM {{zone_name}}.delta_demos.sales_reps
WHERE quarter = 'Q4'
ORDER BY region, region_rank, rep_name;


-- ============================================================================
-- LEARN: NTILE(4) — Performance Quartile Tiers
-- ============================================================================
-- NTILE(4) divides the 10 reps into 4 roughly equal groups by total annual
-- revenue. With 10 rows: tiles get sizes 3, 3, 2, 2.
-- Tier 1 (top) = Carol ($980K), Eve ($830K), Grace ($750K)
-- Tier 2       = David ($630K), Alice ($580K), James ($565K)
-- Tier 3       = Irene ($495K), Bob ($440K)
-- Tier 4 (low) = Frank ($335K), Hank ($275K)

ASSERT ROW_COUNT = 10
ASSERT VALUE performance_tier = 1 WHERE rep_name = 'Carol Wu'
ASSERT VALUE performance_tier = 1 WHERE rep_name = 'Eve Johnson'
ASSERT VALUE performance_tier = 2 WHERE rep_name = 'Alice Chen'
ASSERT VALUE performance_tier = 3 WHERE rep_name = 'Bob Martinez'
ASSERT VALUE performance_tier = 4 WHERE rep_name = 'Hank Davis'
ASSERT VALUE performance_tier = 4 WHERE rep_name = 'Frank Lee'
SELECT rep_name,
       ROUND(SUM(revenue), 2) AS annual_revenue,
       NTILE(4) OVER (ORDER BY SUM(revenue) DESC) AS performance_tier
FROM {{zone_name}}.delta_demos.sales_reps
GROUP BY rep_name
ORDER BY annual_revenue DESC;


-- ============================================================================
-- LEARN: LEAD — Gap to Next-Best Performer
-- ============================================================================
-- LEAD(column) OVER (...) accesses the next row's value within a partition.
-- When ordered by revenue DESC, LEAD gives the next-lower revenue, so the
-- difference shows how much a rep outpaces the next competitor.
-- The last rep in each region gets NULL (no one below them).
--
-- East:  Eve ($245K) leads Frank ($95K) by $150K
-- West:  Grace ($225K) leads Hank ($80K) by $145K
-- North: Alice ($175K) leads Bob ($130K) by $45K; Bob ties Irene, gap = $0

ASSERT ROW_COUNT = 10
ASSERT VALUE revenue_gap = 150000.00 WHERE rep_name = 'Eve Johnson'
ASSERT VALUE revenue_gap = 145000.00 WHERE rep_name = 'Grace Park'
ASSERT VALUE revenue_gap = 45000.00 WHERE rep_name = 'Alice Chen'
ASSERT VALUE revenue_gap = 0.00 WHERE rep_name = 'Bob Martinez'
SELECT rep_name, region, revenue,
       LEAD(revenue) OVER (PARTITION BY region ORDER BY revenue DESC) AS next_best_revenue,
       ROUND(revenue - LEAD(revenue) OVER (PARTITION BY region ORDER BY revenue DESC), 2) AS revenue_gap
FROM {{zone_name}}.delta_demos.sales_reps
WHERE quarter = 'Q4'
ORDER BY region, revenue DESC;


-- ============================================================================
-- LEARN: PERCENT_RANK & CUME_DIST — Relative Standing
-- ============================================================================
-- PERCENT_RANK = (rank - 1) / (row_count - 1).  Range: 0.0 to 1.0.
-- CUME_DIST    = (rows with value <= current) / row_count.  Range: > 0.0 to 1.0.
--
-- These show where each rep falls in the overall Q4 distribution.
-- Carol (highest at $310K): pct_rank=1.0, cume_dist=1.0
-- Hank (lowest at $80K):    pct_rank=0.0, cume_dist=0.1
-- The three-way tie at $175K (Alice, David, James) all get pct_rank=0.44

ASSERT ROW_COUNT = 10
ASSERT VALUE pct_rank = 1.0 WHERE rep_name = 'Carol Wu'
ASSERT VALUE pct_rank = 0.0 WHERE rep_name = 'Hank Davis'
ASSERT VALUE cume_dist = 1.0 WHERE rep_name = 'Carol Wu'
ASSERT VALUE cume_dist = 0.1 WHERE rep_name = 'Hank Davis'
ASSERT VALUE pct_rank = 0.44 WHERE rep_name = 'Alice Chen'
ASSERT VALUE cume_dist = 0.7 WHERE rep_name = 'David Kim'
SELECT rep_name, region, revenue,
       ROUND(PERCENT_RANK() OVER (ORDER BY revenue), 2) AS pct_rank,
       ROUND(CUME_DIST() OVER (ORDER BY revenue), 2) AS cume_dist
FROM {{zone_name}}.delta_demos.sales_reps
WHERE quarter = 'Q4'
ORDER BY revenue DESC;


-- ============================================================================
-- LEARN: Window Frame — 2-Quarter Moving Average Revenue
-- ============================================================================
-- ROWS BETWEEN 1 PRECEDING AND CURRENT ROW averages the current quarter with
-- the prior quarter, smoothing seasonal spikes. Q1 has no predecessor so the
-- moving average equals the raw Q1 value.
--
-- Alice Chen:  Q1=120K, Q2=127.5K, Q3=142.5K, Q4=162.5K
-- Carol Wu:    Q1=200K, Q2=210K,   Q3=235K,   Q4=280K

ASSERT ROW_COUNT = 40
ASSERT VALUE moving_avg_revenue = 127500.0 WHERE rep_name = 'Alice Chen' AND quarter = 'Q2'
ASSERT VALUE moving_avg_revenue = 162500.0 WHERE rep_name = 'Alice Chen' AND quarter = 'Q4'
ASSERT VALUE moving_avg_revenue = 210000.0 WHERE rep_name = 'Carol Wu' AND quarter = 'Q2'
ASSERT VALUE moving_avg_revenue = 280000.0 WHERE rep_name = 'Carol Wu' AND quarter = 'Q4'
SELECT rep_name, quarter, revenue,
       ROUND(AVG(revenue) OVER (PARTITION BY rep_name ORDER BY quarter
                                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS moving_avg_revenue
FROM {{zone_name}}.delta_demos.sales_reps
ORDER BY rep_name, quarter;


-- ============================================================================
-- VERIFY: All Checks — Cross-Cutting Sanity
-- ============================================================================
-- Summary verification ensuring the dataset and analytics produce expected results.

-- Verify total row count
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.sales_reps;

-- Verify distinct rep count
ASSERT VALUE distinct_reps = 10
SELECT COUNT(DISTINCT rep_name) AS distinct_reps
FROM {{zone_name}}.delta_demos.sales_reps;

-- Verify each rep has exactly 4 quarters
ASSERT VALUE bad_rep_count = 0
SELECT COUNT(*) AS bad_rep_count FROM (
    SELECT rep_name, COUNT(*) AS c
    FROM {{zone_name}}.delta_demos.sales_reps
    GROUP BY rep_name
) WHERE c != 4;

-- Verify 4 distinct regions
ASSERT VALUE region_count = 4
SELECT COUNT(DISTINCT region) AS region_count
FROM {{zone_name}}.delta_demos.sales_reps;

-- Verify Carol Wu is the top annual earner
ASSERT VALUE top_rep = 'Carol Wu'
SELECT rep_name AS top_rep FROM (
    SELECT rep_name, SUM(revenue) AS total
    FROM {{zone_name}}.delta_demos.sales_reps
    GROUP BY rep_name
    ORDER BY total DESC
    LIMIT 1
);

-- Verify the three-way Q4 tie count at $175K
ASSERT VALUE tie_count = 3
SELECT COUNT(*) AS tie_count
FROM {{zone_name}}.delta_demos.sales_reps
WHERE quarter = 'Q4' AND revenue = 175000.00;

-- Verify Hank Davis is the lowest annual earner at $275K
ASSERT VALUE lowest_annual = 275000.0
SELECT ROUND(SUM(revenue), 2) AS lowest_annual
FROM {{zone_name}}.delta_demos.sales_reps
WHERE rep_name = 'Hank Davis';
