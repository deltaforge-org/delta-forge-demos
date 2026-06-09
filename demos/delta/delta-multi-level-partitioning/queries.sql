-- ============================================================================
-- Delta Multi-Level Partitioning — Educational Queries
-- ============================================================================
-- WHAT: Multi-level partitioning creates a nested directory hierarchy using
--       two or more partition columns. With PARTITIONED BY (region, quarter),
--       the engine creates 4 × 4 = 16 partition directories:
--         region=us-east/quarter=Q1-2024/
--         region=us-east/quarter=Q2-2024/
--         ...
--         region=ap-south/quarter=Q4-2024/
--
-- WHY:  More partition columns = finer granularity = more aggressive pruning.
--       A single-level partition (BY region) can skip at most 3 of 4 dirs.
--       A multi-level partition (BY region, quarter) can skip up to 15 of 16.
--
-- HOW TO OBSERVE PRUNING:
--   The compute engine logs partition skip counts at INFO level:
--     "File filtering: X of 16 files skipped (partition pruning: X, ...)"
--   Each query below notes the expected pruning ratio in ENGINE LOG comments.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The 16-Partition Fact Table
-- ============================================================================
-- With PARTITIONED BY (region, quarter), Delta creates 16 directories.
-- Each holds 3-4 rows of Parquet data. The distribution is:
--   Q1-Q3: 4 rows per region-quarter combination (4×4×3 = 48 rows)
--   Q4:    3 rows per region (4×3 = 12 rows)
--   Total: 60 rows across 16 partitions.

ASSERT VALUE total_sales = 18735.5 WHERE region = 'us-east'
ASSERT VALUE total_sales = 21630.0 WHERE region = 'us-west'
ASSERT VALUE total_sales = 18595.0 WHERE region = 'eu-west'
ASSERT VALUE total_sales = 16610.0 WHERE region = 'ap-south'
ASSERT ROW_COUNT = 4
SELECT region,
       COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_ml
GROUP BY region
ORDER BY region;


-- ============================================================================
-- EXPLORE: The Two Dimension Tables
-- ============================================================================
-- Two dimension tables drive pruning on different axes:
--   region_dim   → filters which region partitions to scan
--   quarter_dim  → filters which quarter partitions to scan
-- Together they enable compound dynamic pruning across both levels.

ASSERT ROW_COUNT = 4
SELECT region, target_amount, market
FROM {{zone_name}}.delta_demos.region_dim
ORDER BY target_amount DESC;


ASSERT ROW_COUNT = 4
SELECT quarter, budget, half_year
FROM {{zone_name}}.delta_demos.quarter_dim
ORDER BY budget DESC;


-- ============================================================================
-- LEARN: Single-Level Pruning on Region (4 of 16 scanned)
-- ============================================================================
-- Filtering on region='us-east' prunes all non-us-east partitions.
-- In a multi-level table, this skips 12 of 16 directories — all four
-- quarters of the other three regions are eliminated.
--
-- ENGINE LOG: Expect "partition pruning: 12" — only 4 of 16 scanned (75%).

ASSERT VALUE row_count = 15
ASSERT VALUE total_sales = 18735.5
SELECT COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_ml
WHERE region = 'us-east';


-- ============================================================================
-- LEARN: Single-Level Pruning on Quarter (4 of 16 scanned)
-- ============================================================================
-- Filtering on the second partition column (quarter) works the same way.
-- quarter='Q3-2024' prunes all non-Q3 partitions across every region.
--
-- ENGINE LOG: Expect "partition pruning: 12" — only 4 of 16 scanned (75%).
-- Same reduction as the region filter, but on the other axis.

ASSERT VALUE row_count = 16
ASSERT VALUE total_sales = 20385.0
SELECT COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_ml
WHERE quarter = 'Q3-2024';


-- ============================================================================
-- LEARN: Compound Static Pruning (1 of 16 scanned)
-- ============================================================================
-- Filtering on BOTH partition columns narrows to a single directory.
-- WHERE region='us-east' AND quarter='Q1-2024' opens exactly one
-- partition: region=us-east/quarter=Q1-2024/ containing just 4 rows.
--
-- ENGINE LOG: Expect "partition pruning: 15" — 1 of 16 scanned (93.75%).
-- This is the maximum possible pruning: only the exact partition needed.

ASSERT VALUE row_count = 4
ASSERT VALUE total_sales = 4825.5
SELECT COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_ml
WHERE region = 'us-east' AND quarter = 'Q1-2024';


-- ============================================================================
-- LEARN: Dynamic Pruning via Region Dimension (8 of 16 scanned)
-- ============================================================================
-- Joining with region_dim and filtering target_amount > 50000 selects
-- us-east (75000) and us-west (60000). The engine prunes all eu-west
-- and ap-south partitions — 8 of the 16 directories are skipped.
--
-- ENGINE LOG: Expect "partition pruning: 8" — 8 of 16 scanned (50%).
-- Compare this to single-level partitioning where the same filter only
-- skips 2 of 4 partitions. More levels = finer control.

ASSERT VALUE total_sales = 18735.5 WHERE region = 'us-east'
ASSERT VALUE total_sales = 21630.0 WHERE region = 'us-west'
ASSERT ROW_COUNT = 2
SELECT s.region,
       r.target_amount,
       COUNT(*) AS row_count,
       ROUND(SUM(s.amount), 2) AS total_sales
FROM {{zone_name}}.delta_demos.sales_ml s
JOIN {{zone_name}}.delta_demos.region_dim r
    ON s.region = r.region
WHERE r.target_amount > 50000
GROUP BY s.region, r.target_amount
ORDER BY s.region;


-- ============================================================================
-- LEARN: Dynamic Pruning via Quarter Dimension (8 of 16 scanned)
-- ============================================================================
-- Now prune on the OTHER axis. Joining with quarter_dim and filtering
-- budget > 16000 selects Q1 (20000) and Q2 (18000). The engine skips
-- all Q3 and Q4 partitions across every region.
--
-- ENGINE LOG: Expect "partition pruning: 8" — 8 of 16 scanned (50%).
-- Same 50% reduction, but on the quarter dimension instead of region.

ASSERT VALUE total_sales = 21640.5 WHERE quarter = 'Q1-2024'
ASSERT VALUE total_sales = 20520.0 WHERE quarter = 'Q2-2024'
ASSERT ROW_COUNT = 2
SELECT s.quarter,
       q.budget,
       COUNT(*) AS row_count,
       ROUND(SUM(s.amount), 2) AS total_sales
FROM {{zone_name}}.delta_demos.sales_ml s
JOIN {{zone_name}}.delta_demos.quarter_dim q
    ON s.quarter = q.quarter
WHERE q.budget > 16000
GROUP BY s.quarter, q.budget
ORDER BY s.quarter;


-- ============================================================================
-- LEARN: Compound Dynamic Pruning — Both Dimensions (4 of 16 scanned)
-- ============================================================================
-- The key advantage of multi-level partitioning: joining BOTH dimension
-- tables and filtering on both narrows the scan dramatically.
--
-- target_amount > 50000 → regions: us-east, us-west (2 of 4)
-- budget > 16000        → quarters: Q1-2024, Q2-2024  (2 of 4)
-- Combined: 2 × 2 = 4 of 16 partitions scanned (75% pruned)
--
-- ENGINE LOG: Expect "partition pruning: 12" — only 4 of 16 scanned.
-- The pruning multiplies: 50% on each axis = 75% total reduction.

ASSERT VALUE total_sales = 10485.5 WHERE region = 'us-east'
ASSERT VALUE total_sales = 11890.0 WHERE region = 'us-west'
ASSERT ROW_COUNT = 2
SELECT s.region,
       r.target_amount,
       COUNT(*) AS row_count,
       ROUND(SUM(s.amount), 2) AS total_sales
FROM {{zone_name}}.delta_demos.sales_ml s
JOIN {{zone_name}}.delta_demos.region_dim r
    ON s.region = r.region
JOIN {{zone_name}}.delta_demos.quarter_dim q
    ON s.quarter = q.quarter
WHERE r.target_amount > 50000
  AND q.budget > 16000
GROUP BY s.region, r.target_amount
ORDER BY s.region;


-- ============================================================================
-- LEARN: Narrowest Compound Pruning (1 of 16 scanned)
-- ============================================================================
-- The most aggressive dynamic pruning: dimension filters that resolve to
-- a single value on each axis. target_amount = 75000 → us-east only.
-- budget = 20000 → Q1-2024 only. Result: exactly 1 of 16 partitions.
--
-- ENGINE LOG: Expect "partition pruning: 15" — 1 of 16 scanned (93.75%).
-- In a production table with millions of rows across hundreds of
-- partitions, this would read only a tiny fraction of the data.

ASSERT VALUE row_count = 4
ASSERT VALUE total_sales = 4825.5
SELECT COUNT(*) AS row_count,
       ROUND(SUM(s.amount), 2) AS total_sales,
       SUM(s.qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_ml s
JOIN {{zone_name}}.delta_demos.region_dim r
    ON s.region = r.region
JOIN {{zone_name}}.delta_demos.quarter_dim q
    ON s.quarter = q.quarter
WHERE r.target_amount = 75000
  AND q.budget = 20000;


-- ============================================================================
-- CONTRAST: Non-Partition Filter (0 of 16 pruned)
-- ============================================================================
-- Filtering on a non-partition column (channel) cannot prune any partitions.
-- The engine must open ALL 16 directories to find wholesale rows because
-- wholesale sales exist in every region-quarter combination.
--
-- ENGINE LOG: Expect "partition pruning: 0" — all 16 scanned.
-- This shows why partition column choice matters: if most queries filter
-- on channel, partitioning by (region, quarter) offers no pruning benefit.

ASSERT VALUE row_count = 20
ASSERT VALUE total_sales = 31890.0
SELECT COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_ml
WHERE channel = 'wholesale';


-- ============================================================================
-- LEARN: Partition-Level Aggregation
-- ============================================================================
-- GROUP BY on partition columns is especially efficient because the engine
-- can process each partition independently. With 16 partitions, the engine
-- reads each directory and computes its aggregate without cross-partition
-- shuffling. This is a natural fit for time-series and regional analytics.

ASSERT VALUE total_sales = 4825.5 WHERE region = 'us-east' AND quarter = 'Q1-2024'
ASSERT VALUE total_sales = 6380.0 WHERE region = 'us-west' AND quarter = 'Q1-2024'
ASSERT VALUE total_sales = 2840.0 WHERE region = 'ap-south' AND quarter = 'Q4-2024'
ASSERT ROW_COUNT = 16
SELECT region,
       quarter,
       COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_ml
GROUP BY region, quarter
ORDER BY region, quarter;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 60 rows in sales_ml
ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.delta_demos.sales_ml;

-- Verify partition_count: 16 distinct (region, quarter) combinations
ASSERT VALUE cnt = 16
SELECT COUNT(DISTINCT region || '/' || quarter) AS cnt FROM {{zone_name}}.delta_demos.sales_ml;

-- Verify region_count: 4 distinct regions
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT region) AS cnt FROM {{zone_name}}.delta_demos.sales_ml;

-- Verify quarter_count: 4 distinct quarters
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT quarter) AS cnt FROM {{zone_name}}.delta_demos.sales_ml;

-- Verify single_partition: us-east/Q1-2024 has exactly 4 rows
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_ml WHERE region = 'us-east' AND quarter = 'Q1-2024';

-- Verify compound_dynamic: 16 rows when both dimensions > threshold
ASSERT VALUE cnt = 16
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_ml s JOIN {{zone_name}}.delta_demos.region_dim r ON s.region = r.region JOIN {{zone_name}}.delta_demos.quarter_dim q ON s.quarter = q.quarter WHERE r.target_amount > 50000 AND q.budget > 16000;

-- Verify grand_total: all amounts sum correctly
ASSERT VALUE total = 75570.5
SELECT ROUND(SUM(amount), 2) AS total FROM {{zone_name}}.delta_demos.sales_ml;

-- Verify dimension_tables: 4 rows each
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.region_dim;

ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.quarter_dim;
