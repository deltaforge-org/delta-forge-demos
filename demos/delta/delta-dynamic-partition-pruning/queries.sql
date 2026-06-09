-- ============================================================================
-- Delta Dynamic Partition Pruning — Educational Queries
-- ============================================================================
-- WHAT: Dynamic partition pruning skips entire partition directories during
--       query execution when the query engine can determine from a JOIN or
--       subquery that certain partition values will produce no matching rows.
-- WHY:  In star-schema analytics, fact tables are large and partitioned, while
--       dimension tables are small. Without pruning, a JOIN scans ALL fact
--       partitions even when the dimension filter selects only a subset.
-- HOW:  The engine first evaluates the dimension-side filter to determine
--       which partition values are needed, then only opens data files in
--       those partition directories — skipping the rest entirely.
--
-- HOW TO OBSERVE PRUNING:
--   The DeltaForge engine logs partition pruning activity at INFO level.
--   After each query, the compute engine outputs a line like:
--
--     File filtering: 2 of 4 files skipped
--       (partition pruning: 2, statistics: 0, 50.0% reduction)
--
--   The execution plan (DeltaScanExec) also shows skip counts:
--
--     DeltaScanExec: table=sales_facts, files=2/4
--       (skipped: partition=2, stats=0), rows=28 (effective=28)
--
--   "partition=2" means 2 partition directories were never opened.
--   "stats=0" means no additional files skipped by min/max statistics.
--   Check the compute engine's log output to see these lines in action.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Inspect the Partitioned Fact Table
-- ============================================================================
-- The sales_facts table is PARTITIONED BY (region). On disk, Delta creates
-- separate directories for each partition value:
--   region=us-east/
--   region=us-west/
--   region=eu-west/
--   region=ap-south/
-- Each directory contains only the Parquet files for that region's rows.

ASSERT VALUE total_sales = 18415.5 WHERE region = 'us-east'
ASSERT VALUE total_sales = 21380.0 WHERE region = 'us-west'
ASSERT VALUE total_sales = 18405.0 WHERE region = 'eu-west'
ASSERT VALUE total_sales = 13293.0 WHERE region = 'ap-south'
ASSERT ROW_COUNT = 4
SELECT region, COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_facts
GROUP BY region
ORDER BY region;


-- ============================================================================
-- EXPLORE: Inspect the Dimension Table
-- ============================================================================
-- The region_targets table acts as a dimension/lookup table with one row per
-- region, containing target amounts and quantities. It is small and not
-- partitioned — this is the table that drives dynamic partition pruning
-- when joined with the fact table and filtered.

ASSERT ROW_COUNT = 4
SELECT region, target_amount, target_qty
FROM {{zone_name}}.delta_demos.region_targets
ORDER BY target_amount DESC;


-- ============================================================================
-- LEARN: Dynamic Partition Pruning via JOIN
-- ============================================================================
-- When we JOIN sales_facts with region_targets and filter on
-- target_amount > 50000, only us-east (75000) and us-west (60000) match.
--
-- With dynamic partition pruning, the engine:
--   1. Evaluates the dimension filter: target_amount > 50000
--   2. Determines matching regions: us-east, us-west
--   3. Opens ONLY the region=us-east/ and region=us-west/ partition dirs
--   4. Skips region=eu-west/ and region=ap-south/ entirely
--
-- This means only 28 rows are scanned instead of all 55 — a 49% reduction.
--
-- ENGINE LOG: For this query you should see:
--   "File filtering: 2 of 4 files skipped (partition pruning: 2, ...)"
-- The engine skips the eu-west and ap-south partition directories entirely —
-- their Parquet files are never opened or read.

-- Verify only us-east and us-west match target_amount > 50000
ASSERT VALUE target_amount = 75000 WHERE region = 'us-east'
ASSERT VALUE target_amount = 60000 WHERE region = 'us-west'
ASSERT VALUE fact_rows = 14 WHERE region = 'us-east'
ASSERT VALUE actual_sales = 18415.5 WHERE region = 'us-east'
ASSERT VALUE fact_rows = 14 WHERE region = 'us-west'
ASSERT VALUE actual_sales = 21380.0 WHERE region = 'us-west'
ASSERT ROW_COUNT = 2
SELECT s.region, t.target_amount,
       COUNT(*) AS fact_rows,
       ROUND(SUM(s.amount), 2) AS actual_sales,
       ROUND(SUM(s.amount) / t.target_amount * 100, 1) AS pct_of_target
FROM {{zone_name}}.delta_demos.sales_facts s
JOIN {{zone_name}}.delta_demos.region_targets t
    ON s.region = t.region
WHERE t.target_amount > 50000
GROUP BY s.region, t.target_amount
ORDER BY s.region;


-- ============================================================================
-- LEARN: Subquery-Based Dynamic Pruning
-- ============================================================================
-- The classic form of dynamic partition pruning uses an IN-subquery rather
-- than a JOIN. The engine evaluates the subquery first to collect the set of
-- matching partition values, then prunes fact-table partitions that are not
-- in that set — identical to the JOIN approach but more explicit.
--
-- Here we select only regions whose target_qty >= 400 (us-east=500,
-- us-west=400). The engine resolves the subquery to {'us-east','us-west'},
-- then skips the eu-west and ap-south partition directories entirely.
--
-- ENGINE LOG: Same as the JOIN query — expect "partition pruning: 2".

ASSERT VALUE total_sales = 18415.5 WHERE region = 'us-east'
ASSERT VALUE total_sales = 21380.0 WHERE region = 'us-west'
ASSERT ROW_COUNT = 2
SELECT region,
       COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_facts
WHERE region IN (
    SELECT region FROM {{zone_name}}.delta_demos.region_targets
    WHERE target_qty >= 400
)
GROUP BY region
ORDER BY region;


-- ============================================================================
-- CONTRAST: No Pruning Possible — Non-Partition Filter
-- ============================================================================
-- Filtering on a non-partition column (channel) cannot prune partitions.
-- The engine must open ALL 4 partition directories to find 'online' rows
-- because any partition could contain online sales.
--
-- ENGINE LOG: For this query you should see:
--   "File filtering: 0 of 4 files skipped (partition pruning: 0, ...)"
-- Compare this to the pruned queries above where 2 of 4 were skipped.
-- This contrast shows why choosing the right partition column matters:
-- partition by your most common filter dimension.

ASSERT VALUE row_count = 5 WHERE region = 'us-east'
ASSERT VALUE row_count = 5 WHERE region = 'us-west'
ASSERT VALUE row_count = 5 WHERE region = 'eu-west'
ASSERT VALUE row_count = 4 WHERE region = 'ap-south'
ASSERT ROW_COUNT = 4
SELECT region,
       COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales
FROM {{zone_name}}.delta_demos.sales_facts
WHERE channel = 'online'
GROUP BY region
ORDER BY region;


-- ============================================================================
-- LEARN: Static Partition Pruning
-- ============================================================================
-- Static partition pruning is the simplest form: a direct WHERE clause on
-- the partition column. No JOIN or subquery needed — the engine sees
-- WHERE region = 'us-east' and immediately opens only the region=us-east/
-- directory, skipping the other 3 partitions entirely.
--
-- ENGINE LOG: Expect "partition pruning: 3" — 3 of 4 partitions skipped.
-- This is the most aggressive pruning (75% reduction) because only one
-- partition value is selected.

ASSERT VALUE row_count = 14
ASSERT VALUE total_sales = 18415.5
SELECT COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales,
       ROUND(AVG(amount), 2) AS avg_sale,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_facts
WHERE region = 'us-east';


-- ============================================================================
-- LEARN: Anti-Join Pruning (Exclusion Pattern)
-- ============================================================================
-- Pruning also works by exclusion. Here we use NOT IN with a subquery to
-- exclude regions with low targets (target_amount < 50000 → eu-west, ap-south).
-- The engine resolves the exclusion set and prunes those 2 partitions,
-- scanning only us-east and us-west — the same result as the inclusion
-- approach (WHERE target_amount > 50000) but expressed as a negation.
--
-- ENGINE LOG: Same as the JOIN query — expect "partition pruning: 2".
-- Anti-join pruning is useful when the exclusion list is shorter than
-- the inclusion list.

ASSERT VALUE total_sales = 18415.5 WHERE region = 'us-east'
ASSERT VALUE total_sales = 21380.0 WHERE region = 'us-west'
ASSERT ROW_COUNT = 2
SELECT region,
       COUNT(*) AS row_count,
       ROUND(SUM(amount), 2) AS total_sales
FROM {{zone_name}}.delta_demos.sales_facts
WHERE region NOT IN (
    SELECT region FROM {{zone_name}}.delta_demos.region_targets
    WHERE target_amount < 50000
)
GROUP BY region
ORDER BY region;


-- ============================================================================
-- LEARN: Partition-Scoped DML Operations
-- ============================================================================
-- The UPDATE that discounted ap-south amounts by 10% only rewrote files in
-- the region=ap-south/ partition directory. Files in other partitions were
-- untouched. This is a major benefit of partitioning — DML operations
-- scoped to a single partition value avoid rewriting the entire table.
--
-- Let's verify the ap-south discount was applied and other regions are
-- unaffected by checking a known value from each region.

-- Verify ap-south discount was applied (id=46 should have discounted price)
ASSERT VALUE price_status = 'Discounted (x0.90)' WHERE region = 'ap-south'
ASSERT VALUE amount = 468.0 WHERE id = 46
ASSERT ROW_COUNT = 4
SELECT id, region, amount,
       CASE
           WHEN region = 'ap-south' THEN 'Discounted (x0.90)'
           ELSE 'Original price'
       END AS price_status
FROM {{zone_name}}.delta_demos.sales_facts
WHERE id IN (1, 16, 31, 46)
ORDER BY id;


-- ============================================================================
-- EXPLORE: Quarterly Performance by Channel (Full Scan)
-- ============================================================================
-- Partitioning by region optimizes region-based queries, but queries on
-- other dimensions (quarter, channel) still scan all partitions. Choosing
-- the right partition column depends on your most common query patterns.
--
-- ENGINE LOG: No partition pruning here — all 4 partition directories are
-- scanned because neither quarter nor channel is the partition column.

ASSERT VALUE total_amount = 11060.0 WHERE quarter = 'Q1-2024' AND channel = 'wholesale'
ASSERT VALUE total_amount = 5913.0 WHERE quarter = 'Q1-2024' AND channel = 'online'
ASSERT ROW_COUNT = 12
SELECT quarter,
       channel,
       COUNT(*) AS sale_count,
       ROUND(SUM(amount), 2) AS total_amount,
       SUM(qty) AS total_qty
FROM {{zone_name}}.delta_demos.sales_facts
GROUP BY quarter, channel
ORDER BY quarter, channel;


-- ============================================================================
-- LEARN: Target Achievement by Region
-- ============================================================================
-- This star-schema query demonstrates the full power of partitioned fact +
-- dimension tables. Each region's actual sales are compared against targets.
-- Dynamic partition pruning means adding a WHERE clause on the dimension
-- (e.g., WHERE t.target_qty > 300) would skip partition directories for
-- regions that do not meet the filter — without scanning their data files.

ASSERT VALUE actual_amount = 18415.5 WHERE region = 'us-east'
ASSERT VALUE actual_amount = 21380.0 WHERE region = 'us-west'
ASSERT VALUE actual_amount = 18405.0 WHERE region = 'eu-west'
ASSERT VALUE actual_amount = 13293.0 WHERE region = 'ap-south'
ASSERT ROW_COUNT = 4
SELECT s.region,
       t.target_amount,
       ROUND(SUM(s.amount), 2) AS actual_amount,
       ROUND(SUM(s.amount) / t.target_amount * 100, 1) AS amount_pct,
       t.target_qty,
       SUM(s.qty) AS actual_qty,
       ROUND(SUM(s.qty) * 100.0 / t.target_qty, 1) AS qty_pct
FROM {{zone_name}}.delta_demos.sales_facts s
JOIN {{zone_name}}.delta_demos.region_targets t
    ON s.region = t.region
GROUP BY s.region, t.target_amount, t.target_qty
ORDER BY s.region;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 55 rows in sales_facts
ASSERT ROW_COUNT = 55
SELECT * FROM {{zone_name}}.delta_demos.sales_facts;

-- Verify region_count: 4 distinct regions
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT region) AS cnt FROM {{zone_name}}.delta_demos.sales_facts;

-- Verify us_east_count: 14 rows in us-east partition
ASSERT VALUE cnt = 14
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts WHERE region = 'us-east';

-- Verify ap_south_discounted: id=46 amount discounted to 468.0
ASSERT VALUE amount = 468.0
SELECT amount FROM {{zone_name}}.delta_demos.sales_facts WHERE id = 46;

-- Verify cancelled_gone: no rows with qty = 0
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts WHERE qty = 0;

-- Verify channel_count: 3 distinct channels
ASSERT VALUE cnt = 3
SELECT COUNT(DISTINCT channel) AS cnt FROM {{zone_name}}.delta_demos.sales_facts;

-- Verify quarterly_distribution: 16 rows in Q1-2024
ASSERT VALUE cnt = 16
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts WHERE quarter = 'Q1-2024';

-- Verify joined_result: 28 rows when joining with high-target regions
ASSERT VALUE cnt = 28
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts s JOIN {{zone_name}}.delta_demos.region_targets t ON s.region = t.region WHERE t.target_amount > 50000;

-- Verify subquery_result: 28 rows via IN-subquery pruning path
ASSERT VALUE cnt = 28
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts WHERE region IN (SELECT region FROM {{zone_name}}.delta_demos.region_targets WHERE target_qty >= 400);

-- Verify online_sales: 19 online rows across all partitions
ASSERT VALUE cnt = 19
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts WHERE channel = 'online';

-- Verify anti_join: same 28 rows via NOT IN exclusion path
ASSERT VALUE cnt = 28
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.sales_facts WHERE region NOT IN (SELECT region FROM {{zone_name}}.delta_demos.region_targets WHERE target_amount < 50000);
