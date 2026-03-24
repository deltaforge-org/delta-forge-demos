-- ============================================================================
-- Cross-Partition Updates — Educational Queries
-- ============================================================================
-- WHAT: When an UPDATE's WHERE clause doesn't match the partition key, Delta
--       must scan and write deletion vectors (DVs) in EVERY partition that
--       contains matching rows — not just one.
-- WHY:  Real-world billing changes (price increases, policy enforcement) are
--       business-logic predicates (plan, status, usage) that cut across the
--       partitioning dimension (region). This forces cross-partition DV writes.
-- HOW:  Each UPDATE marks old row versions as deleted via DV sidecar files
--       and writes new data files with updated values. OPTIMIZE later merges
--       the DVs back into compacted Parquet files.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Per-Region Plan Distribution & Average Fees
-- ============================================================================
-- The subscriptions table is partitioned by region (americas, europe,
-- asia-pacific). Each region has 20 subscriptions across three plans:
-- starter, professional, and enterprise. Let's see the starting state.

ASSERT ROW_COUNT = 3
ASSERT VALUE subscribers = 20 WHERE region = 'americas'
ASSERT VALUE subscribers = 20 WHERE region = 'europe'
ASSERT VALUE subscribers = 20 WHERE region = 'asia-pacific'
SELECT region,
       COUNT(*) AS subscribers,
       COUNT(*) FILTER (WHERE plan = 'starter') AS starter,
       COUNT(*) FILTER (WHERE plan = 'professional') AS professional,
       COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise,
       ROUND(AVG(monthly_fee), 2) AS avg_fee
FROM {{zone_name}}.delta_demos.subscriptions
GROUP BY region
ORDER BY region;


-- Average monthly fee per plan before any changes:
ASSERT ROW_COUNT = 3
ASSERT VALUE avg_fee = 38.74 WHERE plan = 'starter'
ASSERT VALUE avg_fee = 121.89 WHERE plan = 'professional'
ASSERT VALUE avg_fee = 389.99 WHERE plan = 'enterprise'
SELECT plan,
       COUNT(*) AS subscribers,
       ROUND(AVG(monthly_fee), 2) AS avg_fee,
       MIN(monthly_fee) AS min_fee,
       MAX(monthly_fee) AS max_fee
FROM {{zone_name}}.delta_demos.subscriptions
GROUP BY plan
ORDER BY avg_fee;


-- ============================================================================
-- STEP 1: Cross-Partition UPDATE — 10% Price Increase for Professional Plans
-- ============================================================================
-- A company-wide pricing change: all 'professional' plan subscribers get a
-- 10% fee increase regardless of region. The predicate (plan = 'professional')
-- does NOT align with the partition key (region), so Delta must write DVs in
-- ALL THREE partitions — americas, europe, and asia-pacific.
--
-- Under the hood, each partition's Parquet files are scanned for matching
-- rows. Old row versions are marked deleted via DV sidecar files, and new
-- data files are written with the updated monthly_fee.

ASSERT ROW_COUNT = 21
UPDATE {{zone_name}}.delta_demos.subscriptions
SET monthly_fee = ROUND(monthly_fee * 1.10, 2)
WHERE plan = 'professional';


-- ============================================================================
-- LEARN: Verify the Update Hit All 3 Regions
-- ============================================================================
-- The cross-partition nature is the key insight: 7 professional subscribers
-- per region, all 21 updated. DVs were written in every partition directory.

ASSERT ROW_COUNT = 3
ASSERT VALUE updated_count = 7 WHERE region = 'americas'
ASSERT VALUE updated_count = 7 WHERE region = 'europe'
ASSERT VALUE updated_count = 7 WHERE region = 'asia-pacific'
ASSERT VALUE avg_new_fee = 133.56 WHERE region = 'americas'
ASSERT VALUE avg_new_fee = 135.13 WHERE region = 'europe'
ASSERT VALUE avg_new_fee = 133.56 WHERE region = 'asia-pacific'
SELECT region,
       COUNT(*) AS updated_count,
       ROUND(AVG(monthly_fee), 2) AS avg_new_fee,
       MIN(monthly_fee) AS min_new_fee,
       MAX(monthly_fee) AS max_new_fee
FROM {{zone_name}}.delta_demos.subscriptions
WHERE plan = 'professional'
GROUP BY region
ORDER BY region;

-- Spot-check: id=8 (HyperScale Labs) was 149.99, now 164.99
ASSERT VALUE monthly_fee = 164.99
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 8;

-- Spot-check: id=42 (BangkokCloud) was 99.99, now 109.99
ASSERT VALUE monthly_fee = 109.99
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 42;


-- ============================================================================
-- STEP 2: Cross-Partition UPDATE — Suspend High-Usage Trial Accounts
-- ============================================================================
-- Policy enforcement: trial accounts consuming more than 100 GB are
-- automatically suspended. Again, the predicate (status = 'trial' AND
-- usage_gb > 100) cuts across all regions, creating DVs in every partition.

ASSERT ROW_COUNT = 6
UPDATE {{zone_name}}.delta_demos.subscriptions
SET status = 'suspended'
WHERE status = 'trial' AND usage_gb > 100;


-- ============================================================================
-- LEARN: Verify Suspensions Hit Multiple Regions
-- ============================================================================
-- 6 trial accounts exceeded 100 GB: 2 per region. This confirms the update
-- was truly cross-partition — DVs written in americas, europe, AND asia-pacific.

ASSERT ROW_COUNT = 3
ASSERT VALUE suspended_count = 3 WHERE region = 'americas'
ASSERT VALUE suspended_count = 3 WHERE region = 'europe'
ASSERT VALUE suspended_count = 3 WHERE region = 'asia-pacific'
ASSERT VALUE remaining_trials = 2 WHERE region = 'americas'
ASSERT VALUE remaining_trials = 2 WHERE region = 'europe'
ASSERT VALUE remaining_trials = 1 WHERE region = 'asia-pacific'
ASSERT VALUE active_count = 15 WHERE region = 'americas'
ASSERT VALUE active_count = 15 WHERE region = 'europe'
ASSERT VALUE active_count = 16 WHERE region = 'asia-pacific'
SELECT region,
       COUNT(*) FILTER (WHERE status = 'suspended') AS suspended_count,
       COUNT(*) FILTER (WHERE status = 'trial') AS remaining_trials,
       COUNT(*) FILTER (WHERE status = 'active') AS active_count
FROM {{zone_name}}.delta_demos.subscriptions
GROUP BY region
ORDER BY region;

-- Show the 6 newly suspended accounts
ASSERT ROW_COUNT = 6
SELECT id, customer, region, usage_gb
FROM {{zone_name}}.delta_demos.subscriptions
WHERE status = 'suspended' AND plan = 'starter'
ORDER BY region, id;


-- ============================================================================
-- STEP 3: Partition-Aligned UPDATE — 5% Discount for Asia-Pacific Enterprise
-- ============================================================================
-- Contrast: this update targets enterprise plans in asia-pacific ONLY.
-- Because the WHERE clause includes the partition key (region = 'asia-pacific'),
-- Delta only needs to scan and write DVs in ONE partition directory. The
-- americas and europe partitions are completely untouched.

ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.delta_demos.subscriptions
SET monthly_fee = ROUND(monthly_fee * 0.95, 2)
WHERE plan = 'enterprise' AND region = 'asia-pacific';


-- ============================================================================
-- LEARN: Confirm Only One Partition Was Affected
-- ============================================================================
-- Asia-Pacific enterprise fees decreased. Americas and Europe are unchanged.

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_ent_fee = 399.99 WHERE region = 'americas'
ASSERT VALUE avg_ent_fee = 369.99 WHERE region = 'europe'
ASSERT VALUE avg_ent_fee = 379.99 WHERE region = 'asia-pacific'
ASSERT VALUE enterprise_count = 5 WHERE region = 'americas'
ASSERT VALUE enterprise_count = 5 WHERE region = 'europe'
ASSERT VALUE enterprise_count = 5 WHERE region = 'asia-pacific'
SELECT region,
       ROUND(AVG(monthly_fee) FILTER (WHERE plan = 'enterprise'), 2) AS avg_ent_fee,
       COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_count
FROM {{zone_name}}.delta_demos.subscriptions
GROUP BY region
ORDER BY region;

-- Spot-check: id=43 (ChennaiByte) was 299.99, now 284.99
ASSERT VALUE monthly_fee = 284.99
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 43;

-- Spot-check: id=3 (Cloud Nine, americas) is UNCHANGED at 299.99
ASSERT VALUE monthly_fee = 299.99
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 3;


-- ============================================================================
-- STEP 4: OPTIMIZE — Materialize All Accumulated Deletion Vectors
-- ============================================================================
-- Three rounds of UPDATEs have left DV sidecar files scattered across all
-- three partitions. OPTIMIZE compacts data files and physically removes
-- rows marked as deleted by the DVs. After this, readers no longer need
-- to apply DV filters, improving scan performance.

OPTIMIZE {{zone_name}}.delta_demos.subscriptions;


-- ============================================================================
-- EXPLORE: Final State — Per-Region Revenue Summary
-- ============================================================================
-- After all pricing changes and account suspensions, let's see the final
-- revenue picture across all regions.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_revenue = 3239.80 WHERE region = 'americas'
ASSERT VALUE total_revenue = 3110.80 WHERE region = 'europe'
ASSERT VALUE total_revenue = 3144.80 WHERE region = 'asia-pacific'
ASSERT VALUE active = 15 WHERE region = 'americas'
ASSERT VALUE active = 15 WHERE region = 'europe'
ASSERT VALUE active = 16 WHERE region = 'asia-pacific'
ASSERT VALUE suspended = 3 WHERE region = 'americas'
ASSERT VALUE suspended = 3 WHERE region = 'europe'
ASSERT VALUE suspended = 3 WHERE region = 'asia-pacific'
SELECT region,
       COUNT(*) AS subscribers,
       SUM(monthly_fee) AS total_revenue,
       ROUND(AVG(monthly_fee), 2) AS avg_fee,
       COUNT(*) FILTER (WHERE status = 'active') AS active,
       COUNT(*) FILTER (WHERE status = 'suspended') AS suspended,
       COUNT(*) FILTER (WHERE status = 'trial') AS trial
FROM {{zone_name}}.delta_demos.subscriptions
GROUP BY region
ORDER BY region;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 60 rows (no inserts or deletes, only updates)
ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.delta_demos.subscriptions;

-- Verify region_counts: 20 per region (unchanged by updates)
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.subscriptions WHERE region = 'americas';

ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.subscriptions WHERE region = 'europe';

ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.subscriptions WHERE region = 'asia-pacific';

-- Verify professional_fee_increase: id=5 was 129.99, now 142.99 (+10%)
ASSERT VALUE monthly_fee = 142.99
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 5;

-- Verify professional_fee_increase: id=25 was 139.99, now 153.99 (+10%)
ASSERT VALUE monthly_fee = 153.99
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 25;

-- Verify professional_fee_increase: id=50 was 139.99, now 153.99 (+10%)
ASSERT VALUE monthly_fee = 153.99
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 50;

-- Verify trial_suspension: 6 trials suspended (usage > 100gb)
ASSERT VALUE cnt = 9
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.subscriptions WHERE status = 'suspended';

-- Verify remaining_trials: 5 trials remain (usage <= 100gb)
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.subscriptions WHERE status = 'trial';

-- Verify active_count: 46 active subscriptions
ASSERT VALUE cnt = 46
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.subscriptions WHERE status = 'active';

-- Verify asia_pacific_discount: id=47 was 449.99, now 427.49 (-5%)
ASSERT VALUE monthly_fee = 427.49
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 47;

-- Verify americas_enterprise_unchanged: id=7 still 399.99
ASSERT VALUE monthly_fee = 399.99
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 7;

-- Verify europe_enterprise_unchanged: id=27 still 399.99
ASSERT VALUE monthly_fee = 399.99
SELECT monthly_fee FROM {{zone_name}}.delta_demos.subscriptions WHERE id = 27;

-- Verify grand_total_revenue: sum of all monthly fees
ASSERT VALUE total = 9495.40
SELECT SUM(monthly_fee) AS total FROM {{zone_name}}.delta_demos.subscriptions;

-- Verify distinct_regions: still 3
ASSERT VALUE cnt = 3
SELECT COUNT(DISTINCT region) AS cnt FROM {{zone_name}}.delta_demos.subscriptions;
