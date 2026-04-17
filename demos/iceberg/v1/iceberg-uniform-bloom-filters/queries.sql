-- ============================================================================
-- Demo: Customer Loyalty Program — Bloom Filters with UniForm
-- ============================================================================
-- Tests that BLOOM FILTER COLUMNS (member_id, full_name) are correctly
-- maintained alongside UniForm Iceberg metadata. Point lookups on bloom
-- filter columns enable efficient data skipping.

-- ============================================================================
-- Query 1: Baseline — All 40 Members Present
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.iceberg_demos.members ORDER BY member_id;

-- ============================================================================
-- Query 2: Per-Tier Summary
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE member_count = 11 WHERE tier = 'Bronze'
ASSERT VALUE total_points = 12500 WHERE tier = 'Bronze'
ASSERT VALUE member_count = 10 WHERE tier = 'Silver'
ASSERT VALUE total_points = 48500 WHERE tier = 'Silver'
ASSERT VALUE member_count = 10 WHERE tier = 'Gold'
ASSERT VALUE total_points = 136500 WHERE tier = 'Gold'
ASSERT VALUE member_count = 9 WHERE tier = 'Platinum'
ASSERT VALUE total_points = 264000 WHERE tier = 'Platinum'
SELECT
    tier,
    COUNT(*) AS member_count,
    SUM(points) AS total_points,
    ROUND(SUM(lifetime_spend), 2) AS total_spend
FROM {{zone_name}}.iceberg_demos.members
GROUP BY tier
ORDER BY CASE tier
    WHEN 'Bronze' THEN 1
    WHEN 'Silver' THEN 2
    WHEN 'Gold' THEN 3
    WHEN 'Platinum' THEN 4
END;

-- ============================================================================
-- Query 3: Point Lookup by member_id — Bloom Filter Hit
-- ============================================================================
-- These lookups hit the bloom filter index on member_id, allowing the engine
-- to skip data files that definitely don't contain the target row.

ASSERT ROW_COUNT = 1
ASSERT VALUE full_name = 'David Garcia' WHERE member_id = 4
ASSERT VALUE tier = 'Platinum' WHERE member_id = 4
ASSERT VALUE points = 28000 WHERE member_id = 4
ASSERT VALUE lifetime_spend = 8500.00 WHERE member_id = 4
SELECT *
FROM {{zone_name}}.iceberg_demos.members
WHERE member_id = 4;

-- ============================================================================
-- Query 4: Point Lookup by full_name — Bloom Filter Hit
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE member_id = 24 WHERE full_name = 'Brian Wright'
ASSERT VALUE tier = 'Platinum' WHERE full_name = 'Brian Wright'
ASSERT VALUE points = 35000 WHERE full_name = 'Brian Wright'
ASSERT VALUE lifetime_spend = 10500.00 WHERE full_name = 'Brian Wright'
SELECT *
FROM {{zone_name}}.iceberg_demos.members
WHERE full_name = 'Brian Wright';

-- ============================================================================
-- Query 5: Multi-Row Lookup — Top Platinum Members
-- ============================================================================

ASSERT ROW_COUNT = 9
ASSERT VALUE lifetime_spend = 10500.00 WHERE member_id = 24
ASSERT VALUE lifetime_spend = 9800.00 WHERE member_id = 8
ASSERT VALUE lifetime_spend = 9500.25 WHERE member_id = 39
SELECT member_id, full_name, points, lifetime_spend
FROM {{zone_name}}.iceberg_demos.members
WHERE tier = 'Platinum'
ORDER BY lifetime_spend DESC;

-- ============================================================================
-- Query 6: Gold+ Members — Total Spend
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE gold_plus_count = 19
ASSERT VALUE gold_plus_spend = 119904.50
SELECT
    COUNT(*) AS gold_plus_count,
    ROUND(SUM(lifetime_spend), 2) AS gold_plus_spend
FROM {{zone_name}}.iceberg_demos.members
WHERE tier IN ('Gold', 'Platinum');

-- ============================================================================
-- Query 7: Points Distribution — Min/Max per Tier
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE min_points = 450 WHERE tier = 'Bronze'
ASSERT VALUE max_points = 2000 WHERE tier = 'Bronze'
ASSERT VALUE min_points = 3600 WHERE tier = 'Silver'
ASSERT VALUE max_points = 6100 WHERE tier = 'Silver'
ASSERT VALUE max_points = 17000 WHERE tier = 'Gold'
ASSERT VALUE max_points = 35000 WHERE tier = 'Platinum'
SELECT
    tier,
    MIN(points) AS min_points,
    MAX(points) AS max_points,
    ROUND(AVG(points), 2) AS avg_points
FROM {{zone_name}}.iceberg_demos.members
GROUP BY tier
ORDER BY CASE tier
    WHEN 'Bronze' THEN 1
    WHEN 'Silver' THEN 2
    WHEN 'Gold' THEN 3
    WHEN 'Platinum' THEN 4
END;

-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.members_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.members_iceberg
USING ICEBERG
LOCATION '{{data_path}}/members';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.members_iceberg TO USER {{current_user}};

-- ============================================================================
-- Iceberg Verify 1: Row Count
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.iceberg_demos.members_iceberg ORDER BY member_id;

-- ============================================================================
-- Iceberg Verify 2: Per-Tier Totals — Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE total_points = 264000 WHERE tier = 'Platinum'
ASSERT VALUE total_points = 136500 WHERE tier = 'Gold'
SELECT
    tier,
    COUNT(*) AS member_count,
    SUM(points) AS total_points
FROM {{zone_name}}.iceberg_demos.members_iceberg
GROUP BY tier
ORDER BY total_points DESC;

-- ============================================================================
-- Iceberg Verify 3: Point Lookup — Bloom Filter Member via Iceberg
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE full_name = 'James Wilson' WHERE member_id = 8
ASSERT VALUE points = 32000 WHERE member_id = 8
ASSERT VALUE lifetime_spend = 9800.00 WHERE member_id = 8
SELECT *
FROM {{zone_name}}.iceberg_demos.members_iceberg
WHERE member_id = 8;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_members = 40
ASSERT VALUE total_points = 461500
ASSERT VALUE total_spend = 136910.00
ASSERT VALUE tier_count = 4
ASSERT VALUE avg_points = 11537.50
ASSERT VALUE max_spend = 10500.00
ASSERT VALUE min_spend = 130.00
SELECT
    COUNT(*) AS total_members,
    SUM(points) AS total_points,
    ROUND(SUM(lifetime_spend), 2) AS total_spend,
    COUNT(DISTINCT tier) AS tier_count,
    ROUND(AVG(points), 2) AS avg_points,
    MAX(lifetime_spend) AS max_spend,
    MIN(lifetime_spend) AS min_spend
FROM {{zone_name}}.iceberg_demos.members;
