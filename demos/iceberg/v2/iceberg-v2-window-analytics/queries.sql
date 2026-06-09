-- ============================================================================
-- Iceberg V2 — Airline Loyalty Window Analytics — Queries
-- ============================================================================
-- Demonstrates advanced window function analytics on a native Iceberg V2
-- table. All queries are read-only — the table contains 60 frequent flyer
-- members across 4 tiers and 5 airports. Each query exercises a different
-- window function pattern, proving DeltaForge handles complex analytical
-- SQL on Iceberg data without conversion.
-- ============================================================================


-- ============================================================================
-- Query 1: Full Table Scan — Baseline
-- ============================================================================
-- 60 members: 20 Bronze, 18 Silver, 14 Gold, 8 Platinum across 5 airports.

ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.iceberg_demos.loyalty_members
ORDER BY member_id;


-- ============================================================================
-- Query 2: Per-Tier Breakdown
-- ============================================================================
-- Verifies tier distribution and aggregate stats.

ASSERT ROW_COUNT = 4
ASSERT VALUE cnt = 20 WHERE tier = 'Bronze'
ASSERT VALUE cnt = 18 WHERE tier = 'Silver'
ASSERT VALUE cnt = 14 WHERE tier = 'Gold'
ASSERT VALUE cnt = 8 WHERE tier = 'Platinum'
ASSERT VALUE total_spend = 294131.32 WHERE tier = 'Platinum'
SELECT
    tier,
    COUNT(*) AS cnt,
    SUM(miles_ytd) AS total_miles,
    SUM(flights_ytd) AS total_flights,
    ROUND(SUM(spend_ytd), 2) AS total_spend,
    ROUND(AVG(spend_ytd), 2) AS avg_spend
FROM {{zone_name}}.iceberg_demos.loyalty_members
GROUP BY tier
ORDER BY tier;


-- ============================================================================
-- Query 3: ROW_NUMBER — Overall Ranking by Miles Flown
-- ============================================================================
-- Ranks all 60 members by miles_ytd descending. Top member is Leo Park
-- (Platinum, 198664 miles). Verifies correct ordering across tiers.

ASSERT ROW_COUNT = 60
ASSERT VALUE overall_rank = 1 WHERE member_id = 55
ASSERT VALUE member_name = 'Leo Park' WHERE overall_rank = 1
ASSERT VALUE miles_ytd = 198664 WHERE overall_rank = 1
ASSERT VALUE miles_ytd = 181072 WHERE overall_rank = 2
ASSERT VALUE miles_ytd = 174058 WHERE overall_rank = 3
SELECT
    member_id,
    member_name,
    tier,
    miles_ytd,
    ROW_NUMBER() OVER (ORDER BY miles_ytd DESC) AS overall_rank
FROM {{zone_name}}.iceberg_demos.loyalty_members
ORDER BY overall_rank;


-- ============================================================================
-- Query 4: RANK — Top Spender Per Tier
-- ============================================================================
-- Uses RANK() OVER (PARTITION BY tier) to find the highest spender in
-- each tier. CTE filters to rank=1 only.

ASSERT ROW_COUNT = 4
ASSERT VALUE spend_ytd = 2992.51 WHERE tier = 'Bronze'
ASSERT VALUE spend_ytd = 7975.75 WHERE tier = 'Silver'
ASSERT VALUE spend_ytd = 17193.87 WHERE tier = 'Gold'
ASSERT VALUE spend_ytd = 49162.78 WHERE tier = 'Platinum'
WITH ranked AS (
    SELECT
        member_id,
        member_name,
        tier,
        spend_ytd,
        RANK() OVER (PARTITION BY tier ORDER BY spend_ytd DESC) AS spend_rank
    FROM {{zone_name}}.iceberg_demos.loyalty_members
)
SELECT member_id, member_name, tier, spend_ytd
FROM ranked
WHERE spend_rank = 1
ORDER BY tier;


-- ============================================================================
-- Query 5: NTILE(4) — Spend Quartile Assignment
-- ============================================================================
-- Divides all 60 members into 4 equal quartiles by spend. Each quartile
-- gets exactly 15 members.

ASSERT ROW_COUNT = 4
ASSERT VALUE cnt = 15 WHERE quartile = 1
ASSERT VALUE cnt = 15 WHERE quartile = 2
ASSERT VALUE cnt = 15 WHERE quartile = 3
ASSERT VALUE cnt = 15 WHERE quartile = 4
ASSERT VALUE min_spend = 270.03 WHERE quartile = 1
ASSERT VALUE max_spend = 49162.78 WHERE quartile = 4
WITH quartiled AS (
    SELECT
        member_id,
        spend_ytd,
        NTILE(4) OVER (ORDER BY spend_ytd) AS quartile
    FROM {{zone_name}}.iceberg_demos.loyalty_members
)
SELECT
    quartile,
    COUNT(*) AS cnt,
    ROUND(MIN(spend_ytd), 2) AS min_spend,
    ROUND(MAX(spend_ytd), 2) AS max_spend
FROM quartiled
GROUP BY quartile
ORDER BY quartile;


-- ============================================================================
-- Query 6: LAG/LEAD — Adjacent Member Comparison by Miles
-- ============================================================================
-- For the top 5 members by miles, show the member ranked above (LAG)
-- and below (LEAD). The top member has no LAG (NULL prev_miles).

ASSERT ROW_COUNT = 5
ASSERT VALUE prev_miles IS NULL WHERE member_id = 55
ASSERT VALUE next_miles = 181072 WHERE member_id = 55
ASSERT VALUE prev_miles = 198664 WHERE member_id = 56
WITH ordered AS (
    SELECT
        member_id,
        member_name,
        miles_ytd,
        LAG(miles_ytd, 1) OVER (ORDER BY miles_ytd DESC) AS prev_miles,
        LEAD(miles_ytd, 1) OVER (ORDER BY miles_ytd DESC) AS next_miles
    FROM {{zone_name}}.iceberg_demos.loyalty_members
)
SELECT *
FROM ordered
ORDER BY miles_ytd DESC
LIMIT 5;


-- ============================================================================
-- Query 7: Running SUM — Platinum Tier Cumulative Miles
-- ============================================================================
-- Running total of miles within the Platinum tier, ordered from lowest
-- to highest. The final running total equals the Platinum total (1160290).

ASSERT ROW_COUNT = 8
ASSERT VALUE running_total = 84722 WHERE member_id = 58
ASSERT VALUE running_total = 1160290 WHERE member_id = 55
SELECT
    member_id,
    member_name,
    miles_ytd,
    SUM(miles_ytd) OVER (ORDER BY miles_ytd ROWS UNBOUNDED PRECEDING) AS running_total
FROM {{zone_name}}.iceberg_demos.loyalty_members
WHERE tier = 'Platinum'
ORDER BY miles_ytd;


-- ============================================================================
-- Query 8: CTE + ROW_NUMBER — Top 3 Spenders Per Airport
-- ============================================================================
-- Partitioned ROW_NUMBER finds the 3 highest spenders at each of the
-- 5 airports. Returns exactly 15 rows (3 per airport).

ASSERT ROW_COUNT = 15
ASSERT VALUE spend_ytd = 41757.33 WHERE home_airport = 'ATL' AND rn = 1
ASSERT VALUE spend_ytd = 46765.59 WHERE home_airport = 'DFW' AND rn = 1
ASSERT VALUE spend_ytd = 49162.78 WHERE home_airport = 'JFK' AND rn = 1
ASSERT VALUE spend_ytd = 38387.63 WHERE home_airport = 'LAX' AND rn = 1
ASSERT VALUE spend_ytd = 35949.02 WHERE home_airport = 'ORD' AND rn = 1
WITH ranked AS (
    SELECT
        member_id,
        member_name,
        home_airport,
        tier,
        spend_ytd,
        ROW_NUMBER() OVER (PARTITION BY home_airport ORDER BY spend_ytd DESC) AS rn
    FROM {{zone_name}}.iceberg_demos.loyalty_members
)
SELECT member_id, member_name, home_airport, tier, spend_ytd, rn
FROM ranked
WHERE rn <= 3
ORDER BY home_airport, rn;


-- ============================================================================
-- Query 9: Per-Airport Aggregation
-- ============================================================================
-- Verifies balanced distribution: 12 members per airport.

ASSERT ROW_COUNT = 5
ASSERT VALUE cnt = 12 WHERE home_airport = 'ATL'
ASSERT VALUE cnt = 12 WHERE home_airport = 'DFW'
ASSERT VALUE cnt = 12 WHERE home_airport = 'JFK'
ASSERT VALUE cnt = 12 WHERE home_airport = 'LAX'
ASSERT VALUE cnt = 12 WHERE home_airport = 'ORD'
SELECT
    home_airport,
    COUNT(*) AS cnt,
    ROUND(SUM(spend_ytd), 2) AS total_spend
FROM {{zone_name}}.iceberg_demos.loyalty_members
GROUP BY home_airport
ORDER BY home_airport;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check combining all key invariants.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 60
ASSERT VALUE total_miles = 2685394
ASSERT VALUE total_flights = 1682
ASSERT VALUE total_spend = 615209.16
ASSERT VALUE tier_count = 4
ASSERT VALUE airport_count = 5
ASSERT VALUE avg_spend = 10253.49
SELECT
    COUNT(*) AS total_rows,
    SUM(miles_ytd) AS total_miles,
    SUM(flights_ytd) AS total_flights,
    ROUND(SUM(spend_ytd), 2) AS total_spend,
    COUNT(DISTINCT tier) AS tier_count,
    COUNT(DISTINCT home_airport) AS airport_count,
    ROUND(AVG(spend_ytd), 2) AS avg_spend
FROM {{zone_name}}.iceberg_demos.loyalty_members;
