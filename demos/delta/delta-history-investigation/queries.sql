-- ============================================================================
-- Delta DESCRIBE HISTORY — Root-Cause Investigation — Educational Queries
-- ============================================================================
-- WHAT: Uses DESCRIBE HISTORY and VERSION AS OF as a diagnostic workflow
--       to find when and why data was corrupted.
-- WHY:  When a dashboard shows wrong numbers, the first question is "what
--       changed and when?" Delta's transaction log answers both questions
--       without modifying the table or guessing.
-- HOW:  DESCRIBE HISTORY lists every commit. VERSION AS OF lets you compare
--       any two versions to isolate the exact commit that introduced bad data.
-- ============================================================================


-- ============================================================================
-- DETECT: The Dashboard Looks Wrong — Americas Revenue Seems Too High
-- ============================================================================
-- The VP of Sales notices Americas revenue is disproportionately high
-- compared to other regions. Let's pull the current regional totals.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_revenue = 324000.0 WHERE region = 'Americas'
ASSERT VALUE total_revenue = 143700.0 WHERE region = 'EMEA'
ASSERT VALUE total_revenue = 70100.0 WHERE region = 'Asia-Pacific'
SELECT region,
       COUNT(*) AS records,
       ROUND(SUM(revenue), 2) AS total_revenue,
       SUM(active_users) AS total_users
FROM {{zone_name}}.delta_demos.product_metrics
GROUP BY region
ORDER BY total_revenue DESC;


-- ============================================================================
-- INVESTIGATE: Check the Transaction Log
-- ============================================================================
-- DESCRIBE HISTORY shows every commit to this table. We need to find which
-- version introduced the anomaly. Look for UPDATE operations — one of them
-- may have applied a wrong transformation.

-- Non-deterministic: commit timestamps are set at write time
ASSERT WARNING ROW_COUNT >= 6
DESCRIBE HISTORY {{zone_name}}.delta_demos.product_metrics;


-- ============================================================================
-- NARROW: Find the Last Known-Good State
-- ============================================================================
-- V3 was the last INSERT of correct data. Let's check if the regional
-- totals look reasonable at that version. If Americas was already high
-- at V3, the problem predates V4.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_revenue = 149500.0 WHERE region = 'Americas'
ASSERT VALUE total_revenue = 125900.0 WHERE region = 'EMEA'
ASSERT VALUE total_revenue = 56700.0 WHERE region = 'Asia-Pacific'
SELECT region,
       ROUND(SUM(revenue), 2) AS total_revenue,
       SUM(active_users) AS total_users
FROM {{zone_name}}.delta_demos.product_metrics VERSION AS OF 3
GROUP BY region
ORDER BY total_revenue DESC;


-- ============================================================================
-- PINPOINT: Compare V3 vs V4 — The Smoking Gun
-- ============================================================================
-- Americas revenue at V3 was 149,500. At V4 it jumped to 299,000 — exactly
-- 2.0x. That is not a coincidence. Someone doubled the Americas revenue
-- between V3 and V4. EMEA and Asia-Pacific are unchanged.

ASSERT ROW_COUNT = 2
ASSERT VALUE americas_revenue = 149500.0 WHERE version = 'V3 (before)'
ASSERT VALUE americas_revenue = 299000.0 WHERE version = 'V4 (after)'
SELECT 'V3 (before)' AS version, ROUND(SUM(revenue), 2) AS americas_revenue
FROM {{zone_name}}.delta_demos.product_metrics VERSION AS OF 3
WHERE region = 'Americas'
UNION ALL
SELECT 'V4 (after)', ROUND(SUM(revenue), 2)
FROM {{zone_name}}.delta_demos.product_metrics VERSION AS OF 4
WHERE region = 'Americas';


-- ============================================================================
-- ISOLATE: Which Records Were Damaged?
-- ============================================================================
-- Compare each Americas record's current revenue against its V3 value.
-- Every record that existed at V3 should show exactly 2x its correct value.

ASSERT ROW_COUNT = 10
ASSERT VALUE current_revenue = 25000.0 WHERE id = 1
ASSERT VALUE correct_revenue = 12500.0 WHERE id = 1
ASSERT VALUE current_revenue = 48000.0 WHERE id = 30
ASSERT VALUE correct_revenue = 24000.0 WHERE id = 30
SELECT c.id, c.product, c.revenue AS current_revenue,
       v3.revenue AS correct_revenue
FROM {{zone_name}}.delta_demos.product_metrics c
JOIN {{zone_name}}.delta_demos.product_metrics VERSION AS OF 3 v3
  ON c.id = v3.id
WHERE c.region = 'Americas' AND c.id <= 30
ORDER BY c.id;


-- ============================================================================
-- QUANTIFY: Total Revenue Inflation
-- ============================================================================
-- The bad update inflated total revenue by exactly the Americas sum at V3
-- (149,500). The current total includes V5's new records too, so we compare
-- V3 total + expected V5 additions against the actual current total.

ASSERT ROW_COUNT = 1
ASSERT VALUE v3_total = 332100.0
ASSERT VALUE current_total = 537800.0
ASSERT VALUE inflation = 149500.0
SELECT ROUND(v3.total, 2) AS v3_total,
       ROUND(cur.total, 2) AS current_total,
       ROUND(cur.total - v3.total - 56200.0, 2) AS inflation
FROM (SELECT SUM(revenue) AS total
      FROM {{zone_name}}.delta_demos.product_metrics VERSION AS OF 3) v3,
     (SELECT SUM(revenue) AS total
      FROM {{zone_name}}.delta_demos.product_metrics) cur;


-- ============================================================================
-- IMPACT: Which Products Were Most Affected?
-- ============================================================================
-- Break down the inflated Americas revenue by product to assess which
-- product dashboards are showing the worst distortion.

ASSERT ROW_COUNT = 4
ASSERT VALUE current_revenue = 164000.0 WHERE product = 'DataVault'
ASSERT VALUE current_revenue = 78000.0 WHERE product = 'CloudSync'
ASSERT VALUE current_revenue = 46000.0 WHERE product = 'PipelineX'
ASSERT VALUE current_revenue = 36000.0 WHERE product = 'FlowEngine'
SELECT product,
       ROUND(SUM(revenue), 2) AS current_revenue
FROM {{zone_name}}.delta_demos.product_metrics
WHERE region = 'Americas'
GROUP BY product
ORDER BY current_revenue DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total rows: 35
ASSERT VALUE total = 35
SELECT COUNT(*) AS total FROM {{zone_name}}.delta_demos.product_metrics;

-- Verify 4 products
ASSERT VALUE product_count = 4
SELECT COUNT(DISTINCT product) AS product_count FROM {{zone_name}}.delta_demos.product_metrics;

-- Verify 3 regions (Americas, EMEA, Asia-Pacific — no APAC remaining)
ASSERT VALUE region_count = 3
SELECT COUNT(DISTINCT region) AS region_count FROM {{zone_name}}.delta_demos.product_metrics;

-- Verify no APAC records remain (all renamed to Asia-Pacific)
ASSERT VALUE apac_count = 0
SELECT COUNT(*) FILTER (WHERE region = 'APAC') AS apac_count FROM {{zone_name}}.delta_demos.product_metrics;

-- Verify 11 Americas records
ASSERT VALUE americas_count = 11
SELECT COUNT(*) AS americas_count FROM {{zone_name}}.delta_demos.product_metrics WHERE region = 'Americas';

-- Verify V3 had correct baseline: 30 rows
ASSERT VALUE v3_rows = 30
SELECT COUNT(*) AS v3_rows FROM {{zone_name}}.delta_demos.product_metrics VERSION AS OF 3;
