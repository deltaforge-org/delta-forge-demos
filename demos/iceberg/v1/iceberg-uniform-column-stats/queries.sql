-- ============================================================================
-- Iceberg UniForm Column-Level Statistics — Queries
-- ============================================================================
-- HOW UNIFORM WORKS
-- -----------------
-- All queries below read through the Delta transaction log — standard
-- Delta Forge behaviour. The Iceberg metadata in metadata/ is generated
-- automatically by the post-commit hook and is never read by these queries.
--
-- Each DML operation (INSERT, UPDATE, DELETE) creates:
--   1. A new Delta version in _delta_log/  (what these queries read)
--   2. A new Iceberg snapshot in metadata/ (for external Iceberg engines)
--
-- Column-level statistics (min, max, null count) are stored in Iceberg
-- manifest files and drive data-skipping optimizations in query planners.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify each Iceberg snapshot with:
--   python3 verify_iceberg_metadata.py <table_data_path>/ad_clicks -v
-- ============================================================================
-- ============================================================================
-- EXPLORE: Baseline State (Version 1 / Snapshot 1)
-- ============================================================================
-- 30 ad clicks across 3 campaigns, 4 device types. 14 clicks have NULL
-- conversion_value (non-converted clicks).

ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.iceberg_demos.ad_clicks ORDER BY click_id;
-- ============================================================================
-- Query 1: Per-Campaign Breakdown — Version 1
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE click_count = 10 WHERE campaign_id = 'summer-sale'
ASSERT VALUE click_count = 10 WHERE campaign_id = 'back-to-school'
ASSERT VALUE click_count = 10 WHERE campaign_id = 'holiday-promo'
SELECT
    campaign_id,
    COUNT(*) AS click_count
FROM {{zone_name}}.iceberg_demos.ad_clicks
GROUP BY campaign_id
ORDER BY campaign_id;
-- ============================================================================
-- Query 2: Per-Device Breakdown — Version 1
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE click_count = 9  WHERE device_type = 'desktop'
ASSERT VALUE click_count = 11 WHERE device_type = 'mobile'
ASSERT VALUE click_count = 4  WHERE device_type = 'smart-tv'
ASSERT VALUE click_count = 6  WHERE device_type = 'tablet'
SELECT
    device_type,
    COUNT(*) AS click_count
FROM {{zone_name}}.iceberg_demos.ad_clicks
GROUP BY device_type
ORDER BY device_type;
-- ============================================================================
-- Query 3: Baseline Column Statistics
-- ============================================================================
-- These are the metrics that Iceberg manifest files track per column.
-- MIN/MAX for cost_per_click, MIN/MAX for conversion_value, NULL counts.

ASSERT ROW_COUNT = 1
ASSERT VALUE min_cpc = 0.45
ASSERT VALUE max_cpc = 4.0
ASSERT VALUE min_cv = 8.75
ASSERT VALUE max_cv = 55.0
ASSERT VALUE null_cv_count = 14
ASSERT VALUE nonnull_cv_count = 16
ASSERT VALUE avg_cpc = 1.74
SELECT
    ROUND(MIN(cost_per_click), 2) AS min_cpc,
    ROUND(MAX(cost_per_click), 2) AS max_cpc,
    ROUND(MIN(conversion_value), 2) AS min_cv,
    ROUND(MAX(conversion_value), 2) AS max_cv,
    COUNT(*) FILTER (WHERE conversion_value IS NULL) AS null_cv_count,
    COUNT(*) FILTER (WHERE conversion_value IS NOT NULL) AS nonnull_cv_count,
    ROUND(AVG(cost_per_click), 2) AS avg_cpc
FROM {{zone_name}}.iceberg_demos.ad_clicks;
-- ============================================================================
-- LEARN: INSERT — New Clicks With Extreme Values (Version 2 / Snapshot 2)
-- ============================================================================
-- Insert 5 new clicks. Two have extreme cost_per_click values (0.10 and 6.00)
-- that will shift the MIN/MAX boundaries. One has a tiny conversion_value
-- (0.50) and another has a large one (150.00). Two have NULL conversion_value.

INSERT INTO {{zone_name}}.iceberg_demos.ad_clicks VALUES
    (31, 'summer-sale',    'search-brand',     '2025-06-06 10:00:00', '2025-06-06 10:01:00', 0.15,  NULL,    'mobile',   'US', false),
    (32, 'back-to-school', 'display-retarget', '2025-07-20 14:00:00', '2025-07-20 14:01:00', 5.50,  120.00,  'desktop',  'UK', true),
    (33, 'holiday-promo',  'video-pre-roll',   '2025-11-25 08:00:00', '2025-11-25 08:01:00', 4.25,  NULL,    'tablet',   'DE', false),
    (34, 'holiday-promo',  'shopping',         '2025-11-25 12:00:00', '2025-11-25 12:01:00', 0.10,  0.50,    'smart-tv', 'CA', true),
    (35, 'summer-sale',    'search-generic',   '2025-06-07 09:00:00', '2025-06-07 09:01:00', 6.00,  150.00,  'desktop',  'FR', true);
-- ============================================================================
-- Query 4: Row Count After INSERT
-- ============================================================================

ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.iceberg_demos.ad_clicks ORDER BY click_id;
-- ============================================================================
-- Query 5: Re-Verify Column Statistics After INSERT
-- ============================================================================
-- The new extreme values should shift the boundaries:
--   MIN cpc: 0.45 → 0.10 (click 34)
--   MAX cpc: 4.00 → 6.00 (click 35)
--   MIN cv:  8.75 → 0.50 (click 34)
--   MAX cv:  55.00 → 150.00 (click 35)
--   NULL cv: 14 → 16 (added 2 NULLs from clicks 31, 33)

ASSERT ROW_COUNT = 1
ASSERT VALUE min_cpc = 0.1
ASSERT VALUE max_cpc = 6.0
ASSERT VALUE min_cv = 0.5
ASSERT VALUE max_cv = 150.0
ASSERT VALUE null_cv_count = 16
ASSERT VALUE nonnull_cv_count = 19
SELECT
    ROUND(MIN(cost_per_click), 2) AS min_cpc,
    ROUND(MAX(cost_per_click), 2) AS max_cpc,
    ROUND(MIN(conversion_value), 2) AS min_cv,
    ROUND(MAX(conversion_value), 2) AS max_cv,
    COUNT(*) FILTER (WHERE conversion_value IS NULL) AS null_cv_count,
    COUNT(*) FILTER (WHERE conversion_value IS NOT NULL) AS nonnull_cv_count
FROM {{zone_name}}.iceberg_demos.ad_clicks;
-- ============================================================================
-- LEARN: UPDATE — Late Conversions (Version 3 / Snapshot 3)
-- ============================================================================
-- Three clicks that were originally non-converted (NULL conversion_value)
-- now have late attribution data. This reduces NULL count by 3.

UPDATE {{zone_name}}.iceberg_demos.ad_clicks
SET conversion_value = 5.00, is_converted = true
WHERE click_id = 2;

UPDATE {{zone_name}}.iceberg_demos.ad_clicks
SET conversion_value = 7.50, is_converted = true
WHERE click_id = 6;

UPDATE {{zone_name}}.iceberg_demos.ad_clicks
SET conversion_value = 11.00, is_converted = true
WHERE click_id = 12;
-- ============================================================================
-- Query 6: Post-UPDATE Statistics — NULL Count Decreased
-- ============================================================================
-- NULL conversion_value count: 16 → 13 (filled 3 NULLs)
-- MIN/MAX should remain the same (no new extremes introduced)

ASSERT ROW_COUNT = 1
ASSERT VALUE min_cpc = 0.1
ASSERT VALUE max_cpc = 6.0
ASSERT VALUE min_cv = 0.5
ASSERT VALUE max_cv = 150.0
ASSERT VALUE null_cv_count = 13
ASSERT VALUE nonnull_cv_count = 22
SELECT
    ROUND(MIN(cost_per_click), 2) AS min_cpc,
    ROUND(MAX(cost_per_click), 2) AS max_cpc,
    ROUND(MIN(conversion_value), 2) AS min_cv,
    ROUND(MAX(conversion_value), 2) AS max_cv,
    COUNT(*) FILTER (WHERE conversion_value IS NULL) AS null_cv_count,
    COUNT(*) FILTER (WHERE conversion_value IS NOT NULL) AS nonnull_cv_count
FROM {{zone_name}}.iceberg_demos.ad_clicks;
-- ============================================================================
-- Query 7: Verify Updated Rows
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE conversion_value = 5.00  WHERE click_id = 2
ASSERT VALUE conversion_value = 7.50  WHERE click_id = 6
ASSERT VALUE conversion_value = 11.00 WHERE click_id = 12
SELECT
    click_id,
    campaign_id,
    ROUND(conversion_value, 2) AS conversion_value,
    is_converted
FROM {{zone_name}}.iceberg_demos.ad_clicks
WHERE click_id IN (2, 6, 12)
ORDER BY click_id;
-- ============================================================================
-- Query 8: Data-Skipping — High Cost Clicks
-- ============================================================================
-- A query planner with access to column stats can skip file reads where
-- MAX(cost_per_click) < 3.0, avoiding unnecessary I/O. This query returns
-- only the 8 clicks with cost_per_click > 3.0.

ASSERT ROW_COUNT = 8
SELECT
    click_id,
    campaign_id,
    ROUND(cost_per_click, 2) AS cost_per_click,
    device_type
FROM {{zone_name}}.iceberg_demos.ad_clicks
WHERE cost_per_click > 3.0
ORDER BY click_id;
-- ============================================================================
-- Query 9: Campaign Summary — Final State
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE click_count = 12 WHERE campaign_id = 'summer-sale'
ASSERT VALUE click_count = 11 WHERE campaign_id = 'back-to-school'
ASSERT VALUE click_count = 12 WHERE campaign_id = 'holiday-promo'
SELECT
    campaign_id,
    COUNT(*) AS click_count,
    ROUND(SUM(cost_per_click), 2) AS total_cpc,
    COUNT(*) FILTER (WHERE is_converted = true) AS converted_count
FROM {{zone_name}}.iceberg_demos.ad_clicks
GROUP BY campaign_id
ORDER BY campaign_id;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check covering the full column-stats lifecycle.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_clicks = 35
ASSERT VALUE total_converted = 22
ASSERT VALUE null_conversion_count = 13
ASSERT VALUE min_cpc = 0.1
ASSERT VALUE max_cpc = 6.0
ASSERT VALUE min_cv = 0.5
ASSERT VALUE max_cv = 150.0
ASSERT VALUE campaign_count = 3
ASSERT VALUE device_type_count = 4
SELECT
    COUNT(*) AS total_clicks,
    COUNT(*) FILTER (WHERE is_converted = true) AS total_converted,
    COUNT(*) FILTER (WHERE conversion_value IS NULL) AS null_conversion_count,
    ROUND(MIN(cost_per_click), 2) AS min_cpc,
    ROUND(MAX(cost_per_click), 2) AS max_cpc,
    ROUND(MIN(conversion_value), 2) AS min_cv,
    ROUND(MAX(conversion_value), 2) AS max_cv,
    COUNT(DISTINCT campaign_id) AS campaign_count,
    COUNT(DISTINCT device_type) AS device_type_count
FROM {{zone_name}}.iceberg_demos.ad_clicks;
-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata (including column statistics in manifests) is readable
-- by an Iceberg engine after INSERT + UPDATE mutations.
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.ad_clicks_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.ad_clicks_iceberg
USING ICEBERG
LOCATION '{{data_path}}/ad_clicks';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.ad_clicks_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Verify 1: Row Count + Seed Data Spot-Check
-- ============================================================================
-- Verify total rows and spot-check original seed rows survived the lifecycle.

ASSERT ROW_COUNT = 35
ASSERT VALUE campaign_id = 'summer-sale' WHERE click_id = 1
ASSERT VALUE cost_per_click = 1.25 WHERE click_id = 1
ASSERT VALUE conversion_value = 12.5 WHERE click_id = 1
ASSERT VALUE device_type = 'mobile' WHERE click_id = 1
ASSERT VALUE campaign_id = 'holiday-promo' WHERE click_id = 27
ASSERT VALUE cost_per_click = 4.0 WHERE click_id = 27
ASSERT VALUE conversion_value = 55.0 WHERE click_id = 27
SELECT * FROM {{zone_name}}.iceberg_demos.ad_clicks_iceberg ORDER BY click_id;
-- ============================================================================
-- Iceberg Verify 2: UPDATE Mutations Persisted Through UniForm
-- ============================================================================
-- Clicks 2, 6, 12 were updated from NULL → filled conversion_value.
-- The Iceberg snapshot must reflect the post-UPDATE state.

ASSERT ROW_COUNT = 3
ASSERT VALUE conversion_value = 5.0 WHERE click_id = 2
ASSERT VALUE is_converted = true WHERE click_id = 2
ASSERT VALUE conversion_value = 7.5 WHERE click_id = 6
ASSERT VALUE is_converted = true WHERE click_id = 6
ASSERT VALUE conversion_value = 11.0 WHERE click_id = 12
ASSERT VALUE is_converted = true WHERE click_id = 12
SELECT
    click_id,
    campaign_id,
    ROUND(conversion_value, 2) AS conversion_value,
    is_converted
FROM {{zone_name}}.iceberg_demos.ad_clicks_iceberg
WHERE click_id IN (2, 6, 12)
ORDER BY click_id;
-- ============================================================================
-- Iceberg Verify 3: INSERT Extreme Values Visible
-- ============================================================================
-- Clicks 31–35 were inserted in Version 2 with extreme CPC and CV values.
-- Verify the Iceberg table contains them with correct values.

ASSERT ROW_COUNT = 5
ASSERT VALUE cost_per_click = 0.15 WHERE click_id = 31
ASSERT VALUE cost_per_click = 5.5 WHERE click_id = 32
ASSERT VALUE conversion_value = 120.0 WHERE click_id = 32
ASSERT VALUE cost_per_click = 0.1 WHERE click_id = 34
ASSERT VALUE conversion_value = 0.5 WHERE click_id = 34
ASSERT VALUE cost_per_click = 6.0 WHERE click_id = 35
ASSERT VALUE conversion_value = 150.0 WHERE click_id = 35
SELECT
    click_id,
    campaign_id,
    ROUND(cost_per_click, 2) AS cost_per_click,
    ROUND(conversion_value, 2) AS conversion_value,
    device_type,
    is_converted
FROM {{zone_name}}.iceberg_demos.ad_clicks_iceberg
WHERE click_id >= 31
ORDER BY click_id;
-- ============================================================================
-- Iceberg Verify 4: Column Statistics Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE min_cpc = 0.1
ASSERT VALUE max_cpc = 6.0
ASSERT VALUE min_cv = 0.5
ASSERT VALUE max_cv = 150.0
ASSERT VALUE null_cv_count = 13
ASSERT VALUE nonnull_cv_count = 22
SELECT
    ROUND(MIN(cost_per_click), 2) AS min_cpc,
    ROUND(MAX(cost_per_click), 2) AS max_cpc,
    ROUND(MIN(conversion_value), 2) AS min_cv,
    ROUND(MAX(conversion_value), 2) AS max_cv,
    COUNT(*) FILTER (WHERE conversion_value IS NULL) AS null_cv_count,
    COUNT(*) FILTER (WHERE conversion_value IS NOT NULL) AS nonnull_cv_count
FROM {{zone_name}}.iceberg_demos.ad_clicks_iceberg;
-- ============================================================================
-- Iceberg Verify 5: Campaign Breakdown — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE click_count = 12 WHERE campaign_id = 'summer-sale'
ASSERT VALUE click_count = 11 WHERE campaign_id = 'back-to-school'
ASSERT VALUE click_count = 12 WHERE campaign_id = 'holiday-promo'
ASSERT VALUE converted_count = 8 WHERE campaign_id = 'summer-sale'
ASSERT VALUE converted_count = 8 WHERE campaign_id = 'back-to-school'
ASSERT VALUE converted_count = 6 WHERE campaign_id = 'holiday-promo'
SELECT
    campaign_id,
    COUNT(*) AS click_count,
    COUNT(*) FILTER (WHERE is_converted = true) AS converted_count
FROM {{zone_name}}.iceberg_demos.ad_clicks_iceberg
GROUP BY campaign_id
ORDER BY campaign_id;
