-- ============================================================================
-- Iceberg Native Large Manifests (Web Analytics) — Queries
-- ============================================================================
-- Demonstrates reading a native Iceberg V2 table with 10 data files spread
-- across 10 manifest entries. The reader must traverse the full manifest
-- chain to discover all files and union the results. All queries are
-- read-only and verify correctness of the reconstructed dataset.
--
-- 600 total rows = 10 batches x 60 rows per batch.
-- ============================================================================


-- ============================================================================
-- Query 1: Total Row Count
-- ============================================================================
-- Verifies that all 10 data files (one per manifest) were discovered and
-- their rows unioned correctly. 10 batches x 60 rows = 600.

ASSERT ROW_COUNT = 600
SELECT * FROM {{zone_name}}.iceberg.web_analytics;


-- ============================================================================
-- Query 2: Per-Country Breakdown
-- ============================================================================
-- 10 countries across all batches. Sum must equal 600.

ASSERT ROW_COUNT = 10
ASSERT VALUE cnt = 48 WHERE country = 'AU'
ASSERT VALUE cnt = 66 WHERE country = 'BR'
ASSERT VALUE cnt = 76 WHERE country = 'CA'
ASSERT VALUE cnt = 48 WHERE country = 'DE'
ASSERT VALUE cnt = 71 WHERE country = 'FR'
ASSERT VALUE cnt = 65 WHERE country = 'IN'
ASSERT VALUE cnt = 58 WHERE country = 'JP'
ASSERT VALUE cnt = 57 WHERE country = 'MX'
ASSERT VALUE cnt = 64 WHERE country = 'UK'
ASSERT VALUE cnt = 47 WHERE country = 'US'
SELECT
    country,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg.web_analytics
GROUP BY country
ORDER BY country;


-- ============================================================================
-- Query 3: Per-Device Breakdown
-- ============================================================================
-- Three device types: desktop, mobile, tablet.

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 194 WHERE device_type = 'desktop'
ASSERT VALUE cnt = 204 WHERE device_type = 'mobile'
ASSERT VALUE cnt = 202 WHERE device_type = 'tablet'
SELECT
    device_type,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg.web_analytics
GROUP BY device_type
ORDER BY device_type;


-- ============================================================================
-- Query 4: Bounce Rate
-- ============================================================================
-- Percentage of sessions where is_bounce = true.

ASSERT ROW_COUNT = 1
ASSERT VALUE bounce_count = 165
ASSERT VALUE bounce_pct = 27.50
SELECT
    SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS bounce_count,
    ROUND(AVG(CASE WHEN is_bounce THEN 1.0 ELSE 0.0 END) * 100, 2) AS bounce_pct
FROM {{zone_name}}.iceberg.web_analytics;


-- ============================================================================
-- Query 5: Average Time on Page by Device
-- ============================================================================
-- Exercises floating-point aggregation across all 10 data files.

ASSERT ROW_COUNT = 3
ASSERT VALUE avg_time = 286.74 WHERE device_type = 'desktop'
ASSERT VALUE avg_time = 294.66 WHERE device_type = 'mobile'
ASSERT VALUE avg_time = 288.04 WHERE device_type = 'tablet'
SELECT
    device_type,
    ROUND(AVG(time_on_page), 2) AS avg_time
FROM {{zone_name}}.iceberg.web_analytics
GROUP BY device_type
ORDER BY device_type;


-- ============================================================================
-- Query 6: Average Event Count
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE avg_events = 10.04
SELECT
    ROUND(AVG(event_count), 2) AS avg_events
FROM {{zone_name}}.iceberg.web_analytics;


-- ============================================================================
-- Query 7: Top Referrers
-- ============================================================================
-- 10 referrer sources ranked by session count.

ASSERT ROW_COUNT = 10
ASSERT VALUE cnt = 75 WHERE referrer = 'bing.com'
ASSERT VALUE cnt = 75 WHERE referrer = 'facebook.com'
ASSERT VALUE cnt = 74 WHERE referrer = 'github.com'
ASSERT VALUE cnt = 63 WHERE referrer = 'reddit.com'
ASSERT VALUE cnt = 60 WHERE referrer = 'google.com'
ASSERT VALUE cnt = 60 WHERE referrer = 'twitter.com'
ASSERT VALUE cnt = 56 WHERE referrer = 'linkedin.com'
ASSERT VALUE cnt = 48 WHERE referrer = 'direct'
ASSERT VALUE cnt = 46 WHERE referrer = 'youtube.com'
ASSERT VALUE cnt = 43 WHERE referrer = 'email-campaign'
SELECT
    referrer,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg.web_analytics
GROUP BY referrer
ORDER BY cnt DESC, referrer;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check: row count, country count, device count,
-- bounce count, and total event count across all 10 manifest files.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 600
ASSERT VALUE country_count = 10
ASSERT VALUE device_count = 3
ASSERT VALUE referrer_count = 10
ASSERT VALUE bounce_count = 165
ASSERT VALUE total_events = 6023
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT country) AS country_count,
    COUNT(DISTINCT device_type) AS device_count,
    COUNT(DISTINCT referrer) AS referrer_count,
    SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS bounce_count,
    SUM(event_count) AS total_events
FROM {{zone_name}}.iceberg.web_analytics;
