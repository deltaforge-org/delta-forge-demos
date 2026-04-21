-- ============================================================================
-- Demo: Public Holiday Calendar Sync — Queries
-- ============================================================================
-- Validates the path-parameter-driven incremental ingest flow:
--   • Silver starts with an 8-row launch seed (Norway + Sweden, 2024).
--   • After INVOKE — path_params fill /api/v3/PublicHolidays/2025/NO —
--     the bronze landing holds exactly Norway's 2025 public holidays
--     (12-13 rows for most recent years).
--   • The NOT EXISTS merge on (country_code, holiday_year, holiday_date)
--     promoted every new row without touching the seed.
--   • A replayed anti-join against the target returns zero "still-
--     wanted" combos, proving the wave completed fully.
--   • Specific stable dates (NO Constitution Day, NO Christmas Day)
--     are assertable across both the 2024 seed and the 2025 wave —
--     they exercise the end-to-end path from path_param → HTTPS →
--     JSON flatten → DATE cast → silver merge.
--
-- Aggregate assertions use BETWEEN ranges where Nager.Date data could
-- reasonably shift by one entry (some countries move floating
-- holidays); exact VALUE checks are reserved for the fixed dates
-- whose calendars are settled.
-- ============================================================================

-- ============================================================================
-- Query 1: Wave composition — seed + API split
-- ============================================================================
-- source_batch partitions silver into the 8-row launch seed and the
-- ~12-row Nager.Date wave. Headline total = seed + API; if either
-- number drifts, a wave either double-inserted or lost rows.

ASSERT ROW_COUNT = 1
ASSERT VALUE seed_count = 8
ASSERT VALUE api_count BETWEEN 10 AND 16
ASSERT VALUE total_count BETWEEN 18 AND 24
SELECT
    SUM(CASE WHEN source_batch = 'launch_seed' THEN 1 ELSE 0 END) AS seed_count,
    SUM(CASE WHEN source_batch = 'nager_api'   THEN 1 ELSE 0 END) AS api_count,
    COUNT(*)                                                      AS total_count
FROM {{zone_name}}.hr_calendar.country_holidays;

-- ============================================================================
-- Query 2: Fixed-date wire proof — NO Constitution Day 2025
-- ============================================================================
-- 17 May is Norway's Constitution Day, fixed every year. Asserting
-- exact date + english_name + source_batch proves the full chain:
-- path_params substituted correctly into the URL, the JSON flatten
-- mapped the response's English `name` column, the DATE cast read
-- Nager.Date's ISO-8601 string cleanly, and the merge landed the row
-- under 'nager_api'.

ASSERT ROW_COUNT = 1
ASSERT VALUE holiday_year = 2025
ASSERT VALUE country_code = 'NO'
ASSERT VALUE english_name = 'Constitution Day'
ASSERT VALUE source_batch = 'nager_api'
SELECT holiday_year, country_code, english_name, source_batch
FROM {{zone_name}}.hr_calendar.country_holidays
WHERE country_code = 'NO'
  AND holiday_year = 2025
  AND holiday_date = DATE '2025-05-17';

-- ============================================================================
-- Query 3: Incremental watermark — "which (country, year) is missing?"
-- ============================================================================
-- This is the exact anti-join shape a scheduled pipeline runs against
-- the target table BEFORE every API call. `wanted` holds the set of
-- combos the product owner has declared "eventually we want", and the
-- NOT EXISTS against silver drops any combo already loaded. Post-
-- merge, the result must be zero — the wave completed cleanly and
-- the next run starts from a clean slate.
--
-- In production the output of this query (a list of (country, year)
-- rows) drives:
--     ALTER API ENDPOINT ... SET OPTIONS (
--         path_param.year         = '<year-from-anti-join>',
--         path_param.country_code = '<country-from-anti-join>'
--     );
-- just before each INVOKE. Keeping the compute in SQL against the
-- target — not in an external manifest — makes the loop self-healing:
-- a wave missed on a prior run retries automatically next time.

ASSERT ROW_COUNT = 1
ASSERT VALUE missing_combos = 0
SELECT COUNT(*) AS missing_combos
FROM (VALUES ('NO', 2025)) AS wanted(country_code, holiday_year)
WHERE NOT EXISTS (
    SELECT 1 FROM {{zone_name}}.hr_calendar.country_holidays t
    WHERE t.country_code = wanted.country_code
      AND t.holiday_year = wanted.holiday_year
);

-- ============================================================================
-- Query 4: Bronze landing — narrow wire payload proof
-- ============================================================================
-- The bronze external table reads the raw JSON page written by
-- INVOKE. Because path_param.country_code narrowed the response to
-- just Norway's holidays, bronze holds only that country's rows —
-- never a global or multi-country dataset. Norway's modern public
-- holiday count sits in the 12-14 range; the ROW_COUNT band absorbs
-- minor calendar revisions without letting a full-region mis-route
-- pass silently.

ASSERT ROW_COUNT >= 10
ASSERT ROW_COUNT <= 16
SELECT holiday_date, local_name, english_name, is_global
FROM {{zone_name}}.hr_calendar.public_holidays_bronze
ORDER BY holiday_date;

-- ============================================================================
-- Query 5: Seed preservation — 2024 rows survived the 2025 wave
-- ============================================================================
-- The anti-join merge must not touch rows already present. GROUP BY
-- verifies both the Norway-2024 and Sweden-2024 seed counts are
-- exactly 4 — their original sizes — and that both (country, 2024)
-- groups are still present. If either count moved, the merge's
-- composite-key guard is broken.

-- ASSERT VALUE ... WHERE only supports a single-column predicate, so we
-- rely on `source_batch = 'launch_seed'` in the SELECT to pin the year
-- to 2024 and match ASSERT VALUE rows by country_code alone.
ASSERT ROW_COUNT = 2
ASSERT VALUE holiday_count = 4 WHERE country_code = 'NO'
ASSERT VALUE holiday_count = 4 WHERE country_code = 'SE'
ASSERT VALUE holiday_year = 2024 WHERE country_code = 'NO'
ASSERT VALUE holiday_year = 2024 WHERE country_code = 'SE'
SELECT country_code, holiday_year, COUNT(*) AS holiday_count
FROM {{zone_name}}.hr_calendar.country_holidays
WHERE source_batch = 'launch_seed'
GROUP BY country_code, holiday_year
ORDER BY country_code, holiday_year;

-- ============================================================================
-- Query 6: Silver Delta history — at least two wave writes visible
-- ============================================================================
-- CREATE (v0, schema only) + launch-seed INSERT (v1) + Nager.Date
-- merge INSERT (v2) means DESCRIBE HISTORY must return at least 2
-- rows. Each wave is a discrete Delta version — the prerequisite for
-- VERSION AS OF time-travel rollback if a wave lands bad data.

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.hr_calendar.country_holidays;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One cross-cutting query covering the whole pipeline:
--   • Total row count in the expected band (seed + wave)
--   • Two specific fixed dates present (NO 2025 Christmas, NO 2024
--     Constitution Day) — one from the API, one from the seed
--   • Seed per-country counts preserved at 4 each
--   • 2025 Norway wave count in the expected band
--   • Every row has a source_batch label (provenance not null)
-- If this passes: the credential resolved, the HTTPS fetch used the
-- substituted path params, the JSON flatten produced the expected
-- shape, the DATE cast worked, and the composite-key merge added the
-- new wave without corrupting the seed.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows BETWEEN 18 AND 24
ASSERT VALUE has_no_2025_christmas = 1
ASSERT VALUE has_no_2024_constitution = 1
ASSERT VALUE no_2024_count = 4
ASSERT VALUE se_2024_count = 4
ASSERT VALUE no_2025_count BETWEEN 10 AND 16
ASSERT VALUE every_row_labeled = 1
SELECT
    COUNT(*)                                                                              AS total_rows,
    SUM(CASE WHEN country_code='NO' AND holiday_year=2025 AND holiday_date=DATE '2025-12-25' THEN 1 ELSE 0 END) AS has_no_2025_christmas,
    SUM(CASE WHEN country_code='NO' AND holiday_year=2024 AND holiday_date=DATE '2024-05-17' THEN 1 ELSE 0 END) AS has_no_2024_constitution,
    SUM(CASE WHEN country_code='NO' AND holiday_year=2024 THEN 1 ELSE 0 END)              AS no_2024_count,
    SUM(CASE WHEN country_code='SE' AND holiday_year=2024 THEN 1 ELSE 0 END)              AS se_2024_count,
    SUM(CASE WHEN country_code='NO' AND holiday_year=2025 THEN 1 ELSE 0 END)              AS no_2025_count,
    CASE WHEN SUM(CASE WHEN source_batch IS NULL THEN 1 ELSE 0 END) = 0 THEN 1 ELSE 0 END AS every_row_labeled
FROM {{zone_name}}.hr_calendar.country_holidays;
