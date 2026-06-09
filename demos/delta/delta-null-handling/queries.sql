-- ============================================================================
-- Customer Survey Data Cleansing — Educational Queries
-- ============================================================================
-- WHAT: NULL-handling patterns for real-world survey data cleansing
-- WHY:  Optional survey fields produce NULLs and sentinel values ('N/A', '')
--       that silently corrupt aggregations and comparisons if not handled
-- HOW:  COALESCE provides defaults, NULLIF normalizes sentinels, IS DISTINCT
--       FROM enables safe NULL comparisons, and explicit NULL sorting controls
--       report ordering. Understanding COUNT(*) vs COUNT(column) prevents
--       subtle aggregation bugs.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — NULL Counts Per Column
-- ============================================================================
-- COUNT(*) counts every row regardless of NULLs. COUNT(column) skips NULLs.
-- The difference reveals how many NULLs exist in each column. This is the
-- first step in any data-quality assessment: understanding the "holes" in
-- your dataset before deciding how to fill them.
--
-- Key insight: empty strings ('') are NOT NULL — COUNT(company) includes them.
-- Sentinel values like 'N/A' are also counted. Only true SQL NULLs are skipped.

ASSERT VALUE total_rows = 30
ASSERT VALUE phone_filled = 22
ASSERT VALUE phone_nulls = 8
ASSERT VALUE company_filled = 24
ASSERT VALUE company_nulls = 6
ASSERT VALUE rating_filled = 25
ASSERT VALUE rating_nulls = 5
ASSERT VALUE nps_filled = 27
ASSERT VALUE nps_nulls = 3
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*)                            AS total_rows,
    COUNT(phone)                        AS phone_filled,
    COUNT(*) - COUNT(phone)             AS phone_nulls,
    COUNT(company)                      AS company_filled,
    COUNT(*) - COUNT(company)           AS company_nulls,
    COUNT(satisfaction_rating)          AS rating_filled,
    COUNT(*) - COUNT(satisfaction_rating) AS rating_nulls,
    COUNT(nps_score)                    AS nps_filled,
    COUNT(*) - COUNT(nps_score)         AS nps_nulls
FROM {{zone_name}}.delta_demos.survey_responses;


-- ============================================================================
-- LEARN: COALESCE — Replace NULLs with Sensible Defaults
-- ============================================================================
-- COALESCE(value, default) returns the first non-NULL argument. This is the
-- standard way to provide fallback values for display or downstream processing.
--
-- Use cases shown here:
--   phone → 'No phone provided'   (for customer contact lists)
--   company → 'Independent'       (freelancers without a company)
--   satisfaction_rating → 0       (treat unanswered as zero for reporting)
--
-- COALESCE does NOT modify the stored data — it only transforms the output.

ASSERT ROW_COUNT = 30
ASSERT VALUE display_phone = 'No phone provided' WHERE response_id = 2
ASSERT VALUE display_company = 'Independent' WHERE response_id = 3
ASSERT VALUE display_rating = 0 WHERE response_id = 7
ASSERT VALUE display_phone = '555-0101' WHERE response_id = 1
SELECT
    response_id,
    customer_name,
    COALESCE(phone, 'No phone provided')             AS display_phone,
    COALESCE(company, 'Independent')                  AS display_company,
    COALESCE(satisfaction_rating, 0)                  AS display_rating,
    COALESCE(feedback_text, '(no comment)')           AS display_feedback
FROM {{zone_name}}.delta_demos.survey_responses
ORDER BY response_id;


-- ============================================================================
-- LEARN: NULLIF — Convert Sentinel Values to Proper NULLs
-- ============================================================================
-- NULLIF(value, sentinel) returns NULL when value equals the sentinel, otherwise
-- returns the value unchanged. This normalizes "fake NULLs" — values like 'N/A'
-- or '' that legacy systems insert instead of true NULLs.
--
-- Before NULLIF: referral_source has 5 NULLs + 4 'N/A' strings = messy data
-- After NULLIF:  all 9 missing referrals become proper NULLs
--
-- Before NULLIF: company has 6 NULLs + 4 empty strings
-- After NULLIF:  all 10 missing companies become proper NULLs

ASSERT VALUE referral_nulls_before = 5
ASSERT VALUE referral_nulls_after = 9
ASSERT VALUE company_nulls_before = 6
ASSERT VALUE company_nulls_after = 10
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) - COUNT(referral_source)                        AS referral_nulls_before,
    COUNT(*) - COUNT(NULLIF(referral_source, 'N/A'))         AS referral_nulls_after,
    COUNT(*) - COUNT(company)                                AS company_nulls_before,
    COUNT(*) - COUNT(NULLIF(company, ''))                    AS company_nulls_after
FROM {{zone_name}}.delta_demos.survey_responses;


-- ============================================================================
-- LEARN: IS DISTINCT FROM — Safe NULL-Aware Comparisons
-- ============================================================================
-- Standard SQL comparison (=) returns NULL when either operand is NULL, which
-- means WHERE company = NULL returns zero rows — a common bug. IS NOT DISTINCT
-- FROM treats NULL = NULL as TRUE, enabling safe comparisons.
--
-- IS DISTINCT FROM is the negation: it treats NULL != NULL as FALSE.
--
-- Here we find all rows where company is truly missing (NULL) or effectively
-- missing (empty string converted to NULL via NULLIF).

ASSERT VALUE null_company_count = 6
ASSERT VALUE null_or_empty_company_count = 10
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) FILTER (WHERE company IS NOT DISTINCT FROM NULL)         AS null_company_count,
    COUNT(*) FILTER (WHERE NULLIF(company, '') IS NOT DISTINCT FROM NULL) AS null_or_empty_company_count
FROM {{zone_name}}.delta_demos.survey_responses;


-- ============================================================================
-- LEARN: NULL Sorting — NULLS FIRST vs NULLS LAST
-- ============================================================================
-- By default, NULLs sort to the end in ascending order (implementation-defined
-- in SQL, but most engines including DeltaForge place them last). Explicit
-- NULLS FIRST / NULLS LAST gives you control.
--
-- For reports, you often want missing ratings at the top (NULLS FIRST) so
-- managers see incomplete responses first. For dashboards, NULLS LAST keeps
-- the focus on completed data.
--
-- This query returns all 30 rows sorted by satisfaction_rating with NULLs
-- appearing first. The 5 NULL ratings appear in rows 1-5.

ASSERT ROW_COUNT = 30
ASSERT VALUE satisfaction_rating IS NULL WHERE response_id = 3
ASSERT VALUE satisfaction_rating = 1 WHERE response_id = 14
ASSERT VALUE satisfaction_rating = 5 WHERE response_id = 1
SELECT
    response_id,
    customer_name,
    satisfaction_rating,
    CASE
        WHEN satisfaction_rating IS NULL THEN 'Not answered'
        WHEN satisfaction_rating >= 4 THEN 'Satisfied'
        WHEN satisfaction_rating >= 3 THEN 'Neutral'
        ELSE 'Dissatisfied'
    END AS rating_category
FROM {{zone_name}}.delta_demos.survey_responses
ORDER BY satisfaction_rating NULLS FIRST, response_id;


-- ============================================================================
-- LEARN: NULL in Aggregation — COUNT(*) vs COUNT(column) vs AVG
-- ============================================================================
-- Aggregation functions (except COUNT(*)) silently skip NULLs. This means:
--   AVG(rating) = 93/25 = 3.72  (only 25 non-NULL values)
--   AVG(COALESCE(rating, 0)) = 93/30 = 3.10  (all 30 rows, NULLs as 0)
--
-- The difference (3.72 vs 3.10) is significant! Using the wrong one can
-- misrepresent customer satisfaction. Choose deliberately:
--   - AVG(rating) when "what do respondents think?" (exclude non-answers)
--   - AVG(COALESCE(rating,0)) when "what is the overall score?" (penalize gaps)

ASSERT VALUE count_star = 30
ASSERT VALUE count_rating = 25
ASSERT VALUE sum_rating = 93
ASSERT VALUE avg_rating_skip_nulls = 3.72
ASSERT VALUE avg_rating_with_zeros = 3.10
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*)                                              AS count_star,
    COUNT(satisfaction_rating)                            AS count_rating,
    SUM(satisfaction_rating)                              AS sum_rating,
    ROUND(AVG(satisfaction_rating), 2)                    AS avg_rating_skip_nulls,
    ROUND(AVG(COALESCE(satisfaction_rating, 0)), 2)       AS avg_rating_with_zeros
FROM {{zone_name}}.delta_demos.survey_responses;


-- ============================================================================
-- LEARN: NULL Arithmetic Propagation — NULL Poisons Expressions
-- ============================================================================
-- Any arithmetic involving NULL produces NULL: 5 * NULL = NULL, NULL + 3 = NULL.
-- This "NULL propagation" means a composite score (rating * nps_score) becomes
-- NULL whenever either input is missing — even if the other is perfectly valid.
--
-- 6 rows have at least one NULL in rating or nps_score, so their raw product
-- is NULL. COALESCE fixes this by substituting zeros before multiplication.
--
-- AVG of raw products: 685/24 = 28.54 (only 24 non-NULL products)
-- AVG of coalesced products: 685/30 = 22.83 (all 30 rows)

ASSERT VALUE rows_with_null_product = 6
ASSERT VALUE rows_with_valid_product = 24
ASSERT VALUE avg_raw_product = 28.54
ASSERT VALUE avg_safe_product = 22.83
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) FILTER (WHERE satisfaction_rating * nps_score IS NULL)     AS rows_with_null_product,
    COUNT(*) FILTER (WHERE satisfaction_rating * nps_score IS NOT NULL) AS rows_with_valid_product,
    ROUND(AVG(satisfaction_rating * nps_score), 2)                     AS avg_raw_product,
    ROUND(AVG(COALESCE(satisfaction_rating, 0) * COALESCE(nps_score, 0)), 2) AS avg_safe_product
FROM {{zone_name}}.delta_demos.survey_responses;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Summary verification ensuring the dataset and all NULL-handling patterns
-- produce expected results.

-- Verify total row count
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify NULL phone count
ASSERT VALUE null_phones = 8
SELECT COUNT(*) FILTER (WHERE phone IS NULL) AS null_phones
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify NULL company count (true NULLs only)
ASSERT VALUE null_companies = 6
SELECT COUNT(*) FILTER (WHERE company IS NULL) AS null_companies
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify empty string company count
ASSERT VALUE empty_companies = 4
SELECT COUNT(*) FILTER (WHERE company = '') AS empty_companies
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify NULL satisfaction_rating count
ASSERT VALUE null_ratings = 5
SELECT COUNT(*) FILTER (WHERE satisfaction_rating IS NULL) AS null_ratings
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify NULL nps_score count
ASSERT VALUE null_nps = 3
SELECT COUNT(*) FILTER (WHERE nps_score IS NULL) AS null_nps
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify N/A referral sentinel count
ASSERT VALUE na_referrals = 4
SELECT COUNT(*) FILTER (WHERE referral_source = 'N/A') AS na_referrals
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify NULL referral_source count
ASSERT VALUE null_referrals = 5
SELECT COUNT(*) FILTER (WHERE referral_source IS NULL) AS null_referrals
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify NULLIF produces correct combined NULL count
ASSERT VALUE combined_referral_nulls = 9
SELECT COUNT(*) - COUNT(NULLIF(referral_source, 'N/A')) AS combined_referral_nulls
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify average satisfaction rating (NULLs excluded)
ASSERT VALUE avg_rating = 3.72
SELECT ROUND(AVG(satisfaction_rating), 2) AS avg_rating
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify average NPS score (NULLs excluded)
ASSERT VALUE avg_nps = 6.96
SELECT ROUND(AVG(nps_score), 2) AS avg_nps
FROM {{zone_name}}.delta_demos.survey_responses;

-- Verify composite score arithmetic
ASSERT VALUE total_composite = 685
SELECT SUM(satisfaction_rating * nps_score) AS total_composite
FROM {{zone_name}}.delta_demos.survey_responses;
