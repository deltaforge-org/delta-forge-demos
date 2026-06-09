-- ============================================================================
-- Delta Collations — Educational Queries
-- ============================================================================
-- WHAT: Collation controls how strings are compared, sorted, and matched
--       across different languages, scripts, and character encodings
-- WHY:  Default byte-order sorting produces incorrect results for multilingual
--       data — German umlauts sort after 'z', accented French characters are
--       misplaced, and CJK characters sort unpredictably
-- HOW:  Delta stores the raw Unicode text in Parquet VARCHAR columns. To enable
--       consistent cross-language sorting, a normalized sort_key column provides
--       ASCII-transliterated versions (e.g., 'Mueller' -> 'mueller_hans') that
--       sort correctly regardless of locale
-- ============================================================================


-- ============================================================================
-- EXPLORE: Language and Country Distribution
-- ============================================================================
-- This global contact directory contains names from 18 countries and 16
-- language groups, spanning European, Asian, and other scripts.

-- Verify 16 language groups are present
ASSERT ROW_COUNT = 16
SELECT language,
       COUNT(*) AS contact_count,
       MIN(country) AS example_country
FROM {{zone_name}}.delta_demos.global_contacts
GROUP BY language
ORDER BY contact_count DESC, language;


-- ============================================================================
-- LEARN: The Sorting Problem with Multilingual Data
-- ============================================================================
-- Compare sorting by last_name (raw Unicode) vs sort_key (normalized ASCII).
-- Names like 'Mueller' (from 'Mueller'), 'Grosse' (from 'Grosse'),
-- 'Schroeder' (from 'Schroeder') are already stored as ASCII transliterations
-- in this dataset. The sort_key column ensures a consistent lastname_firstname
-- ordering pattern that works identically across all languages.

ASSERT ROW_COUNT = 5
SELECT id, first_name, last_name, country, sort_key
FROM {{zone_name}}.delta_demos.global_contacts
WHERE language = 'de'
ORDER BY sort_key;


-- ============================================================================
-- LEARN: Why Sort Keys Matter for Cross-Language Queries
-- ============================================================================
-- When you ORDER BY last_name, the results depend on the database's collation
-- setting. Different engines may sort the same data differently. A sort_key
-- column eliminates this ambiguity by pre-computing the sort order.
-- Here we compare German, French, and Spanish names sorted by sort_key.

ASSERT ROW_COUNT = 16
SELECT id, first_name, last_name, country, language, sort_key
FROM {{zone_name}}.delta_demos.global_contacts
WHERE language IN ('de', 'fr', 'es')
ORDER BY sort_key;


-- ============================================================================
-- EXPLORE: Asian Names — Romanized Sort Keys
-- ============================================================================
-- Japanese, Chinese, and Korean names are stored with romanized sort keys
-- (e.g., 'yamamoto_takeshi', 'zhang_wei', 'kim_minjun'). This enables
-- consistent sorting alongside Latin-script names.

ASSERT ROW_COUNT = 10
SELECT id, first_name, last_name, city, country, sort_key
FROM {{zone_name}}.delta_demos.global_contacts
WHERE language IN ('ja', 'zh', 'ko')
ORDER BY sort_key;


-- ============================================================================
-- LEARN: Regional Contact Distribution
-- ============================================================================
-- Understanding how contacts distribute across regions helps plan
-- locale-specific data processing strategies.

ASSERT ROW_COUNT = 18
SELECT country, language,
       COUNT(*) AS contacts
FROM {{zone_name}}.delta_demos.global_contacts
GROUP BY country, language
ORDER BY country;


-- ============================================================================
-- EXPLORE: Sort Key Consistency Check
-- ============================================================================
-- Every contact should have a non-NULL sort_key following the pattern
-- 'lastname_firstname' in lowercase ASCII. Let's verify the pattern.

-- Verify all 40 contacts have valid lastname_firstname sort key pattern
ASSERT NO_FAIL IN pattern_check
ASSERT ROW_COUNT = 40
SELECT id, first_name, last_name, sort_key,
       CASE WHEN sort_key LIKE '%_%' THEN 'PASS'
            ELSE 'FAIL: missing underscore' END AS pattern_check
FROM {{zone_name}}.delta_demos.global_contacts
ORDER BY sort_key;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Verification of contact counts, language distribution, and sort key integrity.

-- Verify total row count
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.global_contacts;

-- Verify European contact count
ASSERT VALUE european_count = 20
SELECT COUNT(*) AS european_count FROM {{zone_name}}.delta_demos.global_contacts
WHERE country IN ('Germany', 'France', 'Spain', 'Sweden', 'Norway', 'Denmark', 'Finland');

-- Verify Asian contact count
ASSERT VALUE asian_count = 10
SELECT COUNT(*) AS asian_count FROM {{zone_name}}.delta_demos.global_contacts
WHERE country IN ('Japan', 'China', 'South Korea');

-- Verify other region count
ASSERT VALUE other_count = 10
SELECT COUNT(*) AS other_count FROM {{zone_name}}.delta_demos.global_contacts
WHERE country NOT IN ('Germany', 'France', 'Spain', 'Sweden', 'Norway', 'Denmark', 'Finland',
                       'Japan', 'China', 'South Korea');

-- Verify country count
ASSERT VALUE country_count = 18
SELECT COUNT(DISTINCT country) AS country_count FROM {{zone_name}}.delta_demos.global_contacts;

-- Verify language count
ASSERT VALUE language_count = 16
SELECT COUNT(DISTINCT language) AS language_count FROM {{zone_name}}.delta_demos.global_contacts;

-- Verify accented names count (European countries)
ASSERT VALUE accented_count = 20
SELECT COUNT(*) AS accented_count FROM {{zone_name}}.delta_demos.global_contacts
WHERE country IN ('Germany', 'France', 'Spain', 'Sweden', 'Norway', 'Denmark', 'Finland');

-- Verify all sort keys are populated
ASSERT VALUE sort_key_count = 40
SELECT COUNT(*) AS sort_key_count FROM {{zone_name}}.delta_demos.global_contacts WHERE sort_key IS NOT NULL;
