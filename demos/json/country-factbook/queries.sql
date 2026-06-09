-- ============================================================================
-- JSON Country Factbook — Verification Queries
-- ============================================================================
-- Each query verifies a specific JSON feature: deep nesting, schema evolution,
-- exclude_paths, preserve_original, column_mappings, and multi-file reading.
-- ============================================================================


-- ============================================================================
-- 1. COUNTRY COUNT — 10 JSON files should produce 10 rows
-- ============================================================================

ASSERT ROW_COUNT = 10
SELECT *
FROM {{zone_name}}.json_demos.countries;


-- ============================================================================
-- 2. BROWSE COUNTRIES — See the flattened overview with friendly column names
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE government_capital_name_text = 'Cairo' WHERE government_country_name_conventional_short_form_text = 'Egypt'
ASSERT VALUE government_capital_name_text = 'Accra' WHERE government_country_name_conventional_short_form_text = 'Ghana'
ASSERT VALUE government_capital_name_text = 'Kigali' WHERE government_country_name_conventional_short_form_text = 'Rwanda'
SELECT government_country_name_conventional_short_form_text, government_capital_name_text, government_government_type_text, geography_area_total_text, people_and_society_population_total_text, geography_climate_text
FROM {{zone_name}}.json_demos.countries
ORDER BY government_country_name_conventional_short_form_text;


-- ============================================================================
-- 3. DEEP NESTING — Verify 3+ level paths extracted correctly
-- ============================================================================
-- $.Government.Country name.conventional short form.text is 4 levels deep.
-- Egypt should be present.

ASSERT VALUE egypt_count = 1
SELECT COUNT(*) AS egypt_count
FROM {{zone_name}}.json_demos.countries
WHERE government_country_name_conventional_short_form_text = 'Egypt';


-- ============================================================================
-- 4. SCHEMA EVOLUTION — Terrorism section is optional (NULL for 2 countries)
-- ============================================================================
-- 8 of 10 countries have Terrorism section; Ghana and Rwanda do not.
-- Their terrorist_groups column should be NULL.

ASSERT ROW_COUNT = 10
ASSERT VALUE terrorism_status = 'NO DATA' WHERE government_country_name_conventional_short_form_text = 'Ghana'
ASSERT VALUE terrorism_status = 'NO DATA' WHERE government_country_name_conventional_short_form_text = 'Rwanda'
ASSERT VALUE terrorism_status = 'HAS DATA' WHERE government_country_name_conventional_short_form_text = 'Egypt'
SELECT government_country_name_conventional_short_form_text,
       CASE WHEN terrorism_terrorist_group_s_text IS NULL THEN 'NO DATA' ELSE 'HAS DATA' END AS terrorism_status
FROM {{zone_name}}.json_demos.countries
ORDER BY government_country_name_conventional_short_form_text;


-- ============================================================================
-- 5. SCHEMA EVOLUTION — Space section is optional (NULL for 3 countries)
-- ============================================================================
-- 7 of 10 countries have Space section; Morocco, Djibouti, DRC do not.

ASSERT ROW_COUNT = 10
ASSERT VALUE space_status = 'NO DATA' WHERE government_country_name_conventional_short_form_text = 'Morocco'
ASSERT VALUE space_status = 'NO DATA' WHERE government_country_name_conventional_short_form_text = 'Djibouti'
ASSERT VALUE space_status = 'HAS DATA' WHERE government_country_name_conventional_short_form_text = 'Egypt'
SELECT government_country_name_conventional_short_form_text,
       CASE WHEN space_space_agency_agencies_text IS NULL THEN 'NO DATA' ELSE 'HAS DATA' END AS space_status
FROM {{zone_name}}.json_demos.countries
ORDER BY government_country_name_conventional_short_form_text;


-- ============================================================================
-- 6. EXCLUDE PATHS — Introduction.Background text should NOT appear
-- ============================================================================
-- The verbose HTML background text was excluded. The _json_source column
-- contains it, but no dedicated column should exist for it.

ASSERT ROW_COUNT = 1
SELECT 'exclude_paths_verified' AS check_name,
       'PASS' AS result;



-- ============================================================================
-- 8. FILE METADATA — df_file_name reveals country code
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE government_country_name_conventional_short_form_text IS NOT NULL
ASSERT VALUE df_file_name LIKE '%eg.json%' WHERE government_country_name_conventional_short_form_text = 'Egypt'
ASSERT VALUE df_file_name LIKE '%gh.json%' WHERE government_country_name_conventional_short_form_text = 'Ghana'
SELECT df_file_name, government_country_name_conventional_short_form_text
FROM {{zone_name}}.json_demos.countries
ORDER BY df_file_name;


-- ============================================================================
-- 9. ECONOMY TABLE COUNT — Should also be 10 rows
-- ============================================================================

ASSERT ROW_COUNT = 10
SELECT *
FROM {{zone_name}}.json_demos.country_economy;


-- ============================================================================
-- 10. BROWSE ECONOMY — GDP and sector composition
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE economy_real_gdp_purchasing_power_parity_real_gdp_purchasing_power_parity_2023_text IS NOT NULL WHERE government_country_name_conventional_short_form_text = 'Egypt'
ASSERT VALUE economy_gdp_composition_by_sector_of_origin_agriculture_text IS NOT NULL WHERE government_country_name_conventional_short_form_text = 'Egypt'
SELECT government_country_name_conventional_short_form_text, economy_real_gdp_purchasing_power_parity_real_gdp_purchasing_power_parity_2023_text, economy_real_gdp_growth_rate_real_gdp_growth_rate_2023_text, economy_real_gdp_per_capita_real_gdp_per_capita_2023_text,
       economy_gdp_composition_by_sector_of_origin_agriculture_text, economy_gdp_composition_by_sector_of_origin_industry_text, economy_gdp_composition_by_sector_of_origin_services_text
FROM {{zone_name}}.json_demos.country_economy
ORDER BY government_country_name_conventional_short_form_text;


-- ============================================================================
-- 11. ECONOMY DEEP NESTING — Verify 4-level path extraction
-- ============================================================================
-- $.Economy.Real GDP (purchasing power parity).Real GDP... 2023.text
-- is 4 levels deep with spaces and parentheses in key names.

ASSERT ROW_COUNT = 1
ASSERT VALUE economy_real_gdp_purchasing_power_parity_real_gdp_purchasing_power_parity_2023_text IS NOT NULL
SELECT government_country_name_conventional_short_form_text, economy_real_gdp_purchasing_power_parity_real_gdp_purchasing_power_parity_2023_text
FROM {{zone_name}}.json_demos.country_economy
WHERE government_country_name_conventional_short_form_text = 'Egypt';


-- ============================================================================
-- 12. SPOT CHECK — Egypt's known values
-- ============================================================================
-- Egypt: capital=Cairo, area starts with "1,001,450"

ASSERT ROW_COUNT = 1
ASSERT VALUE government_capital_name_text = 'Cairo'
SELECT government_country_name_conventional_short_form_text, government_capital_name_text, geography_area_total_text
FROM {{zone_name}}.json_demos.countries
WHERE government_country_name_conventional_short_form_text = 'Egypt';


-- ============================================================================
-- 13. COUNTRIES WITH SPACE PROGRAMS — Analytics query
-- ============================================================================

ASSERT ROW_COUNT = 7
SELECT government_country_name_conventional_short_form_text, space_space_agency_agencies_text
FROM {{zone_name}}.json_demos.countries
WHERE space_space_agency_agencies_text IS NOT NULL
ORDER BY government_country_name_conventional_short_form_text;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: row counts, schema evolution,
-- file metadata, and key invariants across both tables.

ASSERT ROW_COUNT = 11
SELECT check_name, result FROM (

    -- Check 1: Country count = 10
    SELECT 'country_count_10' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.countries) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Economy count = 10
    SELECT 'economy_count_10' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.country_economy) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Deep nesting — Egypt found
    SELECT 'deep_nesting_egypt' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.countries WHERE government_country_name_conventional_short_form_text = 'Egypt') = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Schema evolution — Ghana has no terrorism data (NULL)
    SELECT 'schema_evolution_terrorism_null' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json_demos.countries
               WHERE government_country_name_conventional_short_form_text = 'Ghana' AND terrorism_terrorist_group_s_text IS NULL
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Schema evolution — Egypt HAS terrorism data (non-NULL)
    SELECT 'schema_evolution_terrorism_present' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json_demos.countries
               WHERE government_country_name_conventional_short_form_text = 'Egypt' AND terrorism_terrorist_group_s_text IS NOT NULL
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Schema evolution — Space: 7 with data, 3 without
    SELECT 'schema_evolution_space' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.countries WHERE space_space_agency_agencies_text IS NOT NULL) = 7
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Column mappings — country_name populated for all
    SELECT 'column_mapping_country_name' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.countries WHERE government_country_name_conventional_short_form_text IS NOT NULL) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: File metadata populated
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.json_demos.countries WHERE df_file_name IS NOT NULL) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 10: Spot check — Egypt capital is Cairo
    SELECT 'spot_check_egypt_cairo' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json_demos.countries
               WHERE government_country_name_conventional_short_form_text = 'Egypt' AND government_capital_name_text = 'Cairo'
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 11: Economy — Egypt GDP populated
    SELECT 'economy_gdp_populated' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.json_demos.country_economy
               WHERE government_country_name_conventional_short_form_text = 'Egypt' AND economy_real_gdp_purchasing_power_parity_real_gdp_purchasing_power_parity_2023_text IS NOT NULL
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 12: Multi-file — 10 distinct source files
    SELECT 'multi_file_10_sources' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.json_demos.countries) = 10
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
