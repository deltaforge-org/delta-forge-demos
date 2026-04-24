-- ============================================================================
-- JSON Country Factbook — Setup Script
-- ============================================================================
-- Creates two external tables from 10 CIA World Factbook country JSON files:
--   1. countries       — Flattened overview: one row per country (10 rows)
--   2. country_economy — Economy-focused extraction with more granular data
--
-- Demonstrates:
--   - Deep nesting (3+ levels): $.Geography.Area.total .text
--   - include_paths: selective extraction from 13 top-level sections
--   - exclude_paths: skip verbose Introduction/Background HTML text
--   - column_mappings: deep paths → friendly column names
--   - Schema evolution: Terrorism and Space sections are optional (NULL fill)
--   - Multi-file reading: 10 .json files (one per country)
--   - file_metadata: df_file_name reveals country code (eg.json, sf.json...)
--   - max_depth: control flattening depth on complex documents
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.json_demos
    COMMENT 'JSON-backed external tables';

-- ============================================================================
-- TABLE 1: countries — Flattened overview, one row per country (10 total)
-- ============================================================================
-- Extracts key fields from Geography, People, Government, and optional
-- Terrorism/Space sections. The verbose Introduction.Background HTML text
-- is excluded.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json_demos.countries
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.Government.Country name.conventional short form.text",
            "$.Government.Capital.name.text",
            "$.Government.Government type.text",
            "$.Government.Independence.text",
            "$.Geography.Location.text",
            "$.Geography.Area.total .text",
            "$.Geography.Climate.text",
            "$.Geography.Terrain.text",
            "$.People and Society.Population.total.text",
            "$.People and Society.Languages.Languages.text",
            "$.People and Society.Religions.text",
            "$.Terrorism.Terrorist group(s).text",
            "$.Space.Space agency/agencies.text",
            "$.Space.Space program overview.text"
        ],
        "exclude_paths": [
            "$.Introduction.Background"
        ],
        "column_mappings": {
            "$.Government.Country name.conventional short form.text": "government_country_name_conventional_short_form_text",
            "$.Government.Capital.name.text": "government_capital_name_text",
            "$.Government.Government type.text": "government_government_type_text",
            "$.Government.Independence.text": "government_independence_text",
            "$.Geography.Location.text": "geography_location_text",
            "$.Geography.Area.total .text": "geography_area_total_text",
            "$.Geography.Climate.text": "geography_climate_text",
            "$.Geography.Terrain.text": "geography_terrain_text",
            "$.People and Society.Population.total.text": "people_and_society_population_total_text",
            "$.People and Society.Languages.Languages.text": "people_and_society_languages_languages_text",
            "$.People and Society.Religions.text": "people_and_society_religions_text",
            "$.Terrorism.Terrorist group(s).text": "terrorism_terrorist_group_s_text",
            "$.Space.Space agency/agencies.text": "space_space_agency_agencies_text",
            "$.Space.Space program overview.text": "space_space_program_overview_text"
        },
        "max_depth": 5,
        "separator": "_",
        "infer_types": false
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
-- ============================================================================
-- TABLE 2: country_economy — Economy-focused extraction (10 total)
-- ============================================================================
-- Extracts economic indicators from the deeply nested Economy section.
-- GDP, inflation, unemployment, and sector composition at 3+ levels deep.
-- Introduction.Background is excluded (verbose HTML). Uses column_mappings
-- for clean analytics-ready names.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.json_demos.country_economy
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.Government.Country name.conventional short form.text",
            "$.Economy.Economic overview.text",
            "$.Economy.Real GDP (purchasing power parity).Real GDP (purchasing power parity) 2023.text",
            "$.Economy.Real GDP growth rate.Real GDP growth rate 2023.text",
            "$.Economy.Real GDP per capita.Real GDP per capita 2023.text",
            "$.Economy.GDP (official exchange rate).text",
            "$.Economy.Inflation rate (consumer prices).Inflation rate (consumer prices) 2023.text",
            "$.Economy.GDP - composition, by sector of origin.agriculture.text",
            "$.Economy.GDP - composition, by sector of origin.industry.text",
            "$.Economy.GDP - composition, by sector of origin.services.text",
            "$.Economy.Agricultural products.text",
            "$.Economy.Industries.text",
            "$.Economy.Unemployment rate.Unemployment rate 2023.text",
            "$.Economy.Exports.Exports 2023.text",
            "$.Economy.Imports.Imports 2023.text"
        ],
        "exclude_paths": [
            "$.Introduction.Background"
        ],
        "column_mappings": {
            "$.Government.Country name.conventional short form.text": "government_country_name_conventional_short_form_text",
            "$.Economy.Economic overview.text": "economy_economic_overview_text",
            "$.Economy.Real GDP (purchasing power parity).Real GDP (purchasing power parity) 2023.text": "economy_real_gdp_purchasing_power_parity_real_gdp_purchasing_power_parity_2023_text",
            "$.Economy.Real GDP growth rate.Real GDP growth rate 2023.text": "economy_real_gdp_growth_rate_real_gdp_growth_rate_2023_text",
            "$.Economy.Real GDP per capita.Real GDP per capita 2023.text": "economy_real_gdp_per_capita_real_gdp_per_capita_2023_text",
            "$.Economy.GDP (official exchange rate).text": "economy_gdp_official_exchange_rate_text",
            "$.Economy.Inflation rate (consumer prices).Inflation rate (consumer prices) 2023.text": "economy_inflation_rate_consumer_prices_inflation_rate_consumer_prices_2023_text",
            "$.Economy.GDP - composition, by sector of origin.agriculture.text": "economy_gdp_composition_by_sector_of_origin_agriculture_text",
            "$.Economy.GDP - composition, by sector of origin.industry.text": "economy_gdp_composition_by_sector_of_origin_industry_text",
            "$.Economy.GDP - composition, by sector of origin.services.text": "economy_gdp_composition_by_sector_of_origin_services_text",
            "$.Economy.Agricultural products.text": "economy_agricultural_products_text",
            "$.Economy.Industries.text": "economy_industries_text",
            "$.Economy.Unemployment rate.Unemployment rate 2023.text": "economy_unemployment_rate_unemployment_rate_2023_text",
            "$.Economy.Exports.Exports 2023.text": "economy_exports_exports_2023_text",
            "$.Economy.Imports.Imports 2023.text": "economy_imports_imports_2023_text"
        },
        "max_depth": 5,
        "separator": "_",
        "infer_types": false
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
