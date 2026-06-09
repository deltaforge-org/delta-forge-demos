-- ============================================================================
-- Cleanup: Public Holiday Calendar Sync
-- ============================================================================
-- Reverse order of creation: bronze external table → silver delta
-- table → API endpoint → connection → credential → schema. WITH FILES
-- on both tables removes their on-disk artefacts. The zone is left
-- in place so sibling API demos (reference-catalog, future holiday
-- waves) keep working.
-- ============================================================================

-- 1. Bronze external table (removes the JSON pages INVOKE wrote)
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.holiday_calendar.public_holidays_bronze WITH FILES;

-- 2. Silver Delta table (removes its Delta log + parquet)
DROP DELTA TABLE IF EXISTS {{zone_name}}.holiday_calendar.country_holidays WITH FILES;

-- 3. API endpoint definition (cascades its run history)
DROP API ENDPOINT IF EXISTS {{zone_name}}.holiday_calendar.public_holidays;

-- 4. REST API connection
DROP CONNECTION IF EXISTS holiday_calendar;

-- 5. Credential vault entry (OS keychain backend is never dropped)
DROP CREDENTIAL IF EXISTS holiday_api_token;

-- 6. Schema (zone left in place — sibling API demos share `bronze`)

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'public-holiday-calendar-sync' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.holiday_calendar;
