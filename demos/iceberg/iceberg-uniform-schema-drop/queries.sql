-- ============================================================================
-- Iceberg UniForm Drop Columns (GDPR PII Removal) — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH DROP COLUMN
-- ------------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- When ALTER TABLE DROP COLUMN runs, Delta Forge:
--   1. Removes the column from the Delta schema in _delta_log/
--   2. Adds a new schema entry to metadata.json's "schemas" array
--      with the dropped column removed from the field list
--
-- Because column mapping mode is 'id', the underlying Parquet files still
-- contain the dropped column's data, but it is no longer mapped in the
-- schema. This means:
--   - New queries cannot access dropped columns
--   - Time travel to pre-drop versions still exposes the original schema
--   - Physical data can be vacuumed later for full GDPR erasure
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify schema changes in metadata with:
--   python3 verify_iceberg_metadata.py <table_data_path>/user_profiles -v
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — 20 Users, 9 Columns (Version 1)
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.user_profiles ORDER BY user_id;


-- ============================================================================
-- Query 1: Baseline — Per-Country Distribution (5 Each)
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE user_count = 5 WHERE country = 'DE'
ASSERT VALUE user_count = 5 WHERE country = 'JP'
ASSERT VALUE user_count = 5 WHERE country = 'UK'
ASSERT VALUE user_count = 5 WHERE country = 'US'
SELECT
    country,
    COUNT(*) AS user_count
FROM {{zone_name}}.iceberg_demos.user_profiles
GROUP BY country
ORDER BY country;


-- ============================================================================
-- Query 2: Baseline — Per-Tier Distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE user_count = 5 WHERE subscription_tier = 'enterprise'
ASSERT VALUE user_count = 7 WHERE subscription_tier = 'free'
ASSERT VALUE user_count = 8 WHERE subscription_tier = 'pro'
SELECT
    subscription_tier,
    COUNT(*) AS user_count
FROM {{zone_name}}.iceberg_demos.user_profiles
GROUP BY subscription_tier
ORDER BY subscription_tier;


-- ============================================================================
-- LEARN: GDPR Step 1 — Drop email Column (Version 2)
-- ============================================================================
-- DROP COLUMN is a metadata-only operation when column mapping mode is 'id'.
-- The Iceberg metadata.json gets a new schema entry with the email field
-- removed. The underlying Parquet files still contain email data but it is
-- no longer accessible through the current schema.

ALTER TABLE {{zone_name}}.iceberg_demos.user_profiles DROP COLUMN email;


-- ============================================================================
-- Query 3: Verify email Dropped — 20 Rows, 8 Columns
-- ============================================================================
-- All rows remain intact; only the column count decreases.

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.user_profiles ORDER BY user_id;


-- ============================================================================
-- LEARN: GDPR Step 2 — Drop phone Column (Version 3)
-- ============================================================================

ALTER TABLE {{zone_name}}.iceberg_demos.user_profiles DROP COLUMN phone;


-- ============================================================================
-- LEARN: GDPR Step 3 — Drop ip_address Column (Version 4)
-- ============================================================================

ALTER TABLE {{zone_name}}.iceberg_demos.user_profiles DROP COLUMN ip_address;


-- ============================================================================
-- Query 4: Verify All PII Dropped — 20 Rows, 6 Columns Remain
-- ============================================================================
-- Remaining columns: user_id, username, country, signup_date, last_login,
-- subscription_tier. Per-country and per-tier distributions are unchanged.

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.user_profiles ORDER BY user_id;


-- ============================================================================
-- Query 5: Per-Country Counts Unchanged After PII Removal
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE user_count = 5 WHERE country = 'DE'
ASSERT VALUE user_count = 5 WHERE country = 'JP'
ASSERT VALUE user_count = 5 WHERE country = 'UK'
ASSERT VALUE user_count = 5 WHERE country = 'US'
SELECT
    country,
    COUNT(*) AS user_count
FROM {{zone_name}}.iceberg_demos.user_profiles
GROUP BY country
ORDER BY country;


-- ============================================================================
-- Query 6: Per-Tier Counts Unchanged After PII Removal
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE user_count = 5 WHERE subscription_tier = 'enterprise'
ASSERT VALUE user_count = 7 WHERE subscription_tier = 'free'
ASSERT VALUE user_count = 8 WHERE subscription_tier = 'pro'
SELECT
    subscription_tier,
    COUNT(*) AS user_count
FROM {{zone_name}}.iceberg_demos.user_profiles
GROUP BY subscription_tier
ORDER BY subscription_tier;


-- ============================================================================
-- LEARN: Insert 4 New Users With 6-Column Schema (Version 5)
-- ============================================================================
-- New users added after PII columns were dropped. These rows never contain
-- email, phone, or ip_address — the columns simply do not exist.

INSERT INTO {{zone_name}}.iceberg_demos.user_profiles VALUES
    (21, 'cnguyen',   'US', '2025-01-10', '2025-03-25', 'pro'),
    (22, 'oconnor',   'UK', '2025-01-15', '2025-03-26', 'free'),
    (23, 'fkrause',   'DE', '2025-02-01', '2025-03-24', 'enterprise'),
    (24, 'rnakamura', 'JP', '2025-02-10', '2025-03-27', 'pro');


-- ============================================================================
-- Query 7: Verify 24 Total Users, 6 Per Country
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE user_count = 6 WHERE country = 'DE'
ASSERT VALUE user_count = 6 WHERE country = 'JP'
ASSERT VALUE user_count = 6 WHERE country = 'UK'
ASSERT VALUE user_count = 6 WHERE country = 'US'
SELECT
    country,
    COUNT(*) AS user_count
FROM {{zone_name}}.iceberg_demos.user_profiles
GROUP BY country
ORDER BY country;


-- ============================================================================
-- Query 8: Updated Tier Distribution (24 Users)
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE user_count = 6 WHERE subscription_tier = 'enterprise'
ASSERT VALUE user_count = 8 WHERE subscription_tier = 'free'
ASSERT VALUE user_count = 10 WHERE subscription_tier = 'pro'
SELECT
    subscription_tier,
    COUNT(*) AS user_count
FROM {{zone_name}}.iceberg_demos.user_profiles
GROUP BY subscription_tier
ORDER BY subscription_tier;


-- ============================================================================
-- Query 9: Time Travel — Version 1 Still Has All 9 Columns
-- ============================================================================
-- Reading the pre-drop version exposes the original schema including all PII
-- columns. This is expected behavior: time travel preserves historical state.
-- For full GDPR erasure, VACUUM must be run after the retention period.

ASSERT ROW_COUNT = 20
SELECT
    user_id, username, email, phone, ip_address, country
FROM {{zone_name}}.iceberg_demos.user_profiles VERSION AS OF 1
ORDER BY user_id;


-- ============================================================================
-- VERIFY: Comprehensive Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_users = 24
ASSERT VALUE country_count = 4
ASSERT VALUE free_count = 8
ASSERT VALUE pro_count = 10
ASSERT VALUE enterprise_count = 6
SELECT
    COUNT(*) AS total_users,
    COUNT(DISTINCT country) AS country_count,
    COUNT(*) FILTER (WHERE subscription_tier = 'free') AS free_count,
    COUNT(*) FILTER (WHERE subscription_tier = 'pro') AS pro_count,
    COUNT(*) FILTER (WHERE subscription_tier = 'enterprise') AS enterprise_count
FROM {{zone_name}}.iceberg_demos.user_profiles;


-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents the 6-column schema after PII
-- columns were dropped.
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.user_profiles_iceberg
USING ICEBERG
LOCATION '{{data_path}}/user_profiles';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.user_profiles_iceberg TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg_demos.user_profiles_iceberg;


-- ============================================================================
-- Iceberg Verify 1: Row Count — 24 Users
-- ============================================================================

ASSERT ROW_COUNT = 24
SELECT * FROM {{zone_name}}.iceberg_demos.user_profiles_iceberg ORDER BY user_id;


-- ============================================================================
-- Iceberg Verify 2: 24 Rows, Per-Country = 6 Each
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE user_count = 6 WHERE country = 'DE'
ASSERT VALUE user_count = 6 WHERE country = 'JP'
ASSERT VALUE user_count = 6 WHERE country = 'UK'
ASSERT VALUE user_count = 6 WHERE country = 'US'
SELECT
    country,
    COUNT(*) AS user_count
FROM {{zone_name}}.iceberg_demos.user_profiles_iceberg
GROUP BY country
ORDER BY country;


-- ============================================================================
-- Iceberg Verify 3: Tier Distribution Matches Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE user_count = 6 WHERE subscription_tier = 'enterprise'
ASSERT VALUE user_count = 8 WHERE subscription_tier = 'free'
ASSERT VALUE user_count = 10 WHERE subscription_tier = 'pro'
SELECT
    subscription_tier,
    COUNT(*) AS user_count
FROM {{zone_name}}.iceberg_demos.user_profiles_iceberg
GROUP BY subscription_tier
ORDER BY subscription_tier;
