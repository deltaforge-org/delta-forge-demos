-- ============================================================================
-- Delta UPDATE String Cleansing — CRM Data Normalization — Educational Queries
-- ============================================================================
-- WHAT: Shows how UPDATE SET clauses with string functions (TRIM, LOWER, LPAD,
--       UPPER, ||) clean messy data imported from a legacy CRM system.
-- WHY:  Real-world data imports contain inconsistent casing, extra whitespace,
--       unpadded codes, and missing derived fields. String UPDATEs fix these
--       quality issues in place without re-importing.
-- HOW:  Five UPDATE passes normalize 25 customer records. These queries observe
--       the cleaned results and verify data quality metrics.
-- ============================================================================


-- ============================================================================
-- BASELINE: All Cleaned Customer Records
-- ============================================================================
-- After all five UPDATE passes, every row should have:
--   - Trimmed first/last names (no leading/trailing spaces)
--   - Lowercase email addresses
--   - 6-digit zero-padded account codes
--   - Populated full_name (first || ' ' || last)
--   - Uppercase country codes

ASSERT ROW_COUNT = 25
ASSERT VALUE first_name = 'Alice' WHERE id = 1
ASSERT VALUE email = 'alice.johnson@gmail.com' WHERE id = 1
ASSERT VALUE account_code = '000042' WHERE id = 1
ASSERT VALUE full_name = 'Alice Johnson' WHERE id = 1
ASSERT VALUE country_code = 'US' WHERE id = 1
SELECT id, first_name, last_name, full_name, email, account_code, country_code
FROM {{zone_name}}.delta_demos.customer_imports
ORDER BY id;


-- ============================================================================
-- OBSERVE: Email Normalization — All Addresses Lowercased
-- ============================================================================
-- LOWER() converted mixed-case emails like 'DIANA.BROWN@Hotmail.com' to
-- 'diana.brown@hotmail.com'. This query confirms every email is now
-- consistently lowercase — essential for deduplication and matching.

ASSERT ROW_COUNT = 25
ASSERT VALUE email = 'bob.smith@yahoo.com' WHERE id = 2
ASSERT VALUE email = 'george.wilson@outlook.com' WHERE id = 7
ASSERT VALUE email = 'ivan.taylor@yahoo.com' WHERE id = 9
SELECT id, full_name, email
FROM {{zone_name}}.delta_demos.customer_imports
ORDER BY email;


-- ============================================================================
-- OBSERVE: Account Code Padding — All Codes Are 6 Characters
-- ============================================================================
-- LPAD(account_code, 6, '0') transformed short codes like '7' into '000007'
-- and '4567' into '004567'. This query verifies uniform 6-character length
-- across all 25 records.

ASSERT ROW_COUNT = 25
ASSERT VALUE account_code = '000007' WHERE id = 2
ASSERT VALUE account_code = '000001' WHERE id = 9
ASSERT VALUE account_code = '004567' WHERE id = 7
ASSERT VALUE account_code = '006789' WHERE id = 21
SELECT id, full_name, account_code
FROM {{zone_name}}.delta_demos.customer_imports
ORDER BY account_code;


-- ============================================================================
-- OBSERVE: Full Name Concatenation — Derived from Components
-- ============================================================================
-- The full_name column was empty ('') on import. After trimming first/last
-- names, concatenation via || safely produces clean results like
-- 'George Wilson' instead of '  George  Wilson '.

ASSERT ROW_COUNT = 25
ASSERT VALUE full_name = 'George Wilson' WHERE id = 7
ASSERT VALUE full_name = 'Sam Lee' WHERE id = 19
ASSERT VALUE full_name = 'Patricia Garcia' WHERE id = 16
SELECT id, first_name, last_name, full_name
FROM {{zone_name}}.delta_demos.customer_imports
ORDER BY full_name;


-- ============================================================================
-- OBSERVE: Country Code Uppercasing — ISO Standard Format
-- ============================================================================
-- UPPER() converted lowercase codes ('us', 'uk', 'de', 'ca') to their
-- ISO 3166-1 alpha-2 standard form ('US', 'UK', 'DE', 'CA').

ASSERT ROW_COUNT = 4
ASSERT VALUE customer_count = 11 WHERE country_code = 'US'
ASSERT VALUE customer_count = 5 WHERE country_code = 'UK'
ASSERT VALUE customer_count = 5 WHERE country_code = 'DE'
ASSERT VALUE customer_count = 4 WHERE country_code = 'CA'
SELECT country_code,
       COUNT(*) AS customer_count
FROM {{zone_name}}.delta_demos.customer_imports
GROUP BY country_code
ORDER BY customer_count DESC;


-- ============================================================================
-- LEARN: Data Quality Scorecard — Post-Cleansing Metrics
-- ============================================================================
-- After cleansing, every quality metric should read 25/25 (100%).
-- Each sub-query counts rows that pass a specific quality check.

ASSERT ROW_COUNT = 1
ASSERT VALUE trimmed_names = 25
ASSERT VALUE lowercase_emails = 25
ASSERT VALUE padded_codes = 25
ASSERT VALUE populated_full_names = 25
ASSERT VALUE uppercase_country = 25
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.customer_imports
     WHERE first_name = TRIM(first_name) AND last_name = TRIM(last_name)) AS trimmed_names,
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.customer_imports
     WHERE email = LOWER(email)) AS lowercase_emails,
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.customer_imports
     WHERE LENGTH(account_code) = 6) AS padded_codes,
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.customer_imports
     WHERE full_name = first_name || ' ' || last_name) AS populated_full_names,
    (SELECT COUNT(*) FROM {{zone_name}}.delta_demos.customer_imports
     WHERE country_code = UPPER(country_code)) AS uppercase_country;


-- ============================================================================
-- EXPLORE: Customers by City and Country
-- ============================================================================
-- Cross-reference cleaned country codes with cities to verify geographic
-- consistency. All 25 cities should be distinct.

ASSERT ROW_COUNT = 25
ASSERT VALUE country_code = 'US' WHERE city = 'New York'
ASSERT VALUE country_code = 'CA' WHERE city = 'San Antonio'
SELECT city, country_code, full_name, email
FROM {{zone_name}}.delta_demos.customer_imports
ORDER BY country_code, city;


-- ============================================================================
-- EXPLORE: Account Code Distribution — Before vs After Pattern
-- ============================================================================
-- Show the range of original code lengths (1-4 digits) now uniformly
-- padded to 6 characters. The sorted order demonstrates zero-padding
-- produces correct lexicographic sorting.

ASSERT ROW_COUNT = 5
SELECT id, full_name, account_code
FROM {{zone_name}}.delta_demos.customer_imports
ORDER BY account_code
LIMIT 5;


-- ============================================================================
-- LEARN: Observing the Update Chain via DESCRIBE HISTORY
-- ============================================================================
-- The transaction log records each UPDATE as a separate version.
-- V0: CREATE TABLE, V1: INSERT, V2: TRIM names, V3: LOWER emails,
-- V4: LPAD codes, V5: CONCAT full_name, V6: UPPER country_code.

-- Non-deterministic: commit timestamps set at write time
ASSERT WARNING ROW_COUNT >= 7
DESCRIBE HISTORY {{zone_name}}.delta_demos.customer_imports;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total rows: 25 customers (no inserts or deletes, only updates)
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_imports;

-- Verify distinct emails: 25 unique addresses
ASSERT VALUE cnt = 25
SELECT COUNT(DISTINCT email) AS cnt FROM {{zone_name}}.delta_demos.customer_imports;

-- Verify distinct cities: 25 unique cities
ASSERT VALUE cnt = 25
SELECT COUNT(DISTINCT city) AS cnt FROM {{zone_name}}.delta_demos.customer_imports;

-- Verify distinct country codes: 4 countries
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT country_code) AS cnt FROM {{zone_name}}.delta_demos.customer_imports;

-- Verify all account codes are exactly 6 characters
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_imports WHERE LENGTH(account_code) = 6;

-- Verify no leading/trailing whitespace in names
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_imports
WHERE first_name = TRIM(first_name) AND last_name = TRIM(last_name);

-- Verify all full_names match concatenation of parts
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_imports
WHERE full_name = first_name || ' ' || last_name;

-- Verify all emails are lowercase
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_imports
WHERE email = LOWER(email);

-- Verify all country codes are uppercase
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.customer_imports
WHERE country_code = UPPER(country_code);
