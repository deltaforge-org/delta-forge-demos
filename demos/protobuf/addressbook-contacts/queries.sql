-- ============================================================================
-- Protobuf Address Book Contacts — Verification Queries
-- ============================================================================
-- Each query verifies a specific protobuf feature: nested message flattening,
-- repeated field handling, enum decoding, timestamp conversion, sparse data,
-- multi-file reading, and file metadata.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL CONTACT COUNT — 5 + 5 + 3 = 13 people across 3 team files
-- ============================================================================

ASSERT ROW_COUNT = 13
SELECT *
FROM {{zone_name}}.protobuf_demos.contacts;


-- ============================================================================
-- 2. BROWSE CONTACTS — See flattened data with friendly column names
-- ============================================================================

ASSERT ROW_COUNT = 13
ASSERT VALUE contact_name = 'Alice Chen' WHERE contact_id = 1001
ASSERT VALUE contact_name = 'Luis Hernandez' WHERE contact_id = 3002
ASSERT VALUE email = 'bob.martinez@example.com' WHERE contact_id = 1002
SELECT contact_id, contact_name, email, phone_numbers, phone_types, last_updated
FROM {{zone_name}}.protobuf_demos.contacts
ORDER BY contact_id;


-- ============================================================================
-- 3. EXPLODED PHONE COUNT — 9 + 9 + 4 = 22 phone number rows
-- ============================================================================
-- Each PhoneNumber within each Person becomes its own row.

ASSERT ROW_COUNT = 22
SELECT *
FROM {{zone_name}}.protobuf_demos.contact_phones;


-- ============================================================================
-- 4. BROWSE CONTACT PHONES — Exploded view with one row per phone
-- ============================================================================

ASSERT ROW_COUNT = 22
SELECT contact_id, contact_name, phone_number, phone_type
FROM {{zone_name}}.protobuf_demos.contact_phones
ORDER BY contact_id, phone_type;


-- ============================================================================
-- 5. ENUM DECODING — PhoneType decoded to string labels
-- ============================================================================
-- PhoneType enum: MOBILE=0, HOME=1, WORK=2 should appear as string labels.

ASSERT VALUE distinct_types = 3
SELECT COUNT(DISTINCT phone_type) AS distinct_types
FROM {{zone_name}}.protobuf_demos.contact_phones;


-- ============================================================================
-- 6. PHONE TYPE DISTRIBUTION — Count by MOBILE, HOME, WORK
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE count = 11 WHERE phone_type = 'MOBILE'
ASSERT VALUE count = 8 WHERE phone_type = 'WORK'
ASSERT VALUE count = 3 WHERE phone_type = 'HOME'
SELECT phone_type,
       COUNT(*) AS count
FROM {{zone_name}}.protobuf_demos.contact_phones
GROUP BY phone_type
ORDER BY count DESC;


-- ============================================================================
-- 7. REPEATED FIELD JOIN — phone_numbers column has comma-joined values
-- ============================================================================
-- Contacts with multiple phones should have commas in phone_numbers column.

ASSERT VALUE multi_phone_contacts = 7
SELECT COUNT(*) FILTER (WHERE phone_numbers LIKE '%,%') AS multi_phone_contacts
FROM {{zone_name}}.protobuf_demos.contacts;


-- ============================================================================
-- 8. SPARSE DATA — Luis Hernandez has no phone numbers (empty repeated)
-- ============================================================================
-- Luis (id=3002) has an empty phones list. In contacts table, phone_numbers
-- should be NULL or empty. In contact_phones, he should have zero rows.

ASSERT VALUE luis_phone_count = 0
SELECT COUNT(*) AS luis_phone_count
FROM {{zone_name}}.protobuf_demos.contact_phones
WHERE contact_id = 3002;


-- ============================================================================
-- 9. SPARSE DATA — Maria Schmidt has no email (empty string field)
-- ============================================================================

ASSERT VALUE maria_no_email = 1
SELECT COUNT(*) AS maria_no_email
FROM {{zone_name}}.protobuf_demos.contacts
WHERE contact_id = 3003
  AND (email IS NULL OR email = '');


-- ============================================================================
-- 10. TIMESTAMP CONVERSION — last_updated should be valid ISO 8601 dates
-- ============================================================================
-- All 13 contacts have last_updated set; values should be non-NULL strings
-- starting with a year (e.g., "2024-" or "2025-").

ASSERT VALUE timestamp_count = 13
SELECT COUNT(*) FILTER (WHERE last_updated IS NOT NULL) AS timestamp_count
FROM {{zone_name}}.protobuf_demos.contacts;


-- ============================================================================
-- 11. MULTI-FILE READING — 3 distinct source files
-- ============================================================================

ASSERT VALUE file_count = 3
SELECT COUNT(DISTINCT df_file_name) AS file_count
FROM {{zone_name}}.protobuf_demos.contacts;


-- ============================================================================
-- 12. CONTACTS PER FILE — verify team distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE contact_count = 5 WHERE df_file_name LIKE '%engineering%'
ASSERT VALUE contact_count = 5 WHERE df_file_name LIKE '%sales%'
ASSERT VALUE contact_count = 3 WHERE df_file_name LIKE '%executives%'
SELECT df_file_name, COUNT(*) AS contact_count
FROM {{zone_name}}.protobuf_demos.contacts
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 13. PHONES PER CONTACT — verify repeated field cardinality
-- ============================================================================

ASSERT ROW_COUNT = 12
ASSERT VALUE phone_count = 3 WHERE contact_name = 'Bob Martinez'
ASSERT VALUE phone_count = 3 WHERE contact_name = 'Ingrid Svensson'
ASSERT VALUE phone_count = 3 WHERE contact_name = 'Katherine Park'
ASSERT VALUE phone_count = 1 WHERE contact_name = 'Carol Nakamura'
ASSERT VALUE phone_count = 1 WHERE contact_name = 'Maria Schmidt'
SELECT contact_name, COUNT(*) AS phone_count
FROM {{zone_name}}.protobuf_demos.contact_phones
GROUP BY contact_name
ORDER BY phone_count DESC, contact_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: contact count, phone rows, enum decoding,
-- repeated-field join, sparse data handling, timestamps, and file metadata.

ASSERT ROW_COUNT = 9
ASSERT VALUE result = 'PASS' WHERE check_name = 'contact_count_13'
ASSERT VALUE result = 'PASS' WHERE check_name = 'phone_rows_22'
ASSERT VALUE result = 'PASS' WHERE check_name = 'enum_three_types'
ASSERT VALUE result = 'PASS' WHERE check_name = 'repeated_join_commas'
ASSERT VALUE result = 'PASS' WHERE check_name = 'sparse_no_phones_luis'
ASSERT VALUE result = 'PASS' WHERE check_name = 'sparse_no_email_maria'
ASSERT VALUE result = 'PASS' WHERE check_name = 'timestamps_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'three_source_files'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_metadata_populated'
SELECT check_name, result FROM (

    -- Check 1: Total contacts = 13
    SELECT 'contact_count_13' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.protobuf_demos.contacts) = 13
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Exploded phone rows = 22
    SELECT 'phone_rows_22' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.protobuf_demos.contact_phones) = 22
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 3 distinct phone types (enum decoding)
    SELECT 'enum_three_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT phone_type) FROM {{zone_name}}.protobuf_demos.contact_phones) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Multi-phone contacts have commas (repeated join)
    SELECT 'repeated_join_commas' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf_demos.contacts
               WHERE phone_numbers LIKE '%,%'
           ) = 7 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Sparse — Luis has no phone rows
    SELECT 'sparse_no_phones_luis' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf_demos.contact_phones
               WHERE contact_id = 3002
           ) = 0 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Sparse — Maria has no email
    SELECT 'sparse_no_email_maria' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf_demos.contacts
               WHERE contact_id = 3003 AND (email IS NULL OR email = '')
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Timestamps populated for all contacts
    SELECT 'timestamps_populated' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf_demos.contacts
               WHERE last_updated IS NOT NULL
           ) = 13 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: 3 source files
    SELECT 'three_source_files' AS check_name,
           CASE WHEN (
               SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.protobuf_demos.contacts
           ) = 3 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: File metadata populated for all rows
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.protobuf_demos.contacts
               WHERE df_file_name IS NOT NULL
           ) = 13 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
