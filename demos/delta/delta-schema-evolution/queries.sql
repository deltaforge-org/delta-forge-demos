-- ============================================================================
-- Delta Schema Evolution — Add Columns & NULL Filling — Educational Queries
-- ============================================================================
-- WHAT: Schema evolution allows adding new columns to a Delta table via
--       ALTER TABLE ADD COLUMN, without rewriting existing data files.
-- WHY:  Data models change over time. Without schema evolution, adding a
--       column would require dropping and recreating the table. Delta handles
--       this seamlessly — old Parquet files are read with NULLs for new columns.
-- HOW:  When you ADD COLUMN, Delta updates only the schema metadata in the
--       transaction log. Old Parquet files are NOT rewritten. When those files
--       are read, the engine returns NULL for columns that don't exist in the file.
--
-- Starting state: 30 rows, 4 columns (id, first_name, last_name, email)
-- Final state:    50 rows, 7 columns (+ phone, city, signup_date)
--   - Rows 1-10:  original + backfilled phone, NULL city/signup_date
--   - Rows 11-30: original, NULL phone/city/signup_date
--   - Rows 31-50: fully populated (all 7 columns)
-- ============================================================================


-- ============================================================================
-- BASELINE: Confirm the initial 4-column schema before evolution
-- ============================================================================
-- The table currently has 30 rows with only id, first_name, last_name, email.

ASSERT ROW_COUNT = 3
SELECT id, first_name, last_name, email
FROM {{zone_name}}.delta_demos.contacts
WHERE id IN (1, 15, 30)
ORDER BY id;


-- ============================================================================
-- PHASE 1: Add 3 new columns — existing rows get NULL automatically
-- ============================================================================
-- Each ALTER TABLE ADD COLUMN updates only the Delta transaction log metadata.
-- Old Parquet files are NOT rewritten. When those files are later read, the
-- engine returns NULL for the new columns that don't exist in the file.

ALTER TABLE {{zone_name}}.delta_demos.contacts ADD COLUMN phone VARCHAR;
ALTER TABLE {{zone_name}}.delta_demos.contacts ADD COLUMN city VARCHAR;
ALTER TABLE {{zone_name}}.delta_demos.contacts ADD COLUMN signup_date VARCHAR;


-- ============================================================================
-- OBSERVE: NULL filling in action
-- ============================================================================
-- Now the table has 7 columns, but the original 30 rows have NULLs for
-- phone, city, and signup_date because those columns did not exist when
-- the rows were written.

ASSERT ROW_COUNT = 3
SELECT id, first_name, last_name,
       CASE WHEN phone IS NULL THEN '(NULL)' ELSE phone END AS phone,
       CASE WHEN city IS NULL THEN '(NULL)' ELSE city END AS city,
       CASE WHEN signup_date IS NULL THEN '(NULL)' ELSE signup_date END AS signup_date
FROM {{zone_name}}.delta_demos.contacts
WHERE id IN (1, 15, 30)
ORDER BY id;


-- ============================================================================
-- PHASE 2: Insert 20 new contacts with all 7 columns populated
-- ============================================================================
-- These rows are written to new Parquet files that include the evolved schema.
-- They will have all columns populated — no NULLs.

ASSERT ROW_COUNT = 20
INSERT INTO {{zone_name}}.delta_demos.contacts VALUES
    (31, 'Elena',    'Foster',    'elena.foster@example.com',    '+1-555-0131', 'New York',      '2024-03-01'),
    (32, 'Felix',    'Reed',      'felix.reed@example.com',      '+1-555-0132', 'Los Angeles',   '2024-03-02'),
    (33, 'Gina',     'Cook',      'gina.cook@example.com',       '+1-555-0133', 'Chicago',       '2024-03-03'),
    (34, 'Hugo',     'Morgan',    'hugo.morgan@example.com',     '+1-555-0134', 'Houston',       '2024-03-04'),
    (35, 'Isla',     'Bell',      'isla.bell@example.com',       '+1-555-0135', 'Phoenix',       '2024-03-05'),
    (36, 'Jake',     'Murphy',    'jake.murphy@example.com',     '+1-555-0136', 'San Antonio',   '2024-03-06'),
    (37, 'Kara',     'Rivera',    'kara.rivera@example.com',     '+1-555-0137', 'San Diego',     '2024-03-07'),
    (38, 'Liam',     'Cooper',    'liam.cooper@example.com',     '+1-555-0138', 'Dallas',        '2024-03-08'),
    (39, 'Mia',      'Bailey',    'mia.bailey@example.com',      '+1-555-0139', 'San Jose',      '2024-03-09'),
    (40, 'Noah',     'Howard',    'noah.howard@example.com',     '+1-555-0140', 'Austin',        '2024-03-10'),
    (41, 'Olive',    'Ward',      'olive.ward@example.com',      '+1-555-0141', 'Seattle',       '2024-03-11'),
    (42, 'Pete',     'Torres',    'pete.torres@example.com',     '+1-555-0142', 'Denver',        '2024-03-12'),
    (43, 'Rosa',     'Peterson',  'rosa.peterson@example.com',   '+1-555-0143', 'Boston',        '2024-03-13'),
    (44, 'Sean',     'Gray',      'sean.gray@example.com',       '+1-555-0144', 'Nashville',     '2024-03-14'),
    (45, 'Tara',     'Ramirez',   'tara.ramirez@example.com',    '+1-555-0145', 'Portland',      '2024-03-15'),
    (46, 'Uri',      'James',     'uri.james@example.com',       '+1-555-0146', 'Memphis',       '2024-03-16'),
    (47, 'Vera',     'Watson',    'vera.watson@example.com',     '+1-555-0147', 'Louisville',    '2024-03-17'),
    (48, 'Will',     'Brooks',    'will.brooks@example.com',     '+1-555-0148', 'Baltimore',     '2024-03-18'),
    (49, 'Xena',     'Kelly',     'xena.kelly@example.com',      '+1-555-0149', 'Milwaukee',     '2024-03-19'),
    (50, 'Yuri',     'Price',     'yuri.price@example.com',      '+1-555-0150', 'Tucson',        '2024-03-20');


-- ============================================================================
-- OBSERVE: Contrast old rows (NULL-filled) vs. new rows (fully populated)
-- ============================================================================
-- Picking one representative from each group shows the layered pattern:
--   id=1  — original row, all new columns are NULL
--   id=11 — original row, all new columns are NULL
--   id=31 — post-evolution insert, fully populated

ASSERT ROW_COUNT = 3
SELECT id, first_name, last_name,
       CASE WHEN phone IS NULL THEN '(NULL)' ELSE phone END AS phone,
       CASE WHEN city IS NULL THEN '(NULL)' ELSE city END AS city,
       CASE WHEN signup_date IS NULL THEN '(NULL)' ELSE signup_date END AS signup_date
FROM {{zone_name}}.delta_demos.contacts
WHERE id IN (1, 11, 31)
ORDER BY id;


-- ============================================================================
-- PHASE 3: Backfill phone for first 10 contacts
-- ============================================================================
-- In practice, you may be able to recover some historical data for new columns.
-- Here we backfill phone numbers for the earliest 10 contacts. The UPDATE
-- rewrites those rows into new Parquet files that include the phone column.
-- The old files are marked as removed in the Delta transaction log.
--
-- Note: city and signup_date are left as NULL — this mirrors real-world patterns
-- where you can only recover some historical data.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0101' WHERE id = 1;
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0102' WHERE id = 2;
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0103' WHERE id = 3;
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0104' WHERE id = 4;
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0105' WHERE id = 5;
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0106' WHERE id = 6;
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0107' WHERE id = 7;
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0108' WHERE id = 8;
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0109' WHERE id = 9;
UPDATE {{zone_name}}.delta_demos.contacts SET phone = '+1-555-0110' WHERE id = 10;


-- ============================================================================
-- OBSERVE: Backfilled rows now have phone but still NULL city/signup_date
-- ============================================================================

ASSERT ROW_COUNT = 10
SELECT id, first_name, phone,
       CASE WHEN city IS NULL THEN '(NULL)' ELSE city END AS city,
       CASE WHEN signup_date IS NULL THEN '(NULL)' ELSE signup_date END AS signup_date
FROM {{zone_name}}.delta_demos.contacts
WHERE id BETWEEN 1 AND 10
ORDER BY id;


-- ============================================================================
-- LEARN: Three Distinct Row Groups After Schema Evolution
-- ============================================================================
-- Schema evolution creates a layered pattern in the data:
--
-- Group 1 (ids 1-10):  Original rows + backfilled phone. city/signup_date = NULL
-- Group 2 (ids 11-30): Original rows, never backfilled. phone/city/signup_date = NULL
-- Group 3 (ids 31-50): Inserted AFTER columns were added. Fully populated.
--
-- This layering is natural in production systems where schemas evolve gradually.

ASSERT ROW_COUNT = 3
ASSERT VALUE row_count = 10 WHERE row_group = 'Group 1: Original + backfilled phone'
ASSERT VALUE has_phone = 10 WHERE row_group = 'Group 1: Original + backfilled phone'
ASSERT VALUE has_city = 0 WHERE row_group = 'Group 1: Original + backfilled phone'
ASSERT VALUE row_count = 20 WHERE row_group = 'Group 2: Original, no backfill'
ASSERT VALUE has_phone = 0 WHERE row_group = 'Group 2: Original, no backfill'
ASSERT VALUE row_count = 20 WHERE row_group = 'Group 3: Post-evolution inserts'
ASSERT VALUE has_phone = 20 WHERE row_group = 'Group 3: Post-evolution inserts'
ASSERT VALUE has_city = 20 WHERE row_group = 'Group 3: Post-evolution inserts'
SELECT
    CASE
        WHEN id BETWEEN 1 AND 10 THEN 'Group 1: Original + backfilled phone'
        WHEN id BETWEEN 11 AND 30 THEN 'Group 2: Original, no backfill'
        ELSE 'Group 3: Post-evolution inserts'
    END AS row_group,
    COUNT(*) AS row_count,
    COUNT(phone) AS has_phone,
    COUNT(city) AS has_city,
    COUNT(signup_date) AS has_signup_date
FROM {{zone_name}}.delta_demos.contacts
GROUP BY CASE
    WHEN id BETWEEN 1 AND 10 THEN 'Group 1: Original + backfilled phone'
    WHEN id BETWEEN 11 AND 30 THEN 'Group 2: Original, no backfill'
    ELSE 'Group 3: Post-evolution inserts'
END
ORDER BY row_group;


-- ============================================================================
-- LEARN: Why NULLs Appear — The Parquet File Story
-- ============================================================================
-- When you run ALTER TABLE ADD COLUMN, Delta does NOT rewrite old Parquet files.
-- Instead, it updates the schema in the _delta_log metadata. When old files
-- are read, the engine returns NULL for columns that don't exist in the file.
--
-- This is why schema evolution is fast: it is a metadata-only operation.
-- The cost comes later when you query — the engine must handle the NULL filling.
-- Backfill UPDATEs (like what we did for phone on ids 1-10) rewrite the
-- affected Parquet files with the new column populated.

ASSERT ROW_COUNT = 1
ASSERT VALUE null_phone_rows = 20
ASSERT VALUE null_city_rows = 30
ASSERT VALUE null_signup_rows = 30
ASSERT VALUE total_rows = 50
SELECT COUNT(*) FILTER (WHERE phone IS NULL) AS null_phone_rows,
       COUNT(*) FILTER (WHERE city IS NULL) AS null_city_rows,
       COUNT(*) FILTER (WHERE signup_date IS NULL) AS null_signup_rows,
       COUNT(*) AS total_rows
FROM {{zone_name}}.delta_demos.contacts;


-- ============================================================================
-- EXPLORE: Fully Populated Post-Evolution Rows
-- ============================================================================
-- Rows inserted after the schema was evolved have all 7 columns populated.
-- These rows were written to Parquet files that include the new column schema.

ASSERT ROW_COUNT = 5
SELECT id, first_name, last_name, email, phone, city, signup_date
FROM {{zone_name}}.delta_demos.contacts
WHERE id BETWEEN 31 AND 35
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 50
ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.delta_demos.contacts;

-- Verify 20 rows have NULL phone (ids 11-30 never backfilled)
ASSERT VALUE null_phone_count = 20
SELECT COUNT(*) FILTER (WHERE phone IS NULL) AS null_phone_count FROM {{zone_name}}.delta_demos.contacts;

-- Verify 30 rows have NULL city (ids 1-30 never got city)
ASSERT VALUE null_city_count = 30
SELECT COUNT(*) FILTER (WHERE city IS NULL) AS null_city_count FROM {{zone_name}}.delta_demos.contacts;

-- Verify 30 rows have NULL signup_date
ASSERT VALUE null_signup_date_count = 30
SELECT COUNT(*) FILTER (WHERE signup_date IS NULL) AS null_signup_date_count FROM {{zone_name}}.delta_demos.contacts;

-- Verify all 10 backfilled rows (ids 1-10) have phone populated
ASSERT VALUE backfilled_phone_count = 10
SELECT COUNT(*) FILTER (WHERE id BETWEEN 1 AND 10 AND phone IS NOT NULL) AS backfilled_phone_count FROM {{zone_name}}.delta_demos.contacts;

-- Verify Alice's backfilled phone number
ASSERT VALUE phone = '+1-555-0101'
SELECT phone FROM {{zone_name}}.delta_demos.contacts WHERE id = 1;

-- Verify Elena (id=31) is fully populated with correct values
ASSERT VALUE elena_count = 1
SELECT COUNT(*) AS elena_count FROM {{zone_name}}.delta_demos.contacts
WHERE id = 31 AND first_name = 'Elena' AND phone = '+1-555-0131'
  AND city = 'New York' AND signup_date = '2024-03-01';

-- Verify all post-evolution rows (ids 31-50) are fully populated
ASSERT VALUE fully_populated_rows = 20
SELECT COUNT(*) AS fully_populated_rows FROM {{zone_name}}.delta_demos.contacts
WHERE id BETWEEN 31 AND 50
  AND phone IS NOT NULL AND city IS NOT NULL AND signup_date IS NOT NULL;
