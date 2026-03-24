-- ============================================================================
-- Delta UPDATE NULL Backfill — Patient Data Recovery — Educational Queries
-- ============================================================================
-- WHAT: Shows how UPDATE SET clauses with NULL-handling functions (COALESCE,
--       NULLIF, CASE WHEN IS NULL) replace missing values and sentinel data.
-- WHY:  Legacy database migrations often produce messy data — NULLs mixed with
--       sentinel values like -999, 'N/A', and empty strings. Cleaning these
--       in-place with UPDATE avoids costly ETL rewrites.
-- HOW:  Eight UPDATE passes transformed 25 patient records: NULLIF eliminated
--       numeric/string sentinels, COALESCE backfilled defaults, and CASE WHEN
--       conditionally zeroed unknown ages. These queries examine the results.
-- ============================================================================


-- ============================================================================
-- BASELINE: Sample of Cleaned Patient Records
-- ============================================================================
-- After all eight cleaning passes, here are the first 10 records.
-- Notice: no -999 ages, no 'N/A' contacts, no empty insurance codes.
-- Some columns (insurance_code, temperature, notes) remain NULL where
-- no default was assigned — intentionally left for downstream handling.
ASSERT ROW_COUNT = 10
SELECT id, patient_name, age, blood_type, emergency_contact,
       insurance_code, temperature, notes
FROM {{zone_name}}.backfill_demos.patient_records
ORDER BY id
LIMIT 10;


-- ============================================================================
-- OBSERVE: NULLs Remaining vs Cells Populated
-- ============================================================================
-- Of 150 checkable cells (25 rows x 6 cleaned columns), how many are
-- non-NULL after all passes? Columns checked: age, blood_type,
-- emergency_contact, insurance_code, temperature, notes.
ASSERT ROW_COUNT = 1
ASSERT VALUE non_null_cells = 127
ASSERT VALUE null_cells = 23
SELECT COUNT(age) + COUNT(blood_type) + COUNT(emergency_contact)
       + COUNT(insurance_code) + COUNT(temperature) + COUNT(notes)
       AS non_null_cells,
       (25 * 6) - (COUNT(age) + COUNT(blood_type) + COUNT(emergency_contact)
       + COUNT(insurance_code) + COUNT(temperature) + COUNT(notes))
       AS null_cells
FROM {{zone_name}}.backfill_demos.patient_records;


-- ============================================================================
-- OBSERVE: Sentinel Values Eliminated
-- ============================================================================
-- Confirm that no sentinel values remain in the data. Every -999, 'N/A',
-- and empty string should have been converted to NULL or a placeholder.
ASSERT ROW_COUNT = 1
ASSERT VALUE age_sentinels = 0
ASSERT VALUE temp_sentinels = 0
ASSERT VALUE ec_sentinels = 0
ASSERT VALUE ic_sentinels = 0
ASSERT VALUE notes_sentinels = 0
SELECT SUM(CASE WHEN age = -999 THEN 1 ELSE 0 END) AS age_sentinels,
       SUM(CASE WHEN temperature = -999.00 THEN 1 ELSE 0 END) AS temp_sentinels,
       SUM(CASE WHEN emergency_contact = 'N/A' THEN 1 ELSE 0 END) AS ec_sentinels,
       SUM(CASE WHEN insurance_code = '' THEN 1 ELSE 0 END) AS ic_sentinels,
       SUM(CASE WHEN notes = 'N/A' THEN 1 ELSE 0 END) AS notes_sentinels
FROM {{zone_name}}.backfill_demos.patient_records;


-- ============================================================================
-- LEARN: NULL Distribution Across Columns
-- ============================================================================
-- Which columns still have NULLs? age, blood_type, and emergency_contact
-- were fully backfilled. insurance_code, temperature, and notes still
-- carry NULLs because no default was assigned — a deliberate choice.
ASSERT ROW_COUNT = 1
ASSERT VALUE age_populated = 25
ASSERT VALUE blood_type_populated = 25
ASSERT VALUE emergency_contact_populated = 25
ASSERT VALUE insurance_code_populated = 17
ASSERT VALUE temperature_populated = 19
ASSERT VALUE notes_populated = 16
SELECT COUNT(age) AS age_populated,
       COUNT(blood_type) AS blood_type_populated,
       COUNT(emergency_contact) AS emergency_contact_populated,
       COUNT(insurance_code) AS insurance_code_populated,
       COUNT(temperature) AS temperature_populated,
       COUNT(notes) AS notes_populated
FROM {{zone_name}}.backfill_demos.patient_records;


-- ============================================================================
-- LEARN: COALESCE Backfill Results — Placeholder Counts
-- ============================================================================
-- COALESCE inserted default placeholders for missing data:
--   blood_type = 'PENDING'  → needs lab test
--   emergency_contact = 'UNKNOWN' → needs patient follow-up
--   age = 0 → needs verification
ASSERT ROW_COUNT = 1
ASSERT VALUE pending_blood_types = 9
ASSERT VALUE unknown_contacts = 8
ASSERT VALUE zero_ages = 6
SELECT SUM(CASE WHEN blood_type = 'PENDING' THEN 1 ELSE 0 END)
       AS pending_blood_types,
       SUM(CASE WHEN emergency_contact = 'UNKNOWN' THEN 1 ELSE 0 END)
       AS unknown_contacts,
       SUM(CASE WHEN age = 0 THEN 1 ELSE 0 END)
       AS zero_ages
FROM {{zone_name}}.backfill_demos.patient_records;


-- ============================================================================
-- EXPLORE: Rows That Had the Most Data Issues
-- ============================================================================
-- Count how many columns were fixed per patient. A "fixed" column is one
-- that now holds a placeholder (PENDING, UNKNOWN, age=0) or is still NULL
-- (insurance_code, temperature, notes) after sentinel removal.
ASSERT ROW_COUNT = 7
ASSERT VALUE issue_count = 6 WHERE id = 24
ASSERT VALUE issue_count = 5 WHERE id = 19
SELECT id, patient_name,
       (CASE WHEN age = 0 THEN 1 ELSE 0 END)
       + (CASE WHEN blood_type = 'PENDING' THEN 1 ELSE 0 END)
       + (CASE WHEN emergency_contact = 'UNKNOWN' THEN 1 ELSE 0 END)
       + (CASE WHEN insurance_code IS NULL THEN 1 ELSE 0 END)
       + (CASE WHEN temperature IS NULL THEN 1 ELSE 0 END)
       + (CASE WHEN notes IS NULL THEN 1 ELSE 0 END)
       AS issue_count
FROM {{zone_name}}.backfill_demos.patient_records
WHERE (CASE WHEN age = 0 THEN 1 ELSE 0 END)
      + (CASE WHEN blood_type = 'PENDING' THEN 1 ELSE 0 END)
      + (CASE WHEN emergency_contact = 'UNKNOWN' THEN 1 ELSE 0 END)
      + (CASE WHEN insurance_code IS NULL THEN 1 ELSE 0 END)
      + (CASE WHEN temperature IS NULL THEN 1 ELSE 0 END)
      + (CASE WHEN notes IS NULL THEN 1 ELSE 0 END) >= 3
ORDER BY issue_count DESC;


-- ============================================================================
-- VERIFY: All Checks — Comprehensive Validation
-- ============================================================================
-- Final verification that every cleaning rule was applied correctly.

-- Check 1: Total row count unchanged
ASSERT VALUE cnt = 25
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records;

-- Check 2: No age sentinels remain
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE age = -999;

-- Check 3: No temperature sentinels remain
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE temperature = -999.00;

-- Check 4: No 'N/A' emergency contacts remain
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE emergency_contact = 'N/A';

-- Check 5: No empty-string insurance codes remain
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE insurance_code = '';

-- Check 6: No 'N/A' notes remain
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE notes = 'N/A';

-- Check 7: All blood types populated (no NULLs)
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE blood_type IS NULL;

-- Check 8: All emergency contacts populated (no NULLs)
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE emergency_contact IS NULL;

-- Check 9: All ages populated (no NULLs)
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE age IS NULL;

-- Check 10: insurance_code NULLs = 8 (converted from empty strings)
ASSERT VALUE cnt = 8
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE insurance_code IS NULL;

-- Check 11: temperature NULLs = 6 (converted from -999.00 sentinels)
ASSERT VALUE cnt = 6
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE temperature IS NULL;

-- Check 12: notes NULLs = 9 (4 original NULLs + 5 converted from 'N/A')
ASSERT VALUE cnt = 9
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.backfill_demos.patient_records
WHERE notes IS NULL;
