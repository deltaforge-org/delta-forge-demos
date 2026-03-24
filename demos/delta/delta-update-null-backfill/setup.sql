-- ============================================================================
-- Delta UPDATE NULL Backfill — Patient Data Recovery — Setup Script
-- ============================================================================
-- Demonstrates NULL-handling UPDATE patterns: NULLIF to eliminate sentinel
-- values, COALESCE to backfill defaults, and CASE WHEN for conditional
-- NULLification. A hospital patient records system migrated from a legacy
-- database where NULLs arrived as -999, 'N/A', and empty strings.
--
-- Tables created:
--   1. patient_records — 25 rows (messy legacy data, then 8 cleaning passes)
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE
--   3. INSERT — 25 patient records with realistic sentinel/NULL mix
--   4. UPDATE — NULLIF age sentinel (-999 → NULL)
--   5. UPDATE — NULLIF temperature sentinel (-999.00 → NULL)
--   6. UPDATE — SET emergency_contact = NULL WHERE 'N/A'
--   7. UPDATE — SET insurance_code = NULL WHERE ''
--   8. UPDATE — SET notes = NULL WHERE 'N/A'
--   9. UPDATE — COALESCE blood_type NULL → 'PENDING'
--  10. UPDATE — COALESCE emergency_contact NULL → 'UNKNOWN'
--  11. UPDATE — CASE WHEN age IS NULL THEN 0
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.backfill_demos
    COMMENT 'NULL backfill and sentinel cleanup pattern demos';


-- ============================================================================
-- TABLE: patient_records — Hospital migration data (messy)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.backfill_demos.patient_records (
    id                INT,
    patient_name      VARCHAR,
    age               INT,
    blood_type        VARCHAR,
    emergency_contact VARCHAR,
    insurance_code    VARCHAR,
    last_visit        VARCHAR,
    temperature       DECIMAL(5,2),
    notes             VARCHAR
) LOCATION '{{data_path}}/patient_records';

GRANT ADMIN ON TABLE {{zone_name}}.backfill_demos.patient_records TO USER {{current_user}};


-- ============================================================================
-- VERSION 1: Raw migration data — 25 patient records
-- ============================================================================
-- Sentinel values from legacy system:
--   age = -999           → unknown age
--   temperature = -999.00 → no reading taken
--   emergency_contact = 'N/A' → not provided
--   insurance_code = ''  → empty string (missing)
--   notes = 'N/A'        → not applicable
-- Some fields are genuinely NULL (never populated in source).
--
-- Mix: ~8 fully clean, ~10 with 1-2 issues, ~7 with 3+ issues
INSERT INTO {{zone_name}}.backfill_demos.patient_records VALUES
    -- Fully clean rows (ids 1-8)
    (1,  'Alice Martin',       34,   'A+',   'Bob Martin',       'INS-1001', '2025-11-15', 98.60,    'Annual checkup'),
    (2,  'James Rodriguez',    45,   'O-',   'Maria Rodriguez',  'INS-1002', '2025-10-22', 98.40,    'Follow-up visit'),
    (3,  'Sarah Kim',          28,   'B+',   'David Kim',        'INS-1003', '2025-12-01', 97.90,    'Routine labs'),
    (4,  'Michael Chen',       62,   'AB+',  'Linda Chen',       'INS-1004', '2025-09-30', 99.10,    'Cardiology consult'),
    (5,  'Emily Davis',        51,   'A-',   'Tom Davis',        'INS-1005', '2025-11-08', 98.20,    'Physical exam'),
    (6,  'Robert Wilson',      73,   'O+',   'Nancy Wilson',     'INS-1006', '2025-08-14', 98.70,    'Diabetes management'),
    (7,  'Jennifer Lee',       39,   'B-',   'Kevin Lee',        'INS-1007', '2025-12-10', 98.50,    'Dermatology referral'),
    (8,  'Daniel Brown',       55,   'A+',   'Susan Brown',      'INS-1008', '2025-10-05', 98.30,    'Blood pressure check'),
    -- 1-2 issues each (ids 9-18)
    (9,  'Lisa Thompson',      -999, 'O+',   'Mark Thompson',    'INS-1009', '2025-11-20', 98.80,    'Prenatal visit'),
    (10, 'William Garcia',     41,   NULL,   'Rosa Garcia',      'INS-1010', '2025-09-18', 98.60,    'Allergy testing'),
    (11, 'Patricia Martinez',  67,   'AB-',  'N/A',              'INS-1011', '2025-10-30', 99.20,    'Joint pain evaluation'),
    (12, 'Christopher Taylor', 29,   'A+',   'Amy Taylor',       '',         '2025-12-05', 98.10,    'Sports physical'),
    (13, 'Jessica Anderson',   43,   'B+',   'Paul Anderson',    'INS-1013', '2025-11-01', -999.00,  'Migraine follow-up'),
    (14, 'Matthew Thomas',     58,   'O-',   'Karen Thomas',     'INS-1014', '2025-08-28', 98.90,    'N/A'),
    (15, 'Amanda Jackson',     36,   NULL,   'Steve Jackson',    'INS-1015', '2025-12-12', 98.40,    'Vaccination'),
    (16, 'Andrew White',       -999, 'A-',   'Diane White',      'INS-1016', '2025-10-15', 98.50,    'Wellness visit'),
    (17, 'Stephanie Harris',   70,   'O+',   'N/A',              'INS-1017', '2025-09-22', 98.60,    NULL),
    (18, 'Joshua Clark',       33,   'B-',   'Megan Clark',      '',         '2025-11-28', 97.80,    'Ear infection'),
    -- 3+ issues each (ids 19-25)
    (19, 'Nicole Lewis',       -999, NULL,   'N/A',              'INS-1019', '2025-10-10', -999.00,  'N/A'),
    (20, 'Ryan Robinson',      47,   NULL,   'N/A',              '',         '2025-12-15', 98.30,    NULL),
    (21, 'Lauren Walker',      -999, NULL,   'Jeff Walker',      '',         '2025-09-05', -999.00,  'N/A'),
    (22, 'Kevin Hall',         52,   NULL,   'N/A',              '',         '2025-11-11', -999.00,  'N/A'),
    (23, 'Michelle Young',     -999, NULL,   'N/A',              '',         '2025-08-20', 98.70,    NULL),
    (24, 'Brandon King',       -999, NULL,   'N/A',              '',         '2025-10-25', -999.00,  'N/A'),
    (25, 'Samantha Scott',     31,   NULL,   'N/A',              '',         '2025-12-18', -999.00,  NULL);


-- ============================================================================
-- VERSION 2: NULLIF — Convert age sentinel (-999) to NULL
-- ============================================================================
-- Legacy system used -999 for "unknown age". NULLIF(age, -999) returns NULL
-- when age equals -999, leaving valid ages unchanged.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.backfill_demos.patient_records
SET age = NULLIF(age, -999);


-- ============================================================================
-- VERSION 3: NULLIF — Convert temperature sentinel (-999.00) to NULL
-- ============================================================================
-- Same pattern: -999.00 meant "no reading taken" in the legacy system.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.backfill_demos.patient_records
SET temperature = NULLIF(temperature, -999.00);


-- ============================================================================
-- VERSION 4: Direct NULLification — emergency_contact 'N/A' → NULL
-- ============================================================================
-- The string 'N/A' was used when the patient declined to provide a contact.
-- We convert to proper NULL so IS NULL checks work correctly downstream.
ASSERT ROW_COUNT = 8
UPDATE {{zone_name}}.backfill_demos.patient_records
SET emergency_contact = NULL
WHERE emergency_contact = 'N/A';


-- ============================================================================
-- VERSION 5: Direct NULLification — insurance_code '' → NULL
-- ============================================================================
-- Empty strings are semantically NULL. Converting them allows consistent
-- IS NULL filtering and avoids false matches on string comparisons.
ASSERT ROW_COUNT = 8
UPDATE {{zone_name}}.backfill_demos.patient_records
SET insurance_code = NULL
WHERE insurance_code = '';


-- ============================================================================
-- VERSION 6: Direct NULLification — notes 'N/A' → NULL
-- ============================================================================
-- Same pattern as emergency_contact: 'N/A' sentinel → proper NULL.
ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.backfill_demos.patient_records
SET notes = NULL
WHERE notes = 'N/A';


-- ============================================================================
-- VERSION 7: COALESCE backfill — blood_type NULL → 'PENDING'
-- ============================================================================
-- Patients without a blood type on file get 'PENDING' as a placeholder
-- so lab orders can flag them for testing. COALESCE returns the first
-- non-NULL argument: existing values pass through, NULLs become 'PENDING'.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.backfill_demos.patient_records
SET blood_type = COALESCE(blood_type, 'PENDING');


-- ============================================================================
-- VERSION 8: COALESCE backfill — emergency_contact NULL → 'UNKNOWN'
-- ============================================================================
-- Regulatory compliance requires a non-NULL emergency contact field.
-- 'UNKNOWN' flags records for follow-up by the admissions team.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.backfill_demos.patient_records
SET emergency_contact = COALESCE(emergency_contact, 'UNKNOWN');


-- ============================================================================
-- VERSION 9: CASE WHEN — age IS NULL → 0
-- ============================================================================
-- Age = 0 is a recognized "needs verification" marker in the hospital system.
-- CASE WHEN provides conditional logic: only NULL ages are overwritten.
ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.backfill_demos.patient_records
SET age = CASE WHEN age IS NULL THEN 0 ELSE age END;
