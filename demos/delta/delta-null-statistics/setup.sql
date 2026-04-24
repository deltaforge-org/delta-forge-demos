-- ============================================================================
-- NULL Statistics — NULL-Aware Query Optimization — Setup Script
-- ============================================================================
-- Healthcare patient records with optional fields that start NULL and get
-- populated over time. Delta tracks per-file NULL counts, enabling the engine
-- to skip files when filtering IS NULL or IS NOT NULL.
--
-- Tables created:
--   1. patient_records — 45 patients in 3 batches with different NULL patterns
--
-- Operations performed:
--   1. CREATE DELTA TABLE
--   2. INSERT Batch 1 — 15 newly admitted patients (many NULLs)
--   3. INSERT Batch 2 — 15 partially completed records (some NULLs)
--   4. INSERT Batch 3 — 15 fully completed records (no NULLs)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: patient_records — healthcare records with optional fields
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.patient_records (
    id                    INT,
    patient_id            VARCHAR,
    name                  VARCHAR,
    admission_date        VARCHAR,
    diagnosis_code        VARCHAR,
    discharge_date        VARCHAR,
    secondary_insurance   VARCHAR,
    ward                  VARCHAR
) LOCATION 'patient_records';


-- ============================================================================
-- STEP 2: Batch 1 — Newly admitted patients (many NULLs)
-- diagnosis_code: 10 NULL, 5 set | discharge_date: 15 NULL | insurance: 12 NULL, 3 set
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.patient_records VALUES
    (1,  'PAT-100', 'Alice Chen',      '2025-01-02', NULL,    NULL,         NULL,              'cardiac'),
    (2,  'PAT-101', 'Bob Martinez',     '2025-01-03', NULL,    NULL,         NULL,              'cardiac'),
    (3,  'PAT-102', 'Carol White',      '2025-01-04', 'J06.9', NULL,         NULL,              'general'),
    (4,  'PAT-103', 'David Kim',        '2025-01-05', NULL,    NULL,         NULL,              'orthopedic'),
    (5,  'PAT-104', 'Eva Johnson',      '2025-01-06', 'I21.0', NULL,         'BlueCross-PPO',   'cardiac'),
    (6,  'PAT-105', 'Frank Liu',        '2025-01-07', NULL,    NULL,         NULL,              'general'),
    (7,  'PAT-106', 'Grace Park',       '2025-01-08', NULL,    NULL,         NULL,              'orthopedic'),
    (8,  'PAT-107', 'Henry Adams',      '2025-01-09', 'K35.8', NULL,         NULL,              'surgical'),
    (9,  'PAT-108', 'Iris Patel',       '2025-01-10', NULL,    NULL,         'Aetna-HMO',       'general'),
    (10, 'PAT-109', 'Jack Brown',       '2025-01-11', NULL,    NULL,         NULL,              'cardiac'),
    (11, 'PAT-110', 'Karen Lee',        '2025-01-12', 'M54.5', NULL,         NULL,              'orthopedic'),
    (12, 'PAT-111', 'Leo Garcia',       '2025-01-13', NULL,    NULL,         NULL,              'surgical'),
    (13, 'PAT-112', 'Mia Thompson',     '2025-01-14', NULL,    NULL,         'UnitedHealth-EPO','general'),
    (14, 'PAT-113', 'Noah Wilson',      '2025-01-15', 'S72.0', NULL,         NULL,              'orthopedic'),
    (15, 'PAT-114', 'Olivia Davis',     '2025-01-16', NULL,    NULL,         NULL,              'cardiac');


-- ============================================================================
-- STEP 3: Batch 2 — Partially completed (all diagnosed, some discharged)
-- diagnosis_code: 0 NULL | discharge_date: 7 NULL, 8 set | insurance: 12 NULL, 3 set
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.patient_records
SELECT * FROM (VALUES
    (16, 'PAT-115', 'Peter Zhang',      '2025-01-17', 'I25.1', '2025-01-22', NULL,              'cardiac'),
    (17, 'PAT-116', 'Quinn Roberts',    '2025-01-18', 'J18.9', '2025-01-24', NULL,              'general'),
    (18, 'PAT-117', 'Rachel Moore',     '2025-01-19', 'K80.2', NULL,         'Cigna-PPO',       'surgical'),
    (19, 'PAT-118', 'Sam Taylor',       '2025-01-20', 'M17.1', '2025-01-28', NULL,              'orthopedic'),
    (20, 'PAT-119', 'Tina Anderson',    '2025-01-21', 'I48.0', NULL,         NULL,              'cardiac'),
    (21, 'PAT-120', 'Uma Hernandez',    '2025-01-22', 'J44.1', '2025-01-30', 'BlueCross-HMO',   'general'),
    (22, 'PAT-121', 'Victor Clark',     '2025-01-23', 'S82.0', NULL,         NULL,              'orthopedic'),
    (23, 'PAT-122', 'Wendy Lewis',      '2025-01-24', 'K25.0', '2025-02-01', NULL,              'surgical'),
    (24, 'PAT-123', 'Xander Hall',      '2025-01-25', 'I21.4', NULL,         NULL,              'cardiac'),
    (25, 'PAT-124', 'Yara Allen',       '2025-01-26', 'M54.2', '2025-02-02', NULL,              'orthopedic'),
    (26, 'PAT-125', 'Zach Young',       '2025-01-27', 'J96.0', NULL,         'Aetna-PPO',       'general'),
    (27, 'PAT-126', 'Amy King',         '2025-01-28', 'K57.3', '2025-02-04', NULL,              'surgical'),
    (28, 'PAT-127', 'Brian Scott',      '2025-01-29', 'I50.0', NULL,         NULL,              'cardiac'),
    (29, 'PAT-128', 'Chloe Green',      '2025-01-30', 'S42.0', '2025-02-06', NULL,              'orthopedic'),
    (30, 'PAT-129', 'Dylan Baker',      '2025-01-31', 'M79.3', NULL,         NULL,              'general')
) AS t(id, patient_id, name, admission_date, diagnosis_code, discharge_date, secondary_insurance, ward);


-- ============================================================================
-- STEP 4: Batch 3 — Fully completed records (zero NULLs)
-- diagnosis_code: 0 NULL | discharge_date: 0 NULL | insurance: 0 NULL
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.patient_records
SELECT * FROM (VALUES
    (31, 'PAT-130', 'Elena Rivera',     '2025-02-01', 'I10',   '2025-02-08', 'Humana-PPO',      'cardiac'),
    (32, 'PAT-131', 'Finn Murphy',      '2025-02-02', 'J45.2', '2025-02-07', 'BlueCross-PPO',   'general'),
    (33, 'PAT-132', 'Gina Torres',      '2025-02-03', 'K29.7', '2025-02-10', 'Aetna-HMO',       'surgical'),
    (34, 'PAT-133', 'Hugo Reyes',       '2025-02-04', 'M16.1', '2025-02-12', 'Cigna-HMO',       'orthopedic'),
    (35, 'PAT-134', 'Isla Cooper',      '2025-02-05', 'I63.5', '2025-02-14', 'UnitedHealth-PPO','cardiac'),
    (36, 'PAT-135', 'Jake Reed',        '2025-02-06', 'J20.9', '2025-02-11', 'Humana-HMO',      'general'),
    (37, 'PAT-136', 'Kira Bell',        '2025-02-07', 'K40.9', '2025-02-15', 'BlueCross-HMO',   'surgical'),
    (38, 'PAT-137', 'Liam Price',       '2025-02-08', 'S32.0', '2025-02-16', 'Aetna-PPO',       'orthopedic'),
    (39, 'PAT-138', 'Maya Foster',      '2025-02-09', 'I42.0', '2025-02-18', 'Cigna-PPO',       'cardiac'),
    (40, 'PAT-139', 'Nate Ross',        '2025-02-10', 'J15.9', '2025-02-15', 'UnitedHealth-HMO','general'),
    (41, 'PAT-140', 'Olive Sanders',    '2025-02-11', 'K85.9', '2025-02-20', 'Humana-PPO',      'surgical'),
    (42, 'PAT-141', 'Paul Stewart',     '2025-02-12', 'M23.5', '2025-02-22', 'BlueCross-PPO',   'orthopedic'),
    (43, 'PAT-142', 'Ruby Morgan',      '2025-02-13', 'I35.0', '2025-02-24', 'Aetna-HMO',       'cardiac'),
    (44, 'PAT-143', 'Sean Butler',      '2025-02-14', 'J98.1', '2025-02-19', 'Cigna-HMO',       'general'),
    (45, 'PAT-144', 'Tess Howard',      '2025-02-15', 'S52.5', '2025-02-25', 'UnitedHealth-PPO','orthopedic')
) AS t(id, patient_id, name, admission_date, diagnosis_code, discharge_date, secondary_insurance, ward);
