-- ============================================================================
-- Delta DV Accumulation — Setup Script
-- ============================================================================
-- Creates the patient_visits table and inserts 60 baseline rows across
-- 3 departments. The queries.sql file demonstrates how accumulated deletion
-- vectors from multiple GDPR/HIPAA compliance rounds degrade performance,
-- and how OPTIMIZE recovers it by materializing all DVs.
--
-- Tables created:
--   1. patient_visits — 60 initial rows (20 per department)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: patient_visits — Healthcare patient visit records
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.patient_visits (
    id            INT,
    patient_id    VARCHAR,
    department    VARCHAR,
    diagnosis     VARCHAR,
    visit_date    VARCHAR,
    cost          DECIMAL(10,2),
    status        VARCHAR
) LOCATION 'delta-dv-accumulation/patient_visits';


-- Cardiology department (20 visits)
INSERT INTO {{zone_name}}.delta_demos.patient_visits VALUES
    (1,  'PAT-001', 'cardiology', 'atrial fibrillation',       '2024-03-12', 1850.00, 'active'),
    (2,  'PAT-002', 'cardiology', 'hypertension management',   '2024-04-08', 420.00,  'active'),
    (3,  'PAT-003', 'cardiology', 'chest pain evaluation',     '2024-01-15', 175.00,  'discharged'),
    (4,  'PAT-004', 'cardiology', 'heart failure follow-up',   '2024-05-22', 1200.00, 'active'),
    (5,  'PAT-005', 'cardiology', 'echocardiogram review',     '2024-02-10', 680.00,  'cancelled'),
    (6,  'PAT-006', 'cardiology', 'arrhythmia monitoring',     '2024-06-01', 2100.00, 'active'),
    (7,  'PAT-007', 'cardiology', 'valve replacement consult', '2024-03-28', 2500.00, 'discharged'),
    (8,  'PAT-001', 'cardiology', 'post-ablation follow-up',   '2024-07-14', 950.00,  'active'),
    (9,  'PAT-008', 'cardiology', 'coronary angiography',      '2024-04-19', 1650.00, 'discharged'),
    (10, 'PAT-009', 'cardiology', 'lipid panel review',        '2024-01-30', 310.00,  'cancelled'),
    (11, 'PAT-010', 'cardiology', 'stress test',               '2024-05-05', 890.00,  'active'),
    (12, 'PAT-011', 'cardiology', 'pacemaker check',           '2024-06-18', 1100.00, 'discharged'),
    (13, 'PAT-012', 'cardiology', 'myocardial infarction',     '2024-02-22', 2350.00, 'discharged'),
    (14, 'PAT-013', 'cardiology', 'cardiac rehabilitation',    '2024-07-01', 750.00,  'active'),
    (15, 'PAT-003', 'cardiology', 'chest pain recurrence',     '2024-08-10', 520.00,  'active'),
    (16, 'PAT-014', 'cardiology', 'peripheral artery disease', '2024-03-05', 1400.00, 'discharged'),
    (17, 'PAT-015', 'cardiology', 'blood pressure monitoring', '2024-04-25', 280.00,  'active'),
    (18, 'PAT-016', 'cardiology', 'cardiac MRI',               '2024-05-30', 1950.00, 'discharged'),
    (19, 'PAT-017', 'cardiology', 'angina assessment',         '2024-06-12', 600.00,  'cancelled'),
    (20, 'PAT-018', 'cardiology', 'heart murmur evaluation',   '2024-07-20', 475.00,  'active');

-- Orthopedics department (20 visits)
INSERT INTO {{zone_name}}.delta_demos.patient_visits VALUES
    (21, 'PAT-019', 'orthopedics', 'ACL reconstruction consult', '2024-03-01', 1750.00, 'active'),
    (22, 'PAT-020', 'orthopedics', 'rotator cuff repair',       '2024-04-15', 2200.00, 'discharged'),
    (23, 'PAT-021', 'orthopedics', 'knee pain assessment',      '2024-01-20', 185.00,  'discharged'),
    (24, 'PAT-022', 'orthopedics', 'hip replacement pre-op',    '2024-05-10', 1300.00, 'active'),
    (25, 'PAT-023', 'orthopedics', 'fracture follow-up',        '2024-02-05', 350.00,  'cancelled'),
    (26, 'PAT-024', 'orthopedics', 'spinal fusion consult',     '2024-06-22', 2400.00, 'active'),
    (27, 'PAT-019', 'orthopedics', 'ACL post-surgery review',   '2024-07-08', 900.00,  'active'),
    (28, 'PAT-025', 'orthopedics', 'carpal tunnel release',     '2024-03-18', 1050.00, 'discharged'),
    (29, 'PAT-026', 'orthopedics', 'meniscus tear treatment',   '2024-04-30', 1600.00, 'discharged'),
    (30, 'PAT-027', 'orthopedics', 'ankle sprain evaluation',   '2024-01-10', 220.00,  'cancelled'),
    (31, 'PAT-028', 'orthopedics', 'osteoarthritis management', '2024-05-25', 780.00,  'active'),
    (32, 'PAT-029', 'orthopedics', 'sports injury rehab',       '2024-06-05', 650.00,  'active'),
    (33, 'PAT-020', 'orthopedics', 'rotator cuff follow-up',    '2024-07-22', 480.00,  'active'),
    (34, 'PAT-030', 'orthopedics', 'bone density scan',         '2024-02-28', 550.00,  'discharged'),
    (35, 'PAT-021', 'orthopedics', 'knee replacement consult',  '2024-08-01', 1450.00, 'active'),
    (36, 'PAT-022', 'orthopedics', 'hip replacement post-op',   '2024-08-15', 1100.00, 'discharged'),
    (37, 'PAT-025', 'orthopedics', 'wrist therapy follow-up',   '2024-06-30', 320.00,  'discharged'),
    (38, 'PAT-026', 'orthopedics', 'knee arthroscopy',          '2024-05-15', 1850.00, 'discharged'),
    (39, 'PAT-028', 'orthopedics', 'joint injection therapy',   '2024-07-10', 430.00,  'active'),
    (40, 'PAT-030', 'orthopedics', 'osteoporosis screening',    '2024-03-22', 290.00,  'cancelled');

-- Neurology department (20 visits)
INSERT INTO {{zone_name}}.delta_demos.patient_visits VALUES
    (41, 'PAT-001', 'neurology', 'migraine management',       '2024-04-02', 560.00,  'active'),
    (42, 'PAT-002', 'neurology', 'EEG monitoring',            '2024-05-18', 1350.00, 'active'),
    (43, 'PAT-003', 'neurology', 'nerve conduction study',    '2024-01-25', 190.00,  'discharged'),
    (44, 'PAT-004', 'neurology', 'epilepsy follow-up',        '2024-06-08', 820.00,  'active'),
    (45, 'PAT-005', 'neurology', 'memory assessment',         '2024-02-14', 450.00,  'cancelled'),
    (46, 'PAT-006', 'neurology', 'multiple sclerosis review', '2024-07-05', 2150.00, 'active'),
    (47, 'PAT-007', 'neurology', 'neuropathy evaluation',     '2024-03-30', 730.00,  'discharged'),
    (48, 'PAT-008', 'neurology', 'stroke rehabilitation',     '2024-04-22', 1900.00, 'discharged'),
    (49, 'PAT-009', 'neurology', 'tremor assessment',         '2024-05-28', 680.00,  'active'),
    (50, 'PAT-010', 'neurology', 'sleep study review',        '2024-01-08', 390.00,  'cancelled'),
    (51, 'PAT-011', 'neurology', 'Parkinson disease consult', '2024-06-20', 1550.00, 'discharged'),
    (52, 'PAT-012', 'neurology', 'brain MRI follow-up',       '2024-07-15', 2050.00, 'active'),
    (53, 'PAT-013', 'neurology', 'vertigo treatment',         '2024-02-08', 340.00,  'discharged'),
    (54, 'PAT-014', 'neurology', 'concussion protocol',       '2024-08-05', 620.00,  'active'),
    (55, 'PAT-015', 'neurology', 'headache clinic',           '2024-03-15', 280.00,  'active'),
    (56, 'PAT-016', 'neurology', 'dementia screening',        '2024-04-10', 950.00,  'discharged'),
    (57, 'PAT-017', 'neurology', 'seizure evaluation',        '2024-05-02', 1150.00, 'active'),
    (58, 'PAT-018', 'neurology', 'lumbar puncture consult',   '2024-01-22', 160.00,  'discharged'),
    (59, 'PAT-029', 'neurology', 'carpal tunnel EMG',         '2024-06-28', 520.00,  'cancelled'),
    (60, 'PAT-030', 'neurology', 'Bell palsy treatment',      '2024-07-30', 870.00,  'active');
