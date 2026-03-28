-- ============================================================================
-- Iceberg V3 UniForm — Pharmaceutical Drug Registry Schema Evolution — Setup
-- ============================================================================
-- Creates a Delta table with Iceberg UniForm V3 and column mapping enabled.
-- Seeds 30 pharmaceutical compounds across 4 therapeutic categories and
-- 4 manufacturers. Schema evolution (ADD COLUMN) happens in queries.sql.
--
-- Dataset: 30 drugs with columns: drug_id, drug_name, category, manufacturer,
-- dosage_mg, approval_status, submission_date.
-- Categories: Oncology (8), Cardiology (8), Neurology (7), Immunology (7)
-- Statuses: approved (17), pending (11), rejected (2)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm V3 and column mapping
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.drug_registry (
    drug_id           INT,
    drug_name         VARCHAR,
    category          VARCHAR,
    manufacturer      VARCHAR,
    dosage_mg         INT,
    approval_status   VARCHAR,
    submission_date   VARCHAR
) LOCATION '{{data_path}}/drug_registry'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '3',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.drug_registry TO USER {{current_user}};

-- STEP 3: Seed 30 pharmaceutical compounds
INSERT INTO {{zone_name}}.iceberg_demos.drug_registry VALUES
    (1,  'Oncarex',        'Oncology',    'PharmaCorp',  250,  'approved',  '2024-01-10'),
    (2,  'Cardivex',       'Cardiology',  'PharmaCorp',  100,  'approved',  '2024-01-15'),
    (3,  'Neurolif',       'Neurology',   'BioGenix',    50,   'pending',   '2024-02-01'),
    (4,  'Immunoxa',       'Immunology',  'BioGenix',    200,  'approved',  '2024-02-10'),
    (5,  'Oncabin',        'Oncology',    'MedStar',     500,  'pending',   '2024-02-15'),
    (6,  'Cardiflow',      'Cardiology',  'MedStar',     75,   'approved',  '2024-03-01'),
    (7,  'Neurozen',       'Neurology',   'PharmaCorp',  25,   'rejected',  '2024-03-05'),
    (8,  'Immusync',       'Immunology',  'TheraChem',   150,  'approved',  '2024-03-10'),
    (9,  'Oncolyze',       'Oncology',    'TheraChem',   300,  'approved',  '2024-03-15'),
    (10, 'Cardipro',       'Cardiology',  'BioGenix',    50,   'pending',   '2024-03-20'),
    (11, 'Neurafix',       'Neurology',   'MedStar',     100,  'approved',  '2024-04-01'),
    (12, 'Immunodel',      'Immunology',  'PharmaCorp',  250,  'pending',   '2024-04-05'),
    (13, 'Oncarex-XR',     'Oncology',    'PharmaCorp',  750,  'pending',   '2024-04-10'),
    (14, 'Cardivex-SR',    'Cardiology',  'PharmaCorp',  200,  'approved',  '2024-04-15'),
    (15, 'Neurolif-ER',    'Neurology',   'BioGenix',    75,   'approved',  '2024-04-20'),
    (16, 'Immunoxa-Plus',  'Immunology',  'BioGenix',    400,  'pending',   '2024-04-25'),
    (17, 'Tumorix',        'Oncology',    'MedStar',     125,  'approved',  '2024-05-01'),
    (18, 'Heartguard',     'Cardiology',  'TheraChem',   150,  'approved',  '2024-05-05'),
    (19, 'Cognisafe',      'Neurology',   'TheraChem',   30,   'rejected',  '2024-05-10'),
    (20, 'Allergix',       'Immunology',  'MedStar',     100,  'approved',  '2024-05-15'),
    (21, 'Cytoblast',      'Oncology',    'BioGenix',    600,  'pending',   '2024-05-20'),
    (22, 'Vasoplex',       'Cardiology',  'MedStar',     80,   'approved',  '2024-05-25'),
    (23, 'Memantine-DF',   'Neurology',   'PharmaCorp',  40,   'approved',  '2024-06-01'),
    (24, 'Rheumaclear',    'Immunology',  'TheraChem',   175,  'pending',   '2024-06-05'),
    (25, 'Radiomab',       'Oncology',    'TheraChem',   350,  'approved',  '2024-06-10'),
    (26, 'Pacefiber',      'Cardiology',  'BioGenix',    60,   'pending',   '2024-06-15'),
    (27, 'Synaptol',       'Neurology',   'MedStar',     90,   'pending',   '2024-06-20'),
    (28, 'Autoimmix',      'Immunology',  'PharmaCorp',  300,  'approved',  '2024-06-25'),
    (29, 'Celltarget',     'Oncology',    'PharmaCorp',  450,  'pending',   '2024-06-28'),
    (30, 'Arteriex',       'Cardiology',  'TheraChem',   120,  'approved',  '2024-06-30');
