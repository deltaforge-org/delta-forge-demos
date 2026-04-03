-- ============================================================================
-- Iceberg UniForm Column Reorder — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table and seeds 20 patient records across
-- 4 diagnosis groups (cardiology, orthopedics, neurology, oncology). Column
-- reordering happens in queries.sql to demonstrate how both Delta and Iceberg
-- metadata track position changes when columns are moved with FIRST/AFTER.
--
-- Dataset: 20 patients with columns:
-- record_id, last_name, first_name, dob, mrn, diagnosis_code,
-- admission_date, discharge_date, attending_physician.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm and column mapping
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.patient_records (
    record_id            INT,
    last_name            VARCHAR,
    first_name           VARCHAR,
    dob                  VARCHAR,
    mrn                  VARCHAR,
    diagnosis_code       VARCHAR,
    admission_date       VARCHAR,
    discharge_date       VARCHAR,
    attending_physician  VARCHAR
) LOCATION '{{data_path}}/patient_records'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.patient_records TO USER {{current_user}};

-- STEP 3: Seed 20 patient records (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.patient_records VALUES
    (1,  'Smith',    'John',      '1955-03-12', 'MRN-1001', 'I25.10',  '2025-01-05', '2025-01-12', 'Dr. Chen'),
    (2,  'Johnson',  'Mary',      '1962-07-24', 'MRN-1002', 'I48.0',   '2025-01-08', '2025-01-15', 'Dr. Chen'),
    (3,  'Williams', 'Robert',    '1948-11-30', 'MRN-1003', 'I50.9',   '2025-01-10', '2025-01-20', 'Dr. Patel'),
    (4,  'Brown',    'Patricia',  '1970-05-18', 'MRN-1004', 'I25.10',  '2025-01-14', '2025-01-18', 'Dr. Patel'),
    (5,  'Jones',    'Michael',   '1958-09-05', 'MRN-1005', 'I48.0',   '2025-01-16', '2025-01-22', 'Dr. Chen'),
    (6,  'Davis',    'Linda',     '1975-02-14', 'MRN-1006', 'M17.11',  '2025-01-06', '2025-01-13', 'Dr. Kim'),
    (7,  'Miller',   'James',     '1968-08-22', 'MRN-1007', 'S72.001', '2025-01-09', '2025-01-25', 'Dr. Kim'),
    (8,  'Wilson',   'Barbara',   '1980-04-10', 'MRN-1008', 'M54.5',   '2025-01-11', '2025-01-14', 'Dr. Lopez'),
    (9,  'Moore',    'David',     '1972-12-01', 'MRN-1009', 'M17.11',  '2025-01-15', '2025-01-22', 'Dr. Lopez'),
    (10, 'Taylor',   'Susan',     '1965-06-28', 'MRN-1010', 'S72.001', '2025-01-18', '2025-02-01', 'Dr. Kim'),
    (11, 'Anderson', 'Richard',   '1952-10-15', 'MRN-1011', 'G43.909', '2025-01-07', '2025-01-10', 'Dr. Nakamura'),
    (12, 'Thomas',   'Karen',     '1960-01-20', 'MRN-1012', 'G20',     '2025-01-12', '2025-01-19', 'Dr. Nakamura'),
    (13, 'Jackson',  'Charles',   '1945-06-08', 'MRN-1013', 'G30.9',   '2025-01-14', '2025-01-28', 'Dr. Singh'),
    (14, 'White',    'Nancy',     '1978-03-25', 'MRN-1014', 'G43.909', '2025-01-17', '2025-01-20', 'Dr. Singh'),
    (15, 'Harris',   'Daniel',    '1956-11-12', 'MRN-1015', 'G20',     '2025-01-19', '2025-01-30', 'Dr. Nakamura'),
    (16, 'Martin',   'Lisa',      '1967-04-03', 'MRN-1016', 'C34.90',  '2025-01-08', '2025-01-22', 'Dr. Okafor'),
    (17, 'Garcia',   'Thomas',    '1953-08-17', 'MRN-1017', 'C50.911', '2025-01-11', '2025-01-25', 'Dr. Okafor'),
    (18, 'Martinez', 'Jennifer',  '1971-12-29', 'MRN-1018', 'C18.9',   '2025-01-13', '2025-01-27', 'Dr. Reeves'),
    (19, 'Robinson', 'William',   '1949-02-06', 'MRN-1019', 'C34.90',  '2025-01-16', '2025-01-30', 'Dr. Reeves'),
    (20, 'Clark',    'Elizabeth',  '1974-07-21', 'MRN-1020', 'C50.911', '2025-01-20', '2025-02-03', 'Dr. Okafor');
