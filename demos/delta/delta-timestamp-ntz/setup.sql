-- ============================================================================
-- SETUP: Hospital Shift Handover System — Timestamp NTZ Demo
-- ============================================================================
--
-- Scenario: Multi-hospital shift handover across 5 US hospitals in different
-- timezones (NYC UTC-4, Chicago UTC-5, Denver UTC-6, LA UTC-7, Honolulu UTC-10).
--
-- Table: shift_handover — 30 staff members with local + UTC shift times.
-- All timestamps are stored as VARCHAR to demonstrate the NTZ concept:
-- local times are preserved exactly as entered while UTC columns enable
-- cross-timezone chronological ordering.
--
-- Operations:
--   1. CREATE ZONE and SCHEMA
--   2. CREATE DELTA TABLE shift_handover
--   3. INSERT 30 rows in 3 batches of 10
--   4. UPDATE 3 rows to on_break status
--   5. UPDATE 2 rows to completed status
-- ============================================================================

-- ============================================================================
-- Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';

-- ============================================================================
-- Create Delta Table
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.shift_handover (
    id                  INT,
    staff_id            VARCHAR,
    staff_name          VARCHAR,
    hospital            VARCHAR,
    timezone_offset     VARCHAR,
    shift_start_local   VARCHAR,
    shift_end_local     VARCHAR,
    shift_start_utc     VARCHAR,
    role                VARCHAR,
    department          VARCHAR,
    status              VARCHAR
) LOCATION 'delta-timestamp-ntz/shift_handover';


-- ============================================================================
-- Batch 1: Metro General NYC (UTC-4) + Pacific Medical LA (UTC-7)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.shift_handover VALUES
    (1,  'S001', 'James Chen',        'Metro General',   'UTC-4',  '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 11:00:00', 'doctor',     'Emergency',  'active'),
    (2,  'S002', 'Maria Santos',      'Metro General',   'UTC-4',  '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 11:00:00', 'nurse',      'ICU',        'active'),
    (3,  'S003', 'David Kim',         'Metro General',   'UTC-4',  '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-14 19:00:00', 'technician', 'Surgery',    'active'),
    (4,  'S004', 'Sarah Mitchell',    'Metro General',   'UTC-4',  '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-14 19:00:00', 'doctor',     'Radiology',  'active'),
    (5,  'S005', 'Robert Patel',      'Metro General',   'UTC-4',  '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 03:00:00', 'nurse',      'Pediatrics', 'active'),
    (6,  'S006', 'Lisa Johansson',    'Metro General',   'UTC-4',  '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 03:00:00', 'technician', 'Oncology',   'active'),
    (7,  'S007', 'Michael Torres',    'Pacific Medical', 'UTC-7',  '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 14:00:00', 'doctor',     'Emergency',  'active'),
    (8,  'S008', 'Jennifer Wu',       'Pacific Medical', 'UTC-7',  '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 14:00:00', 'nurse',      'ICU',        'active'),
    (9,  'S009', 'Daniel Okafor',     'Pacific Medical', 'UTC-7',  '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-14 22:00:00', 'technician', 'Surgery',    'active'),
    (10, 'S010', 'Amanda Foster',     'Pacific Medical', 'UTC-7',  '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-14 22:00:00', 'doctor',     'Radiology',  'active');

-- ============================================================================
-- Batch 2: Pacific Medical (cont) + Central Health Chicago (UTC-5) +
--          Mountain Care Denver (UTC-6, partial)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.shift_handover VALUES
    (11, 'S011', 'Kevin Nakamura',    'Pacific Medical', 'UTC-7',  '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 06:00:00', 'nurse',      'Pediatrics', 'active'),
    (12, 'S012', 'Rachel Hernandez',  'Pacific Medical', 'UTC-7',  '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 06:00:00', 'technician', 'Oncology',   'active'),
    (13, 'S013', 'Thomas Bradley',    'Central Health',  'UTC-5',  '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 12:00:00', 'doctor',     'Emergency',  'active'),
    (14, 'S014', 'Emily Larson',      'Central Health',  'UTC-5',  '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 12:00:00', 'nurse',      'ICU',        'active'),
    (15, 'S015', 'Christopher Reeves','Central Health',  'UTC-5',  '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-14 20:00:00', 'technician', 'Surgery',    'active'),
    (16, 'S016', 'Natalie Gupta',     'Central Health',  'UTC-5',  '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-14 20:00:00', 'doctor',     'Radiology',  'active'),
    (17, 'S017', 'Andrew Fitzgerald', 'Central Health',  'UTC-5',  '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 04:00:00', 'nurse',      'Pediatrics', 'active'),
    (18, 'S018', 'Megan Kowalski',    'Central Health',  'UTC-5',  '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 04:00:00', 'technician', 'Oncology',   'active'),
    (19, 'S019', 'William Dubois',    'Mountain Care',   'UTC-6',  '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 13:00:00', 'doctor',     'Emergency',  'active'),
    (20, 'S020', 'Jessica Morales',   'Mountain Care',   'UTC-6',  '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 13:00:00', 'nurse',      'ICU',        'active');

-- ============================================================================
-- Batch 3: Mountain Care Denver (cont) + Island Hospital Honolulu (UTC-10)
-- ============================================================================

INSERT INTO {{zone_name}}.delta_demos.shift_handover VALUES
    (21, 'S021', 'Brian Sullivan',    'Mountain Care',   'UTC-6',  '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-14 21:00:00', 'technician', 'Surgery',    'active'),
    (22, 'S022', 'Stephanie Yamamoto','Mountain Care',   'UTC-6',  '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-14 21:00:00', 'doctor',     'Radiology',  'active'),
    (23, 'S023', 'Patrick O''Brien',  'Mountain Care',   'UTC-6',  '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 05:00:00', 'nurse',      'Pediatrics', 'active'),
    (24, 'S024', 'Olivia Andersson',  'Mountain Care',   'UTC-6',  '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 05:00:00', 'technician', 'Oncology',   'active'),
    (25, 'S025', 'Marcus Thompson',   'Island Hospital', 'UTC-10', '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 17:00:00', 'doctor',     'Emergency',  'active'),
    (26, 'S026', 'Diana Kapoor',      'Island Hospital', 'UTC-10', '2025-07-14 07:00:00', '2025-07-14 15:00:00', '2025-07-14 17:00:00', 'nurse',      'ICU',        'active'),
    (27, 'S027', 'Jason Rivera',      'Island Hospital', 'UTC-10', '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-15 01:00:00', 'technician', 'Surgery',    'active'),
    (28, 'S028', 'Nicole Chang',      'Island Hospital', 'UTC-10', '2025-07-14 15:00:00', '2025-07-14 23:00:00', '2025-07-15 01:00:00', 'doctor',     'Radiology',  'active'),
    (29, 'S029', 'Gregory Hawkins',   'Island Hospital', 'UTC-10', '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 09:00:00', 'nurse',      'Pediatrics', 'active'),
    (30, 'S030', 'Samantha Reyes',    'Island Hospital', 'UTC-10', '2025-07-14 23:00:00', '2025-07-15 07:00:00', '2025-07-15 09:00:00', 'technician', 'Oncology',   'active');

-- ============================================================================
-- Status Updates — Simulate real-time shift activity
-- ============================================================================

-- Three staff members go on break
UPDATE {{zone_name}}.delta_demos.shift_handover SET status = 'on_break' WHERE id = 2;

UPDATE {{zone_name}}.delta_demos.shift_handover SET status = 'on_break' WHERE id = 14;

UPDATE {{zone_name}}.delta_demos.shift_handover SET status = 'on_break' WHERE id = 26;

-- Two staff members complete their shifts
UPDATE {{zone_name}}.delta_demos.shift_handover SET status = 'completed' WHERE id = 5;

UPDATE {{zone_name}}.delta_demos.shift_handover SET status = 'completed' WHERE id = 19;
