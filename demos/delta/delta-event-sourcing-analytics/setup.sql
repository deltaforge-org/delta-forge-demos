-- ============================================================================
-- Delta Event Sourcing Analytics — ED Patient Flow — Setup Script
-- ============================================================================
-- Demonstrates analytical queries over an event-sourced patient flow log.
--
-- Tables created:
--   1. ed_events — 35 emergency department events for 10 patients
--
-- Operations performed:
--   1. CREATE DELTA TABLE with 7 columns
--   2. INSERT — 14 events (patients P001–P004)
--   3. INSERT — 10 events (patients P005–P007)
--   4. INSERT — 11 events (patients P008–P010)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: ed_events — 35 emergency department patient flow events
-- ============================================================================
-- Event types: triage, admit, transfer, discharge
-- Each patient follows: triage → admit → (optional transfer) → discharge
-- Payload varies by event type:
--   triage:    {"chief_complaint", "vitals_bp", "vitals_hr"}
--   admit:     {"bed", "attending", "nurse"}
--   transfer:  {"bed", "reason", "attending"}
--   discharge: {"disposition", "followup", "outcome"}
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.ed_events (
    id             INT,
    patient_id     VARCHAR,
    event_type     VARCHAR,
    department     VARCHAR,
    severity       VARCHAR,
    payload        VARCHAR,
    event_time     VARCHAR
) LOCATION 'delta-event-sourcing-analytics/ed_events';


-- STEP 2: Insert events for patients P001–P004 (14 events)
INSERT INTO {{zone_name}}.delta_demos.ed_events VALUES
    (1,  'P001', 'triage',    'ed_intake',  'critical', '{"chief_complaint":"chest_pain","vitals_bp":"180/110","vitals_hr":110}',      '2024-06-15 06:00:00'),
    (2,  'P001', 'admit',     'ed_beds',    'critical', '{"bed":"ED-01","attending":"Dr. Chen","nurse":"RN-Adams"}',                   '2024-06-15 06:12:00'),
    (3,  'P001', 'transfer',  'cardiology', 'critical', '{"bed":"CARD-05","reason":"acute_mi","attending":"Dr. Shah"}',                '2024-06-15 06:45:00'),
    (4,  'P001', 'discharge', 'cardiology', 'stable',   '{"disposition":"admitted_inpatient","followup":"48h","outcome":"improving"}',  '2024-06-15 14:00:00'),
    (5,  'P002', 'triage',    'ed_intake',  'moderate', '{"chief_complaint":"fracture_arm","vitals_bp":"130/85","vitals_hr":88}',      '2024-06-15 06:30:00'),
    (6,  'P002', 'admit',     'ed_beds',    'moderate', '{"bed":"ED-03","attending":"Dr. Patel","nurse":"RN-Brooks"}',                 '2024-06-15 06:52:00'),
    (7,  'P002', 'discharge', 'ed_beds',    'stable',   '{"disposition":"home","followup":"7d","outcome":"cast_applied"}',             '2024-06-15 09:30:00'),
    (8,  'P003', 'triage',    'ed_intake',  'urgent',   '{"chief_complaint":"allergic_reaction","vitals_bp":"100/60","vitals_hr":120}', '2024-06-15 07:00:00'),
    (9,  'P003', 'admit',     'ed_beds',    'urgent',   '{"bed":"ED-02","attending":"Dr. Chen","nurse":"RN-Clark"}',                    '2024-06-15 07:08:00'),
    (10, 'P003', 'transfer',  'icu',        'critical', '{"bed":"ICU-01","reason":"anaphylaxis","attending":"Dr. Kim"}',                '2024-06-15 07:25:00'),
    (11, 'P003', 'discharge', 'icu',        'stable',   '{"disposition":"admitted_inpatient","followup":"24h","outcome":"stabilized"}',  '2024-06-15 18:00:00'),
    (12, 'P004', 'triage',    'ed_intake',  'low',      '{"chief_complaint":"fever_cough","vitals_bp":"120/80","vitals_hr":78}',       '2024-06-15 07:15:00'),
    (13, 'P004', 'admit',     'ed_beds',    'low',      '{"bed":"ED-05","attending":"Dr. Patel","nurse":"RN-Adams"}',                  '2024-06-15 07:45:00'),
    (14, 'P004', 'discharge', 'ed_beds',    'low',      '{"disposition":"home","followup":"3d","outcome":"prescribed_meds"}',          '2024-06-15 08:30:00');


-- ============================================================================
-- STEP 3: Insert events for patients P005–P007 (10 events)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.ed_events
SELECT * FROM (VALUES
    (15, 'P005', 'triage',    'ed_intake',  'moderate', '{"chief_complaint":"laceration_hand","vitals_bp":"125/82","vitals_hr":72}',   '2024-06-15 08:00:00'),
    (16, 'P005', 'admit',     'ed_beds',    'moderate', '{"bed":"ED-04","attending":"Dr. Chen","nurse":"RN-Brooks"}',                  '2024-06-15 08:18:00'),
    (17, 'P005', 'discharge', 'ed_beds',    'stable',   '{"disposition":"home","followup":"5d","outcome":"sutured"}',                  '2024-06-15 10:00:00'),
    (18, 'P006', 'triage',    'ed_intake',  'urgent',   '{"chief_complaint":"abdominal_pain","vitals_bp":"140/90","vitals_hr":95}',    '2024-06-15 09:00:00'),
    (19, 'P006', 'admit',     'ed_beds',    'urgent',   '{"bed":"ED-01","attending":"Dr. Patel","nurse":"RN-Clark"}',                  '2024-06-15 09:15:00'),
    (20, 'P006', 'transfer',  'surgery',    'urgent',   '{"bed":"SURG-02","reason":"appendicitis","attending":"Dr. Lopez"}',           '2024-06-15 10:30:00'),
    (21, 'P006', 'discharge', 'surgery',    'stable',   '{"disposition":"admitted_inpatient","followup":"72h","outcome":"post_op"}',    '2024-06-15 16:00:00'),
    (22, 'P007', 'triage',    'ed_intake',  'low',      '{"chief_complaint":"severe_headache","vitals_bp":"135/88","vitals_hr":70}',   '2024-06-15 09:30:00'),
    (23, 'P007', 'admit',     'ed_beds',    'low',      '{"bed":"ED-06","attending":"Dr. Chen","nurse":"RN-Adams"}',                   '2024-06-15 10:00:00'),
    (24, 'P007', 'discharge', 'ed_beds',    'low',      '{"disposition":"home","followup":"7d","outcome":"prescribed_meds"}',          '2024-06-15 11:00:00')
) AS t(id, patient_id, event_type, department, severity, payload, event_time);


-- ============================================================================
-- STEP 4: Insert events for patients P008–P010 (11 events)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.ed_events
SELECT * FROM (VALUES
    (25, 'P008', 'triage',    'ed_intake',  'critical', '{"chief_complaint":"stroke_symptoms","vitals_bp":"200/120","vitals_hr":105}', '2024-06-15 10:00:00'),
    (26, 'P008', 'admit',     'ed_beds',    'critical', '{"bed":"ED-02","attending":"Dr. Kim","nurse":"RN-Clark"}',                    '2024-06-15 10:05:00'),
    (27, 'P008', 'transfer',  'neurology',  'critical', '{"bed":"NEURO-03","reason":"acute_stroke","attending":"Dr. Reeves"}',         '2024-06-15 10:30:00'),
    (28, 'P008', 'discharge', 'neurology',  'moderate', '{"disposition":"admitted_inpatient","followup":"24h","outcome":"tpa_given"}',  '2024-06-15 20:00:00'),
    (29, 'P009', 'triage',    'ed_intake',  'low',      '{"chief_complaint":"ankle_sprain","vitals_bp":"118/75","vitals_hr":68}',      '2024-06-15 11:00:00'),
    (30, 'P009', 'admit',     'ed_beds',    'low',      '{"bed":"ED-03","attending":"Dr. Patel","nurse":"RN-Brooks"}',                 '2024-06-15 11:20:00'),
    (31, 'P009', 'discharge', 'ed_beds',    'low',      '{"disposition":"home","followup":"10d","outcome":"brace_applied"}',           '2024-06-15 12:30:00'),
    (32, 'P010', 'triage',    'ed_intake',  'urgent',   '{"chief_complaint":"dyspnea","vitals_bp":"145/95","vitals_hr":100}',          '2024-06-15 12:00:00'),
    (33, 'P010', 'admit',     'ed_beds',    'urgent',   '{"bed":"ED-04","attending":"Dr. Chen","nurse":"RN-Adams"}',                   '2024-06-15 12:10:00'),
    (34, 'P010', 'transfer',  'pulmonary',  'urgent',   '{"bed":"PULM-01","reason":"pneumonia","attending":"Dr. Grant"}',              '2024-06-15 13:00:00'),
    (35, 'P010', 'discharge', 'pulmonary',  'moderate', '{"disposition":"admitted_inpatient","followup":"48h","outcome":"antibiotics"}', '2024-06-15 22:00:00')
) AS t(id, patient_id, event_type, department, severity, payload, event_time);
