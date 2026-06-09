-- ============================================================================
-- Delta Event Sourcing Analytics — ED Patient Flow — Educational Queries
-- ============================================================================
-- WHAT: An event-sourced patient flow log captures every state change in the
--       emergency department as an immutable event row.
-- WHY:  Healthcare operations teams need real-time visibility into patient wait
--       times, department bottlenecks, and bed utilization — derived entirely
--       from the event stream, not from mutable status fields.
-- HOW:  Window functions (LAG, ROW_NUMBER, SUM OVER) and CTEs reconstruct
--       patient timelines, compute inter-event durations, and derive current
--       state from history.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Event type distribution across the ED
-- ============================================================================
-- Every patient generates 3–4 events: triage → admit → (transfer) → discharge.
-- The ratio of transfers to admissions reveals how often patients need
-- specialist care beyond the ED. Here, 5 of 10 patients were transferred.

ASSERT ROW_COUNT = 4
ASSERT VALUE event_count = 10 WHERE event_type = 'triage'
ASSERT VALUE event_count = 10 WHERE event_type = 'admit'
ASSERT VALUE event_count = 5 WHERE event_type = 'transfer'
ASSERT VALUE event_count = 10 WHERE event_type = 'discharge'
SELECT event_type, COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.ed_events
GROUP BY event_type
ORDER BY event_type;


-- ============================================================================
-- EXPLORE: Triage severity breakdown
-- ============================================================================
-- Triage severity drives resource allocation. Critical patients (P001, P008)
-- need immediate attention; low-severity patients (P004, P007, P009) can wait.
-- This distribution helps staffing models predict demand.

ASSERT ROW_COUNT = 4
ASSERT VALUE patient_count = 2 WHERE severity = 'critical'
ASSERT VALUE patient_count = 3 WHERE severity = 'low'
ASSERT VALUE patient_count = 2 WHERE severity = 'moderate'
ASSERT VALUE patient_count = 3 WHERE severity = 'urgent'
SELECT severity, COUNT(*) AS patient_count
FROM {{zone_name}}.delta_demos.ed_events
WHERE event_type = 'triage'
GROUP BY severity
ORDER BY severity;


-- ============================================================================
-- LEARN: LAG window function — triage-to-admit wait time
-- ============================================================================
-- LAG looks back at the previous event for the same patient (partitioned by
-- patient_id, ordered by id). By comparing the admit event_time to the
-- preceding triage event_time, we compute how long each patient waited.
-- This is the most critical ED metric — long waits correlate with worse outcomes.

ASSERT ROW_COUNT = 10
SELECT patient_id,
       event_time AS admit_time,
       LAG(event_time) OVER (PARTITION BY patient_id ORDER BY id) AS triage_time,
       event_type
FROM {{zone_name}}.delta_demos.ed_events
WHERE event_type = 'admit'
ORDER BY patient_id;


-- ============================================================================
-- LEARN: ROW_NUMBER — derive current state from event history
-- ============================================================================
-- In event sourcing, the "current state" is the most recent event per entity.
-- ROW_NUMBER() with DESC ordering assigns rn=1 to each patient's latest event.
-- This replaces the need for a mutable "status" column — state is derived.

ASSERT ROW_COUNT = 10
SELECT patient_id, event_type, department, severity
FROM (
    SELECT patient_id, event_type, department, severity,
           ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY id DESC) AS rn
    FROM {{zone_name}}.delta_demos.ed_events
) sub
WHERE rn = 1
ORDER BY patient_id;


-- ============================================================================
-- LEARN: Department event volume — where patients spend time
-- ============================================================================
-- ed_intake handles all 10 triages, ed_beds handles all 10 admissions plus
-- discharges for non-transferred patients. Specialist departments (cardiology,
-- ICU, surgery, neurology, pulmonary) only see transferred patients.

ASSERT ROW_COUNT = 7
ASSERT VALUE event_count = 10 WHERE department = 'ed_intake'
ASSERT VALUE event_count = 15 WHERE department = 'ed_beds'
SELECT department, COUNT(*) AS event_count
FROM {{zone_name}}.delta_demos.ed_events
GROUP BY department
ORDER BY department;


-- ============================================================================
-- LEARN: CTE — discharge disposition analysis
-- ============================================================================
-- A CTE extracts the disposition from discharge event payloads using LIKE
-- pattern matching, then groups to count home discharges vs inpatient admits.
-- Operations teams use this to forecast downstream bed demand.

ASSERT ROW_COUNT = 2
ASSERT VALUE discharge_count = 5 WHERE disposition = 'admitted_inpatient'
ASSERT VALUE discharge_count = 5 WHERE disposition = 'home'
WITH dispositions AS (
    SELECT patient_id,
           CASE
               WHEN payload LIKE '%"disposition":"home"%' THEN 'home'
               WHEN payload LIKE '%"disposition":"admitted_inpatient"%' THEN 'admitted_inpatient'
           END AS disposition
    FROM {{zone_name}}.delta_demos.ed_events
    WHERE event_type = 'discharge'
)
SELECT disposition, COUNT(*) AS discharge_count
FROM dispositions
GROUP BY disposition
ORDER BY disposition;


-- ============================================================================
-- LEARN: Window SUM — running patient count by arrival order
-- ============================================================================
-- SUM(1) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) computes a running count
-- of triage events. This shows how the ED patient load builds up over the
-- morning shift — essential for predicting when the department hits capacity.

ASSERT ROW_COUNT = 10
ASSERT VALUE running_count = 1 WHERE patient_id = 'P001'
ASSERT VALUE running_count = 5 WHERE patient_id = 'P005'
ASSERT VALUE running_count = 10 WHERE patient_id = 'P010'
SELECT patient_id, event_time,
       SUM(1) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS running_count
FROM {{zone_name}}.delta_demos.ed_events
WHERE event_type = 'triage'
ORDER BY id;


-- ============================================================================
-- LEARN: Transfer pattern analysis — which specialties receive patients
-- ============================================================================
-- Transfers reveal the most common specialist pathways. Each transfer payload
-- contains the reason — a JSON field unique to transfer events. The CASE WHEN
-- extracts the reason for human-readable grouping.

ASSERT ROW_COUNT = 5
SELECT patient_id, department,
       CASE
           WHEN payload LIKE '%"reason":"acute_mi"%' THEN 'acute_mi'
           WHEN payload LIKE '%"reason":"anaphylaxis"%' THEN 'anaphylaxis'
           WHEN payload LIKE '%"reason":"appendicitis"%' THEN 'appendicitis'
           WHEN payload LIKE '%"reason":"acute_stroke"%' THEN 'acute_stroke'
           WHEN payload LIKE '%"reason":"pneumonia"%' THEN 'pneumonia'
       END AS transfer_reason
FROM {{zone_name}}.delta_demos.ed_events
WHERE event_type = 'transfer'
ORDER BY id;


-- ============================================================================
-- LEARN: CTE + window — events per patient with timeline position
-- ============================================================================
-- Combines ROW_NUMBER (event sequence within patient) with COUNT OVER (total
-- events per patient) to show each patient's journey length. Patients with
-- 4 events had a transfer; patients with 3 did not.

ASSERT ROW_COUNT = 35
SELECT patient_id, event_type, department, event_time,
       ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY id) AS step_num,
       COUNT(*) OVER (PARTITION BY patient_id) AS total_steps
FROM {{zone_name}}.delta_demos.ed_events
ORDER BY patient_id, id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total event count is 35
ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.delta_demos.ed_events;

-- Verify 10 distinct patients
ASSERT VALUE patient_count = 10
SELECT COUNT(DISTINCT patient_id) AS patient_count FROM {{zone_name}}.delta_demos.ed_events;

-- Verify 10 triage events
ASSERT VALUE triage_count = 10
SELECT COUNT(*) AS triage_count FROM {{zone_name}}.delta_demos.ed_events WHERE event_type = 'triage';

-- Verify 5 transfer events
ASSERT VALUE transfer_count = 5
SELECT COUNT(*) AS transfer_count FROM {{zone_name}}.delta_demos.ed_events WHERE event_type = 'transfer';

-- Verify 7 distinct departments
ASSERT VALUE dept_count = 7
SELECT COUNT(DISTINCT department) AS dept_count FROM {{zone_name}}.delta_demos.ed_events;

-- Verify all events have non-null payloads
ASSERT VALUE payload_count = 35
SELECT COUNT(*) AS payload_count FROM {{zone_name}}.delta_demos.ed_events WHERE payload IS NOT NULL;
