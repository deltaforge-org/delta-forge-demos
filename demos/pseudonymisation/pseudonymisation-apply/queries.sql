-- ============================================================================
-- Pseudonymisation Apply — Clinical Trial De-identification — Demo Queries
-- ============================================================================
-- Queries showcasing query-time pseudonymisation of clinical trial data.
-- A pharmaceutical company protects participant PII before sharing trial
-- results with external researchers using GDPR-compliant rules.
--
-- Three query-time rules are active:
--   ssn              → redact (***-**-****)
--   participant_name → keyed_hash (deterministic pseudonym)
--   email            → mask (first 3 chars visible)
--
-- NOTE: APPLY PSEUDONYMISATION (permanent on-disk transformation) is a future
-- feature. This demo demonstrates the rule-based query-time approach which
-- achieves the same de-identification goal without modifying stored data.
--
-- Table: trial_participants (6 rows)
--   TR-2024-A Phase III: 3 participants (SUBJ-001 to SUBJ-003)
--   TR-2024-B Phase II:  3 participants (SUBJ-004 to SUBJ-006)
--   Statuses: Active (4), Completed (1), Withdrawn (1)
--   Treatment arms: Drug A (2), Drug B (2), Placebo (2)
-- ============================================================================


-- ============================================================================
-- 1. SHOW RULES — Review Active Protection Rules
-- ============================================================================
-- Lists all pseudonymisation rules on the trial_participants table.
-- Each row shows the column, transform type, and parameters.
--
-- Expected: 3 rules (ssn → redact, participant_name → keyed_hash, email → mask)

ASSERT ROW_COUNT = 3
SHOW PSEUDONYMISATION RULES FOR {{zone_name}}.pseudonymisation_demos.trial_participants;


-- ============================================================================
-- 2. Query with Rules Active — Pseudonymised Participant Data
-- ============================================================================
-- With rules active, SELECT returns transformed PII at runtime:
--   - ssn:              ***-**-**** (fully redacted)
--   - participant_name: SHA256 keyed hash (deterministic pseudonym)
--   - email:            First 3 chars visible, rest masked
--   - All other columns: Unchanged (clinical data preserved)
--
-- Expected: 6 rows, PII columns transformed, clinical fields intact

ASSERT ROW_COUNT = 6
ASSERT VALUE status = 'Active' WHERE subject_id = 'SUBJ-001'
ASSERT VALUE ssn_redacted = '***-**-****' WHERE subject_id = 'SUBJ-001'
ASSERT VALUE treatment_arm = 'Drug A' WHERE subject_id = 'SUBJ-001'
SELECT
    subject_id,
    participant_name AS name_hash,
    date_of_birth,
    email            AS email_masked,
    ssn              AS ssn_redacted,
    status,
    trial_id,
    treatment_arm,
    efficacy_score,
    outcome
FROM {{zone_name}}.pseudonymisation_demos.trial_participants
ORDER BY subject_id;


-- ============================================================================
-- 3. Efficacy by Treatment Arm — Aggregations on Protected Data
-- ============================================================================
-- Aggregations work correctly on pseudonymised data. This query groups by
-- treatment arm to compare drug efficacy — the core analytical use case
-- for external researchers who do not need to see individual PII.
--
-- Expected results (3 arms, 2 participants each):
--   Drug A  | 2 | avg ~0.88
--   Drug B  | 2 | avg ~0.73
--   Placebo | 2 | avg ~0.37

ASSERT ROW_COUNT = 3
ASSERT VALUE participant_count = 2 WHERE treatment_arm = 'Drug A'
ASSERT VALUE participant_count = 2 WHERE treatment_arm = 'Drug B'
ASSERT VALUE participant_count = 2 WHERE treatment_arm = 'Placebo'
-- Non-deterministic: float aggregation
ASSERT WARNING VALUE avg_efficacy BETWEEN 0.87 AND 0.89 WHERE treatment_arm = 'Drug A'
ASSERT WARNING VALUE avg_efficacy BETWEEN 0.72 AND 0.74 WHERE treatment_arm = 'Drug B'
ASSERT WARNING VALUE avg_efficacy BETWEEN 0.35 AND 0.37 WHERE treatment_arm = 'Placebo'
SELECT
    treatment_arm,
    COUNT(*) AS participant_count,
    ROUND(AVG(efficacy_score), 2) AS avg_efficacy
FROM {{zone_name}}.pseudonymisation_demos.trial_participants
GROUP BY treatment_arm
ORDER BY avg_efficacy DESC;


-- ============================================================================
-- 4. Trial Enrollment by Status — Participant Distribution
-- ============================================================================
-- Groups participants by enrollment status. Useful for regulatory reporting
-- and monitoring trial attrition rates.
--
-- Expected results:
--   Active    | 4
--   Completed | 1
--   Withdrawn | 1

ASSERT ROW_COUNT = 3
ASSERT VALUE status_count = 4 WHERE status = 'Active'
ASSERT VALUE status_count = 1 WHERE status = 'Completed'
ASSERT VALUE status_count = 1 WHERE status = 'Withdrawn'
SELECT
    status,
    COUNT(*) AS status_count
FROM {{zone_name}}.pseudonymisation_demos.trial_participants
GROUP BY status
ORDER BY status_count DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, PII transformation verification,
-- and clinical data integrity across the trial_participants table.

ASSERT ROW_COUNT = 6
ASSERT VALUE treatment_arm = 'Drug A' WHERE subject_id = 'SUBJ-001'
ASSERT VALUE ssn_redacted = '***-**-****' WHERE subject_id = 'SUBJ-003'
ASSERT VALUE status = 'Withdrawn' WHERE subject_id = 'SUBJ-003'
SELECT
    subject_id,
    participant_name AS name_hash,
    ssn              AS ssn_redacted,
    status,
    treatment_arm,
    efficacy_score
FROM {{zone_name}}.pseudonymisation_demos.trial_participants
ORDER BY subject_id;
