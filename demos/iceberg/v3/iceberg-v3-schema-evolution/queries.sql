-- ============================================================================
-- Iceberg V3 UniForm — Pharmaceutical Drug Registry Schema Evolution — Queries
-- ============================================================================
-- Demonstrates schema evolution on a V3 UniForm Delta table. Each ALTER TABLE
-- ADD COLUMN updates the Delta schema and generates a new Iceberg V3 schema
-- entry in metadata.json. The progression:
--   V1: 30 drugs, 7 columns (seed)
--   V2: ADD COLUMN trial_phase → NULL backfill → UPDATE to populate
--   V3: ADD COLUMN priority_score → UPDATE to populate
--   V4: INSERT 5 new drugs with full 9-column schema
-- The Iceberg read-back proves the V3 shadow metadata tracks all schema
-- versions and that evolved columns are readable through the Iceberg chain.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — 30 Drugs, 7 Columns
-- ============================================================================

ASSERT ROW_COUNT = 30
ASSERT VALUE drug_name = 'Oncarex' WHERE drug_id = 1
ASSERT VALUE category = 'Oncology' WHERE drug_id = 1
ASSERT VALUE dosage_mg = 250 WHERE drug_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.drug_registry
ORDER BY drug_id;


-- ============================================================================
-- Query 2: Per-Category Drug Counts
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE drug_count = 8 WHERE category = 'Cardiology'
ASSERT VALUE drug_count = 7 WHERE category = 'Immunology'
ASSERT VALUE drug_count = 7 WHERE category = 'Neurology'
ASSERT VALUE drug_count = 8 WHERE category = 'Oncology'
SELECT
    category,
    COUNT(*) AS drug_count
FROM {{zone_name}}.iceberg_demos.drug_registry
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 3: Per-Status Counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE drug_count = 17 WHERE approval_status = 'approved'
ASSERT VALUE drug_count = 11 WHERE approval_status = 'pending'
ASSERT VALUE drug_count = 2 WHERE approval_status = 'rejected'
SELECT
    approval_status,
    COUNT(*) AS drug_count
FROM {{zone_name}}.iceberg_demos.drug_registry
GROUP BY approval_status
ORDER BY approval_status;


-- ============================================================================
-- Query 4: Average Dosage by Category
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE avg_dosage = 104.38 WHERE category = 'Cardiology'
ASSERT VALUE avg_dosage = 225.0 WHERE category = 'Immunology'
ASSERT VALUE avg_dosage = 58.57 WHERE category = 'Neurology'
ASSERT VALUE avg_dosage = 415.63 WHERE category = 'Oncology'
SELECT
    category,
    ROUND(AVG(dosage_mg), 2) AS avg_dosage
FROM {{zone_name}}.iceberg_demos.drug_registry
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Schema Evolution Step 1: ADD COLUMN trial_phase
-- ============================================================================
-- Metadata-only operation. Iceberg V3 metadata.json gets a new schema entry
-- with an incremented schema-id. Existing data files return NULL for the
-- new column.

ALTER TABLE {{zone_name}}.iceberg_demos.drug_registry ADD COLUMN trial_phase VARCHAR;


-- ============================================================================
-- Query 5: Verify NULL Backfill on New Column
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total = 30
ASSERT VALUE has_phase = 0
ASSERT VALUE missing_phase = 30
SELECT
    COUNT(*) AS total,
    COUNT(trial_phase) AS has_phase,
    COUNT(*) - COUNT(trial_phase) AS missing_phase
FROM {{zone_name}}.iceberg_demos.drug_registry;


-- ============================================================================
-- Backfill trial_phase Based on Approval Status
-- ============================================================================

ASSERT ROW_COUNT = 30
UPDATE {{zone_name}}.iceberg_demos.drug_registry
SET trial_phase = CASE
    WHEN approval_status = 'approved' THEN 'Phase IV'
    WHEN approval_status = 'pending' THEN 'Phase III'
    WHEN approval_status = 'rejected' THEN 'Discontinued'
END;


-- ============================================================================
-- Query 6: Trial Phase Distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE drug_count = 2 WHERE trial_phase = 'Discontinued'
ASSERT VALUE drug_count = 11 WHERE trial_phase = 'Phase III'
ASSERT VALUE drug_count = 17 WHERE trial_phase = 'Phase IV'
SELECT
    trial_phase,
    COUNT(*) AS drug_count
FROM {{zone_name}}.iceberg_demos.drug_registry
GROUP BY trial_phase
ORDER BY trial_phase;


-- ============================================================================
-- Schema Evolution Step 2: ADD COLUMN priority_score
-- ============================================================================

ALTER TABLE {{zone_name}}.iceberg_demos.drug_registry ADD COLUMN priority_score DOUBLE;


-- ============================================================================
-- Backfill priority_score Based on Category
-- ============================================================================

ASSERT ROW_COUNT = 30
UPDATE {{zone_name}}.iceberg_demos.drug_registry
SET priority_score = CASE
    WHEN category = 'Oncology' THEN 9.5
    WHEN category = 'Cardiology' THEN 8.0
    WHEN category = 'Neurology' THEN 7.5
    WHEN category = 'Immunology' THEN 7.0
END;


-- ============================================================================
-- Query 7: Average Priority by Category — Verify Backfill
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE avg_priority = 8.0 WHERE category = 'Cardiology'
ASSERT VALUE avg_priority = 7.0 WHERE category = 'Immunology'
ASSERT VALUE avg_priority = 7.5 WHERE category = 'Neurology'
ASSERT VALUE avg_priority = 9.5 WHERE category = 'Oncology'
SELECT
    category,
    ROUND(AVG(priority_score), 2) AS avg_priority
FROM {{zone_name}}.iceberg_demos.drug_registry
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 8: Insert 5 New Drugs with Full 9-Column Schema
-- ============================================================================
-- These rows are inserted after both schema evolutions, so all 9 columns
-- are populated from the start.

INSERT INTO {{zone_name}}.iceberg_demos.drug_registry
SELECT * FROM (VALUES
    (31, 'Immutarget',  'Oncology',    'BioGenix',   1200, 'pending', '2024-07-01', 'Phase II',  9.5),
    (32, 'Cardionova',  'Cardiology',  'PharmaCorp',   95, 'pending', '2024-07-05', 'Phase II',  8.0),
    (33, 'Neuropath',   'Neurology',   'TheraChem',    55, 'pending', '2024-07-10', 'Phase I',   7.5),
    (34, 'Allerguard',  'Immunology',  'MedStar',     225, 'pending', '2024-07-15', 'Phase I',   7.0),
    (35, 'Hemablast',   'Oncology',    'MedStar',     800, 'pending', '2024-07-20', 'Phase III', 9.5)
) AS t(drug_id, drug_name, category, manufacturer, dosage_mg, approval_status, submission_date, trial_phase, priority_score);


-- ============================================================================
-- Query 9: Final Drug Counts by Category — 35 Total
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE drug_count = 9 WHERE category = 'Cardiology'
ASSERT VALUE drug_count = 8 WHERE category = 'Immunology'
ASSERT VALUE drug_count = 8 WHERE category = 'Neurology'
ASSERT VALUE drug_count = 10 WHERE category = 'Oncology'
SELECT
    category,
    COUNT(*) AS drug_count
FROM {{zone_name}}.iceberg_demos.drug_registry
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 10: Final Trial Phase Distribution — 5 Phases
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE drug_count = 2 WHERE trial_phase = 'Discontinued'
ASSERT VALUE drug_count = 2 WHERE trial_phase = 'Phase I'
ASSERT VALUE drug_count = 2 WHERE trial_phase = 'Phase II'
ASSERT VALUE drug_count = 12 WHERE trial_phase = 'Phase III'
ASSERT VALUE drug_count = 17 WHERE trial_phase = 'Phase IV'
SELECT
    trial_phase,
    COUNT(*) AS drug_count
FROM {{zone_name}}.iceberg_demos.drug_registry
GROUP BY trial_phase
ORDER BY trial_phase;


-- ============================================================================
-- Query 11: Time Travel — Read Version 1 (Pre-Evolution Schema)
-- ============================================================================
-- Version 1 had only 7 columns. trial_phase and priority_score did not exist.

ASSERT ROW_COUNT = 30
SELECT
    drug_id, drug_name, category, manufacturer, dosage_mg, approval_status, submission_date
FROM {{zone_name}}.iceberg_demos.drug_registry VERSION AS OF 1
ORDER BY drug_id;


-- ============================================================================
-- Query 12: Version History — Schema Evolution Trail
-- ============================================================================
-- Non-deterministic: DESCRIBE HISTORY returns commit timestamps and version
-- counts that can vary by one or two based on engine internals. Use a range
-- assertion with WARNING severity.

ASSERT WARNING ROW_COUNT >= 6
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.drug_registry;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_drugs = 35
ASSERT VALUE category_count = 4
ASSERT VALUE phase_count = 5
ASSERT VALUE all_have_phase = 35
ASSERT VALUE all_have_priority = 35
ASSERT VALUE avg_dosage = 243.43
ASSERT VALUE avg_priority = 8.09
SELECT
    COUNT(*) AS total_drugs,
    COUNT(DISTINCT category) AS category_count,
    COUNT(DISTINCT trial_phase) AS phase_count,
    COUNT(trial_phase) AS all_have_phase,
    COUNT(priority_score) AS all_have_priority,
    ROUND(AVG(dosage_mg), 2) AS avg_dosage,
    ROUND(AVG(priority_score), 2) AS avg_priority
FROM {{zone_name}}.iceberg_demos.drug_registry;


-- ============================================================================
-- ICEBERG V3 READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and query
-- through the V3 metadata chain. This proves the UniForm V3 shadow metadata
-- correctly tracks all schema evolution steps.

DROP TABLE IF EXISTS {{zone_name}}.iceberg_demos.drug_registry_iceberg;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.drug_registry_iceberg
USING ICEBERG
LOCATION '{{data_path}}/drug_registry';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.drug_registry_iceberg TO USER {{current_user}};


-- ============================================================================
-- Iceberg Verify 1: Row Count — 35 Drugs
-- ============================================================================

ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.iceberg_demos.drug_registry_iceberg ORDER BY drug_id;


-- ============================================================================
-- Iceberg Verify 2: Spot-Check Seed Row — Original Drug with Evolved Columns
-- ============================================================================
-- Oncarex (drug_id=1) was seeded with 7 columns, then gained trial_phase
-- and priority_score via UPDATE. The Iceberg reader must see all 9 columns.

ASSERT ROW_COUNT = 1
ASSERT VALUE drug_name = 'Oncarex' WHERE drug_id = 1
ASSERT VALUE category = 'Oncology' WHERE drug_id = 1
ASSERT VALUE manufacturer = 'PharmaCorp' WHERE drug_id = 1
ASSERT VALUE dosage_mg = 250 WHERE drug_id = 1
ASSERT VALUE approval_status = 'approved' WHERE drug_id = 1
ASSERT VALUE trial_phase = 'Phase IV' WHERE drug_id = 1
ASSERT VALUE priority_score = 9.5 WHERE drug_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.drug_registry_iceberg
WHERE drug_id = 1;


-- ============================================================================
-- Iceberg Verify 3: Spot-Check Post-Evolution Insert — Full Schema Row
-- ============================================================================
-- Immutarget (drug_id=31) was inserted after both ADD COLUMNs, with all
-- 9 columns populated from the start.

ASSERT ROW_COUNT = 1
ASSERT VALUE drug_name = 'Immutarget' WHERE drug_id = 31
ASSERT VALUE category = 'Oncology' WHERE drug_id = 31
ASSERT VALUE dosage_mg = 1200 WHERE drug_id = 31
ASSERT VALUE trial_phase = 'Phase II' WHERE drug_id = 31
ASSERT VALUE priority_score = 9.5 WHERE drug_id = 31
SELECT *
FROM {{zone_name}}.iceberg_demos.drug_registry_iceberg
WHERE drug_id = 31;


-- ============================================================================
-- Iceberg Verify 4: Rejected Drug — Discontinued Phase
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE drug_name = 'Neurozen' WHERE drug_id = 7
ASSERT VALUE approval_status = 'rejected' WHERE drug_id = 7
ASSERT VALUE trial_phase = 'Discontinued' WHERE drug_id = 7
ASSERT VALUE priority_score = 7.5 WHERE drug_id = 7
SELECT *
FROM {{zone_name}}.iceberg_demos.drug_registry_iceberg
WHERE drug_id = 7;


-- ============================================================================
-- Iceberg Verify 5: Per-Category Aggregates Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE drug_count = 9 WHERE category = 'Cardiology'
ASSERT VALUE avg_dosage = 103.33 WHERE category = 'Cardiology'
ASSERT VALUE drug_count = 10 WHERE category = 'Oncology'
ASSERT VALUE avg_dosage = 532.5 WHERE category = 'Oncology'
SELECT
    category,
    COUNT(*) AS drug_count,
    ROUND(AVG(dosage_mg), 2) AS avg_dosage,
    ROUND(AVG(priority_score), 2) AS avg_priority
FROM {{zone_name}}.iceberg_demos.drug_registry_iceberg
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Iceberg Verify 6: Grand Totals Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_drugs = 35
ASSERT VALUE phase_count = 5
ASSERT VALUE all_have_phase = 35
ASSERT VALUE all_have_priority = 35
ASSERT VALUE avg_dosage = 243.43
ASSERT VALUE avg_priority = 8.09
SELECT
    COUNT(*) AS total_drugs,
    COUNT(DISTINCT trial_phase) AS phase_count,
    COUNT(trial_phase) AS all_have_phase,
    COUNT(priority_score) AS all_have_priority,
    ROUND(AVG(dosage_mg), 2) AS avg_dosage,
    ROUND(AVG(priority_score), 2) AS avg_priority
FROM {{zone_name}}.iceberg_demos.drug_registry_iceberg;
