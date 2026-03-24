-- ============================================================================
-- Delta DV Accumulation — Performance Degradation & Recovery
-- ============================================================================
-- WHAT: Each individual DELETE creates a new deletion vector (DV) bitmap file.
--       When GDPR/HIPAA deletion requests arrive in small batches over time,
--       DV files accumulate, adding I/O overhead to every query — readers must
--       open and apply each DV bitmap before returning results.
-- WHY:  In healthcare systems, compliance deletions are ongoing: patient
--       consent withdrawals, record expiry, and data minimisation requests
--       each generate a separate DELETE transaction. Without periodic
--       compaction, query performance degrades proportionally to DV count.
-- HOW:  OPTIMIZE materializes all accumulated DVs by rewriting data files
--       without the deleted rows, collapsing many small DV bitmaps into
--       zero. This restores read performance to baseline.
--
-- This demo simulates:
--   1. BASELINE  — 60 patient visits, 20 per department
--   2. ROUND 1   — GDPR batch: delete 3 cancelled visits (DV file #1)
--   3. ROUND 2   — GDPR batch: delete 3 more cancelled visits (DV file #2)
--   4. ROUND 3   — Data minimisation: delete 4 low-cost visits (DV file #3)
--   5. INSPECT    — Verify 50 rows remain, check accumulated DV state
--   6. OPTIMIZE   — Materialize all DVs into clean data files
--   7. SUMMARY    — Post-optimize department-level aggregates
--   8. VERIFY     — Final correctness checks
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — department distribution before any deletions
-- ============================================================================
-- The table starts with 60 patient visits evenly distributed across 3
-- departments. Each department has 20 visits with a mix of active,
-- discharged, and cancelled statuses.

ASSERT ROW_COUNT = 3
ASSERT VALUE visit_count = 20 WHERE department = 'cardiology'
ASSERT VALUE visit_count = 20 WHERE department = 'neurology'
ASSERT VALUE visit_count = 20 WHERE department = 'orthopedics'
SELECT department, COUNT(*) AS visit_count,
       SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active,
       SUM(CASE WHEN status = 'discharged' THEN 1 ELSE 0 END) AS discharged,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
FROM {{zone_name}}.delta_demos.patient_visits
GROUP BY department
ORDER BY department;


-- ============================================================================
-- STEP 1: GDPR Round 1 — Delete 3 cancelled visits (DV file #1)
-- ============================================================================
-- A patient consent withdrawal triggers deletion of cancelled visits for
-- PAT-005 (cardiology id=5), PAT-023 (orthopedics id=25), and PAT-005
-- (neurology id=45). Delta writes a single DV bitmap file marking these
-- 3 row indices as deleted — no Parquet rewrite needed.

ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.delta_demos.patient_visits
WHERE id IN (5, 25, 45);
-- Removes: id=5 (echocardiogram review, cancelled, $680)
--          id=25 (fracture follow-up, cancelled, $350)
--          id=45 (memory assessment, cancelled, $450)
-- 60 - 3 = 57 rows remaining, 1 DV file accumulated


-- ============================================================================
-- STEP 2: GDPR Round 2 — Delete 3 more cancelled visits (DV file #2)
-- ============================================================================
-- A second compliance batch arrives: PAT-009 (cardiology id=10), PAT-027
-- (orthopedics id=30), PAT-010 (neurology id=50). Delta creates another
-- DV bitmap file — now 2 DV files exist on the data files.

ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.delta_demos.patient_visits
WHERE id IN (10, 30, 50);
-- Removes: id=10 (lipid panel review, cancelled, $310)
--          id=30 (ankle sprain evaluation, cancelled, $220)
--          id=50 (sleep study review, cancelled, $390)
-- 57 - 3 = 54 rows remaining, 2 DV files accumulated


-- ============================================================================
-- STEP 3: Data Minimisation Round 3 — Delete 4 low-cost visits (DV file #3)
-- ============================================================================
-- A data minimisation review flags visits costing under $200 as
-- administrative noise — brief triage encounters that should not persist.
-- This third DELETE creates yet another DV bitmap file, bringing the
-- accumulated total to 3 separate DV files that readers must process.

ASSERT ROW_COUNT = 4
DELETE FROM {{zone_name}}.delta_demos.patient_visits
WHERE cost < 200.00;
-- Removes: id=3  (chest pain evaluation, discharged, $175)
--          id=23 (knee pain assessment, discharged, $185)
--          id=43 (nerve conduction study, discharged, $190)
--          id=58 (lumbar puncture consult, discharged, $160)
-- 54 - 4 = 50 rows remaining, 3 DV files accumulated


-- ============================================================================
-- LEARN: Verify accumulation — 50 rows remain across 3 departments
-- ============================================================================
-- After 3 rounds of deletes, 10 rows have been removed. The original
-- Parquet data files are untouched — only 3 DV bitmap files were written.
-- Every query must now read all 3 DV files to filter out the 10 deleted
-- rows, adding I/O overhead proportional to the number of DV files.

ASSERT ROW_COUNT = 3
ASSERT VALUE visit_count = 17 WHERE department = 'cardiology'
ASSERT VALUE visit_count = 16 WHERE department = 'neurology'
ASSERT VALUE visit_count = 17 WHERE department = 'orthopedics'
SELECT department, COUNT(*) AS visit_count,
       SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active,
       SUM(CASE WHEN status = 'discharged' THEN 1 ELSE 0 END) AS discharged,
       SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
FROM {{zone_name}}.delta_demos.patient_visits
GROUP BY department
ORDER BY department;


-- ============================================================================
-- INSPECT: Table detail before OPTIMIZE — DVs are accumulated
-- ============================================================================
-- DESCRIBE DETAIL shows the physical file layout. Before OPTIMIZE, you'll
-- see the original data files with accumulated DV bitmaps referencing them.
-- Each DV file adds read overhead — readers must open and merge all DV
-- bitmaps before scanning the data file.

ASSERT ROW_COUNT >= 3
DESCRIBE DETAIL {{zone_name}}.delta_demos.patient_visits;


-- ============================================================================
-- STEP 4: OPTIMIZE — Materialize all accumulated DVs
-- ============================================================================
-- OPTIMIZE rewrites the data files, physically removing the 10 rows marked
-- as deleted across 3 DV bitmap files. After OPTIMIZE:
--   - Zero DV bitmap files remain (all materialized into data)
--   - Fewer, cleaner data files (deleted rows physically gone)
--   - Same 50 logical rows, but queries no longer pay DV overhead
-- This is the recovery step — performance returns to baseline.

OPTIMIZE {{zone_name}}.delta_demos.patient_visits;


-- ============================================================================
-- INSPECT: Table detail after OPTIMIZE — DVs are gone
-- ============================================================================
-- After OPTIMIZE, the table is fully compacted. Compare with the
-- pre-OPTIMIZE detail: the DV bitmap files are gone. Queries now read
-- clean data files directly, with no per-row bitmap filtering needed.

ASSERT ROW_COUNT >= 1
DESCRIBE DETAIL {{zone_name}}.delta_demos.patient_visits;


-- ============================================================================
-- EXPLORE: Post-optimize department summary
-- ============================================================================
-- With DVs materialized, this aggregate query reads compacted files with
-- no DV overhead. The numbers reflect all 3 deletion rounds:
-- 10 rows removed (6 cancelled + 4 low-cost discharged), 50 remain.

ASSERT ROW_COUNT = 3
ASSERT VALUE visit_count = 17 WHERE department = 'cardiology'
ASSERT VALUE visit_count = 16 WHERE department = 'neurology'
ASSERT VALUE visit_count = 17 WHERE department = 'orthopedics'
SELECT department,
       COUNT(*) AS visit_count,
       ROUND(AVG(cost), 2) AS avg_cost,
       SUM(cost) AS total_cost,
       MIN(cost) AS min_cost,
       MAX(cost) AS max_cost
FROM {{zone_name}}.delta_demos.patient_visits
GROUP BY department
ORDER BY department;
-- Expected:
--   cardiology:  17 visits, avg $1234.41, total $20985.00
--   neurology:   16 visits, avg $1032.50, total $16520.00
--   orthopedics: 17 visits, avg $1123.53, total $19100.00


-- ============================================================================
-- EXPLORE: Transaction History — every DV operation is logged
-- ============================================================================
-- DESCRIBE HISTORY reveals the full version log. Each DELETE and OPTIMIZE
-- is a separate transaction with its own version number:
--   v0: CREATE TABLE  |  v1-3: INSERTs (one per department)
--   v4-6: DELETEs (DVs created — one per GDPR/minimisation round)
--   v7: OPTIMIZE (DVs materialized into compacted files)

ASSERT ROW_COUNT = 8
DESCRIBE HISTORY {{zone_name}}.delta_demos.patient_visits;


-- ============================================================================
-- EXPLORE: Time Travel — access the original 60 rows before any deletions
-- ============================================================================
-- Even after 3 DELETE rounds and OPTIMIZE, the original data is accessible
-- via time travel. Version 3 is the state after all 3 INSERTs completed
-- but before any deletions — all 60 patient visits intact.

ASSERT VALUE original_count = 60
SELECT COUNT(*) AS original_count
FROM {{zone_name}}.delta_demos.patient_visits VERSION AS OF 3;


-- ============================================================================
-- STEP 5: VACUUM — Purge orphaned files
-- ============================================================================
-- OPTIMIZE left behind the old data files and DV bitmaps as orphaned files.
-- VACUUM removes files no longer referenced by any active transaction,
-- reclaiming storage. This completes the full DV accumulation lifecycle:
--   CREATE (3x DELETE) -> ACCUMULATE DVs -> MATERIALIZE (OPTIMIZE) -> PURGE (VACUUM)

VACUUM {{zone_name}}.delta_demos.patient_visits RETAIN 0 HOURS;


-- ============================================================================
-- VERIFY: Final correctness checks
-- ============================================================================

-- Verify total_row_count: 50 rows remain after 10 deletions
ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.delta_demos.patient_visits;

-- Verify deleted_cancelled_gone: the 6 cancelled visits from rounds 1 & 2 are gone
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.patient_visits
WHERE id IN (5, 10, 25, 30, 45, 50);

-- Verify deleted_lowcost_gone: the 4 low-cost visits from round 3 are gone
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.patient_visits
WHERE id IN (3, 23, 43, 58);

-- Verify remaining_cancelled: only 3 cancelled visits survive (ids 19, 40, 59)
ASSERT VALUE cnt = 3
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.patient_visits
WHERE status = 'cancelled';

-- Verify active_count: 29 active visits unchanged by the deletes
ASSERT VALUE cnt = 29
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.patient_visits
WHERE status = 'active';

-- Verify discharged_count: 22 original - 4 low-cost = 18 discharged
ASSERT VALUE cnt = 18
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.patient_visits
WHERE status = 'discharged';

-- Verify no_cheap_visits: all visits under $200 were deleted in round 3
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.patient_visits
WHERE cost < 200.00;

-- Verify cardiology_total_cost: $20985.00 across 17 remaining visits
ASSERT VALUE total_cost = 20985.00
SELECT SUM(cost) AS total_cost FROM {{zone_name}}.delta_demos.patient_visits
WHERE department = 'cardiology';

-- Verify neurology_total_cost: $16520.00 across 16 remaining visits
ASSERT VALUE total_cost = 16520.00
SELECT SUM(cost) AS total_cost FROM {{zone_name}}.delta_demos.patient_visits
WHERE department = 'neurology';

-- Verify orthopedics_total_cost: $19100.00 across 17 remaining visits
ASSERT VALUE total_cost = 19100.00
SELECT SUM(cost) AS total_cost FROM {{zone_name}}.delta_demos.patient_visits
WHERE department = 'orthopedics';
