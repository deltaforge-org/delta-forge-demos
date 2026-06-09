-- ============================================================================
-- Delta Deletion Vectors — Educational Queries
-- ============================================================================
-- WHAT: Deletion vectors (DVs) are bitmap files that mark specific rows as
--       deleted without rewriting the underlying Parquet data files.
-- WHY:  Without DVs, deleting a single row from a Parquet file requires
--       rewriting the entire file — expensive for large tables. DVs make
--       DELETE and UPDATE operations O(changed rows) instead of O(file size).
-- HOW:  When DVs are enabled, DELETE writes a small .bin bitmap file that
--       records which row indices are deleted. Readers skip those rows.
--       UPDATE = DV on old row + write new row to a new file. OPTIMIZE
--       later materializes DVs by rewriting files without the deleted rows.
--       VACUUM then cleans up the orphaned files left behind.
--
-- This demo covers the full DV lifecycle:
--   1. DELETE  — create DVs (two rounds)
--   2. UPDATE  — create DVs for old rows + write new rows
--   3. INSPECT — DESCRIBE DETAIL to see accumulated DVs
--   4. OPTIMIZE — materialize DVs into compacted files
--   5. INSPECT — DESCRIBE DETAIL to confirm DVs are gone
--   6. HISTORY — DESCRIBE HISTORY to see every DV operation logged
--   7. TIME TRAVEL — VERSION AS OF to access pre-DELETE data
--   8. VACUUM  — purge orphaned files, completing the lifecycle
-- ============================================================================


-- ============================================================================
-- BASELINE: Inspect the table before any modifications
-- ============================================================================
-- The table starts with 60 sessions across 3 regions and 4 statuses.
-- Let's see the baseline distribution before we start creating DVs.

ASSERT ROW_COUNT = 4
ASSERT VALUE session_count = 20 WHERE status = 'active'
ASSERT VALUE session_count = 16 WHERE status = 'completed'
ASSERT VALUE session_count = 14 WHERE status = 'expired'
ASSERT VALUE session_count = 10 WHERE status = 'bounced'
SELECT status, COUNT(*) AS session_count,
       ROUND(AVG(duration_ms), 0) AS avg_duration_ms,
       ROUND(AVG(page_views), 1) AS avg_page_views
FROM {{zone_name}}.delta_demos.web_sessions
GROUP BY status
ORDER BY session_count DESC;


-- ============================================================================
-- STEP 1: DELETE — Remove 10 bounced sessions (creates DVs)
-- ============================================================================
-- Bounced sessions (page_views=1, very short duration) add no analytical value.
-- Delta uses deletion vectors: a lightweight bitmap marks these rows as deleted
-- without rewriting the underlying Parquet data files.

ASSERT ROW_COUNT = 10
DELETE FROM {{zone_name}}.delta_demos.web_sessions
WHERE status = 'bounced';
-- Removes ids: 14,15,16,17 (us-east), 33,34,35 (eu-west), 52,53,54 (ap-south)
-- 60 - 10 = 50 rows remaining


-- ============================================================================
-- OBSERVE: Confirm bounced sessions are gone
-- ============================================================================
-- The DELETE did not rewrite any Parquet files — it only created DV bitmap
-- files marking which row indices to skip. Queries automatically filter
-- out rows flagged by DVs, so bounced sessions no longer appear.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 0
SELECT id, session_id, status
FROM {{zone_name}}.delta_demos.web_sessions
WHERE status = 'bounced';


-- ============================================================================
-- STEP 2: DELETE — Remove expired sessions older than 2024-02-01 (more DVs)
-- ============================================================================
-- Expired sessions from January 2024 are stale data. This second DELETE
-- accumulates additional deletion vectors on the same data files.
-- Distribution: us-east 2 (ids 18,19), eu-west 3 (ids 36,37,38),
-- ap-south 3 (ids 55,56,57) — totalling 8 rows.

ASSERT ROW_COUNT = 8
DELETE FROM {{zone_name}}.delta_demos.web_sessions
WHERE status = 'expired' AND started_at < '2024-02-01';
-- 50 - 8 = 42 rows remaining


-- ============================================================================
-- OBSERVE: Remaining expired sessions survived the purge
-- ============================================================================
-- Only expired sessions from February 2024 onward survived the DELETE.
-- Sessions expired before 2024-02-01 were purged as stale data.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 6
SELECT id, session_id, region, started_at, duration_ms
FROM {{zone_name}}.delta_demos.web_sessions
WHERE status = 'expired'
ORDER BY started_at;


-- ============================================================================
-- OBSERVE: Status distribution after both DELETEs
-- ============================================================================
-- Two rounds of DVs now exist on the data files. The original Parquet files
-- are untouched — only small bitmap files were written. Let's confirm
-- 50 - 8 = 42 rows remain.

ASSERT ROW_COUNT = 3
ASSERT VALUE session_count = 20 WHERE status = 'active'
ASSERT VALUE session_count = 16 WHERE status = 'completed'
ASSERT VALUE session_count = 6 WHERE status = 'expired'
SELECT status, COUNT(*) AS session_count,
       ROUND(AVG(duration_ms), 0) AS avg_duration_ms,
       ROUND(AVG(page_views), 1) AS avg_page_views
FROM {{zone_name}}.delta_demos.web_sessions
GROUP BY status
ORDER BY session_count DESC;


-- ============================================================================
-- STEP 3: UPDATE — Upgrade long-running active sessions to completed
-- ============================================================================
-- Active sessions with duration >= 18 seconds have naturally completed — the
-- user finished browsing. We mark them 'completed' and add 5000ms to account
-- for session teardown overhead. UPDATE in Delta with DVs works by marking
-- the old row as deleted (DV) and writing a new row with updated values.
-- Row count stays 42 but status and duration_ms change for 5 rows.

ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.delta_demos.web_sessions
SET status = 'completed', duration_ms = duration_ms + 5000
WHERE status = 'active' AND duration_ms >= 18000;
-- Matches across all 3 regions:
--   id=3  (us-east):  25000 -> 30000
--   id=5  (us-east):  18000 -> 23000
--   id=8  (us-east):  20000 -> 25000
--   id=24 (eu-west):  22000 -> 27000
--   id=42 (ap-south): 21000 -> 26000
-- active: 20 - 5 = 15, completed: 16 + 5 = 21


-- ============================================================================
-- OBSERVE: Verify the UPDATE created DVs for old rows + wrote new rows
-- ============================================================================
-- Verify id=8 duration updated from 20000 to 25000
ASSERT VALUE duration_ms = 25000
SELECT duration_ms FROM {{zone_name}}.delta_demos.web_sessions WHERE id = 8;

-- Verify id=3 duration updated from 25000 to 30000
ASSERT VALUE duration_ms = 30000
SELECT duration_ms FROM {{zone_name}}.delta_demos.web_sessions WHERE id = 3;
-- Under the hood, Delta performed two things for each updated row:
--   1. Marked the old row as deleted (added a DV entry for that row index)
--   2. Wrote a new row with the updated values to a new data file
--
-- The row count stays at 42 — the old rows are "deleted" via DVs and
-- replaced by new rows in new files.

ASSERT ROW_COUNT = 8
SELECT id, session_id, status, duration_ms,
       CASE
           WHEN id IN (3, 5, 8) THEN 'Updated (+5000ms, now completed)'
           ELSE 'Original'
       END AS update_status
FROM {{zone_name}}.delta_demos.web_sessions
WHERE id <= 8
ORDER BY id;


-- ============================================================================
-- INSPECT: Table detail before OPTIMIZE — DVs are accumulated
-- ============================================================================
-- DESCRIBE DETAIL returns one row per data file in the table. Before
-- OPTIMIZE, you'll see the original data files plus new files from the
-- UPDATE. The DV bitmap files are tracked alongside the data files they
-- reference — each row shows whether deletion vectors apply to that file.

ASSERT ROW_COUNT >= 4
DESCRIBE DETAIL {{zone_name}}.delta_demos.web_sessions;


-- ============================================================================
-- STEP 4: OPTIMIZE — Materialize all DVs into compacted files
-- ============================================================================
-- After multiple DELETEs and UPDATEs, the table has accumulated many DVs.
-- OPTIMIZE rewrites the data files, physically removing rows that were
-- marked as deleted by DVs. After OPTIMIZE:
--   - No more DV bitmap files (all materialized)
--   - Fewer, larger data files (small files merged)
--   - Same logical data, better read performance

OPTIMIZE {{zone_name}}.delta_demos.web_sessions;


-- ============================================================================
-- INSPECT: Table detail after OPTIMIZE — DVs are gone
-- ============================================================================
-- After OPTIMIZE, the table is fully compacted. Compare with the pre-OPTIMIZE
-- detail: fewer, larger files and no deletion vectors. The logical data is
-- identical — only the storage layout changed.

ASSERT ROW_COUNT >= 1
DESCRIBE DETAIL {{zone_name}}.delta_demos.web_sessions;


-- ============================================================================
-- OBSERVE: Post-OPTIMIZE — data is now fully compacted
-- ============================================================================
-- The table now contains 42 rows in clean, compacted Parquet files with no
-- lingering deletion vectors. Let's confirm the region distribution.

ASSERT ROW_COUNT = 3
ASSERT VALUE sessions = 14 WHERE region = 'ap-south'
ASSERT VALUE sessions = 14 WHERE region = 'eu-west'
ASSERT VALUE sessions = 14 WHERE region = 'us-east'
SELECT region,
       COUNT(*) AS sessions,
       SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active,
       SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
       SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) AS expired
FROM {{zone_name}}.delta_demos.web_sessions
GROUP BY region
ORDER BY region;


-- ============================================================================
-- EXPLORE: Transaction History — every DV operation is logged
-- ============================================================================
-- DESCRIBE HISTORY reveals the full version log. Each DELETE, UPDATE, and
-- OPTIMIZE is a separate transaction with its own version number:
--   v0: CREATE TABLE  |  v1-3: INSERTs  |  v4-5: DELETEs (DVs created)
--   v6: UPDATE (DVs created + new rows)  |  v7: OPTIMIZE (DVs materialized)

ASSERT ROW_COUNT = 8
DESCRIBE HISTORY {{zone_name}}.delta_demos.web_sessions;


-- ============================================================================
-- EXPLORE: Time Travel — access the original 60 rows before any DELETEs
-- ============================================================================
-- Even after DELETEs and OPTIMIZE, the original data is still accessible via
-- time travel. Version 3 is the state after all three INSERTs completed but
-- before any modifications — all 60 rows intact.

ASSERT VALUE original_count = 60
SELECT COUNT(*) AS original_count
FROM {{zone_name}}.delta_demos.web_sessions VERSION AS OF 3;


-- ============================================================================
-- EXPLORE: Session Duration Distribution by Region
-- ============================================================================
-- After removing bounced sessions (very short) and old expired sessions,
-- the remaining data represents genuine user engagement. The 5 updated
-- sessions had 5000ms added to their duration.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_page_views = 103 WHERE region = 'ap-south'
ASSERT VALUE total_page_views = 106 WHERE region = 'eu-west'
ASSERT VALUE total_page_views = 109 WHERE region = 'us-east'
SELECT region,
       MIN(duration_ms) AS min_duration,
       MAX(duration_ms) AS max_duration,
       ROUND(AVG(duration_ms), 0) AS avg_duration,
       SUM(page_views) AS total_page_views
FROM {{zone_name}}.delta_demos.web_sessions
GROUP BY region
ORDER BY avg_duration DESC;


-- ============================================================================
-- STEP 5: VACUUM — Clean up orphaned files
-- ============================================================================
-- OPTIMIZE left behind the old data files and DV bitmaps as orphaned files.
-- VACUUM removes files no longer referenced by any active transaction,
-- reclaiming storage. This completes the full DV lifecycle:
--   CREATE (DELETE/UPDATE) -> ACCUMULATE -> MATERIALIZE (OPTIMIZE) -> PURGE (VACUUM)

VACUUM {{zone_name}}.delta_demos.web_sessions RETAIN 0 HOURS;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 42 rows remain after DELETEs and UPDATEs
ASSERT ROW_COUNT = 42
SELECT * FROM {{zone_name}}.delta_demos.web_sessions;

-- Verify bounced_gone: all bounced sessions were deleted
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.web_sessions WHERE status = 'bounced';

-- Verify expired_old_gone: expired sessions before 2024-02-01 were purged
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.web_sessions WHERE status = 'expired' AND started_at < '2024-02-01';

-- Verify completed_count: 16 original + 5 upgraded = 21 completed
ASSERT VALUE cnt = 21
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.web_sessions WHERE status = 'completed';

-- Verify active_count: 20 original - 5 upgraded = 15 active
ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.web_sessions WHERE status = 'active';

-- Verify region_distribution: each region has 14 rows after deletions
ASSERT VALUE cnt = 14
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.web_sessions WHERE region = 'us-east';

-- Verify updated_duration: id=8 duration increased by 5000ms (20000 -> 25000)
ASSERT VALUE duration_ms = 25000
SELECT duration_ms FROM {{zone_name}}.delta_demos.web_sessions WHERE id = 8;

-- Verify session_integrity: all 42 remaining rows have unique session_ids
ASSERT VALUE cnt = 42
SELECT COUNT(DISTINCT session_id) AS cnt FROM {{zone_name}}.delta_demos.web_sessions;
