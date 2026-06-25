-- ============================================================================
-- Card Payment Ledger - When a B-tree index helps, and when it does NOT
-- ============================================================================
-- 20,000,000 authorizations across ~144 Delta files. txn_id is a scattered
-- key (a random reference), so Delta's per-file min/max stats cannot prune a
-- txn_id lookup: SHOW STATS PLAN reports files_planned = total_files. Even when
-- runtime stats narrow the read to one file and row group, the engine still
-- DECODES that whole ~130k-row batch to extract one row (ACTUAL rows_consumed).
--
-- The single, honest signal in this demo:
--   ACTUAL rows_consumed IS NULL  -> the index served the row, NO scan/decode.
--   ACTUAL rows_consumed populated -> a table scan ran and decoded rows.
--
-- WHEN IT HELPS (rows_consumed -> NULL): equality point lookup and keyed point
--   UPDATE on the indexed key. The index reads/locates exactly one row.
-- WHEN IT HAS NO BENEFIT (rows_consumed stays populated): range scans, lookups
--   on a non-indexed column, broad scans, and COUNT. The engine scans anyway.
-- THE COST: building and maintaining one sorted leaf per row.
-- ============================================================================


-- ============================================================================
-- BASELINE: a point lookup with NO index
-- ============================================================================
-- The planner cannot prune the scattered key: it plans to scan every file.

ASSERT VALUE value = '144' WHERE metric = 'total_files'
ASSERT VALUE value = '144' WHERE metric = 'files_planned'
ASSERT VALUE value = '0'   WHERE metric = 'files_pruned_stats'
SHOW STATS PLAN
SELECT txn_id, card_id, amount, status
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 654435761;

-- At runtime it narrows to one file, but DECODES a whole batch to return one
-- row: rows_consumed is populated (tens of thousands), rows_returned is 1.

ASSERT VALUE value IS NOT NULL WHERE metric = 'rows_consumed'
ASSERT VALUE value = '1' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT txn_id, card_id, amount, status
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 654435761;


-- ============================================================================
-- BUILD: create the B+ tree index on the lookup key
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_txn
    ON TABLE {{zone_name}}.delta_demos.payments (txn_id)
    USING btree
    WITH (auto_update = true);


-- ============================================================================
-- HELPS (1): the SAME point lookup, now with the index
-- ============================================================================
-- No scan is planned at all.

ASSERT VALUE value = '0' WHERE metric = 'files_planned'
SHOW STATS PLAN
SELECT txn_id, card_id, amount, status
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 654435761;

-- And nothing is decoded: rows_consumed IS NULL because the index pointer path
-- read exactly the one row. This is the whole benefit.

ASSERT VALUE value IS NULL WHERE metric = 'rows_consumed'
ASSERT VALUE value = '1' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT txn_id, card_id, amount, status
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 654435761;

-- Correctness: the indexed lookup returns exactly the right row.

ASSERT ROW_COUNT = 1
ASSERT VALUE txn_id = 654435761
ASSERT VALUE card_id = 1
ASSERT VALUE amount = 2.5
ASSERT VALUE status = 'authorized'
SELECT txn_id, card_id, amount, status, authorized_at
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 654435761;


-- ============================================================================
-- THE COST: what the index takes to keep
-- ============================================================================
-- One sorted leaf per row (20,000,000), persisted as a child Delta table, kept
-- current on every write while auto_update is on. That build + storage + write
-- maintenance is the price you pay for the point-lookup and point-update speed.

ASSERT ROW_COUNT = 1
ASSERT VALUE name = 'idx_txn'
ASSERT VALUE algorithm = 'btree'
ASSERT VALUE auto_update = true
ASSERT VALUE status = 'current'
ASSERT VALUE leaf_count = 20000000
DESCRIBE INDEXES ON TABLE {{zone_name}}.delta_demos.payments;


-- ============================================================================
-- NO BENEFIT (1): a range on the indexed key
-- ============================================================================
-- The matching rows are spread across every file (scattered key), so a range
-- still full-scans: rows_consumed stays populated (millions decoded).

ASSERT VALUE value IS NOT NULL WHERE metric = 'rows_consumed'
ASSERT VALUE value = '2,001' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT txn_id
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id BETWEEN 654400000 AND 654500000;


-- ============================================================================
-- NO BENEFIT (2): a lookup on a column that is NOT indexed
-- ============================================================================
-- The index is on txn_id, not card_id, so this is a full scan.

ASSERT VALUE value IS NOT NULL WHERE metric = 'rows_consumed'
ASSERT VALUE value = '100' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT txn_id, amount
FROM {{zone_name}}.delta_demos.payments
WHERE card_id = 1;


-- ============================================================================
-- NO BENEFIT (3): a broad scan that reads most of the table
-- ============================================================================

ASSERT VALUE value IS NOT NULL WHERE metric = 'rows_consumed'
ASSERT VALUE value = '20,000,000' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT txn_id
FROM {{zone_name}}.delta_demos.payments
WHERE amount > 0;


-- ============================================================================
-- NO BENEFIT (4): COUNT(*) is answered from metadata, not the index
-- ============================================================================

ASSERT VALUE total_rows = 20000000
SELECT COUNT(*) AS total_rows
FROM {{zone_name}}.delta_demos.payments;


-- ============================================================================
-- HELPS (2): a keyed point UPDATE
-- ============================================================================
-- An UPDATE is locate + rewrite. The index locates the single row to change;
-- deletion vectors keep the rewrite to a bitmap mark.

UPDATE {{zone_name}}.delta_demos.payments
   SET status = 'refunded'
 WHERE txn_id = 654435761;

ASSERT ROW_COUNT = 1
ASSERT VALUE txn_id = 654435761
ASSERT VALUE status = 'refunded'
SELECT txn_id, status, amount
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 654435761;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- 20,000,000 rows total; 400,000 declined (every 50th id per 5M chunk), the one
-- disputed authorization now refunded, the rest authorized.

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 19599999 WHERE status = 'authorized'
ASSERT VALUE cnt = 400000   WHERE status = 'declined'
ASSERT VALUE cnt = 1        WHERE status = 'refunded'
SELECT status, COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.payments
GROUP BY status
ORDER BY status;
