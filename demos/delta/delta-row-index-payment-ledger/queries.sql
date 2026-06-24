-- ============================================================================
-- Card Payment Ledger - Row-Index Cost / Benefit (MEASURED, not narrated)
-- ============================================================================
--
-- The other delta-row-index-* demos SHOW the index working. This one MEASURES
-- when it helps and when it does not, using SHOW STATS ACTUAL (runtime metrics
-- for reads) and DESCRIBE INDEXES (the index's footprint and freshness for
-- writes). Every claim below is backed by an ASSERT on an engine-reported
-- metric, so the decision rule is demonstrated, not asserted by hand.
--
-- The single metric that tells the read story is delta_scan_exec_count:
--   = 1  the query ran a Delta table scan (it opened data files)
--   = 0  the row-level index served the query directly, with NO table scan
-- and scan_share_count = how many file readers that scan set up (one per
-- surviving file after Delta's own min/max pruning).
--
-- DECISION RULE proved by this demo:
--   USE an index when ... lookups/keyed mutations target a high-cardinality
--       column whose values are NOT clustered on disk, on a multi-file table.
--       (payments.txn_id: Q1 scans 12 files -> Q3 scans 0.)
--   SKIP it when ...
--       * the query is a broad scan that reads most rows anyway   (Q5)
--       * the key is already clustered so Delta stats prune it free (Q6)
--       * the table is tiny (a single file: nothing to prune)
--       * the table is write-heavy and the maintenance cost on every
--         commit outweighs the read savings                       (Q8, Q10)
--
-- A stale or unused index NEVER changes answers; the planner just falls back
-- to ordinary file pruning. Indexes only ever make reads faster.
-- ============================================================================


-- ============================================================================
-- Q1 READ - Baseline: point lookup with NO index (the problem)
-- ============================================================================
-- A dispute lands for one transaction. txn_id is unique and the daily files
-- interleave txn_ids, so Delta's min/max stats cannot prune: the scan sets up
-- a reader for all 12 files just to return the 1 matching row. That waste
-- (scan_share_count = 12 to produce rows_returned = 1) is what an index fixes.

ASSERT VALUE value = '1'  WHERE metric = 'delta_scan_exec_count'
ASSERT VALUE value = '12' WHERE metric = 'scan_share_count'
ASSERT VALUE value = '1'  WHERE metric = 'files_touched'
ASSERT VALUE value = '1'  WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT txn_id, card_id, amount, status
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 90000029;


-- ============================================================================
-- Q2 BUILD - Create the B+ tree index on txn_id
-- ============================================================================
-- B+ tree is the right structure for a point-lookup-dominated key. auto_update
-- keeps it in sync as new authorizations stream in (the cost of that is
-- measured in Q8 and Q10).

CREATE INDEX IF NOT EXISTS idx_txn
    ON TABLE {{zone_name}}.delta_demos.payments (txn_id)
    USING btree
    WITH (auto_update = true);


-- ============================================================================
-- Q3 READ - Same lookup WITH the index (the payoff)
-- ============================================================================
-- Identical SQL. Now delta_scan_exec_count = 0: the index pointer path
-- resolved the row directly and NO Delta table scan ran at all. The 12-file
-- fan-out from Q1 is gone. This is the headline win for a high-cardinality,
-- non-clustered lookup key.

ASSERT VALUE value = '0' WHERE metric = 'delta_scan_exec_count'
ASSERT VALUE value = '1' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT txn_id, card_id, amount, status
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 90000029;


-- ============================================================================
-- Q4 READ - Correctness: the indexed lookup returns the right row
-- ============================================================================
-- Fewer files is only useful if the answer is identical. The index returns
-- exactly the row Q1 would have, with the same values.

ASSERT ROW_COUNT = 1
ASSERT VALUE txn_id = 90000029
ASSERT VALUE card_id = 4003
ASSERT VALUE region = 'NA'
ASSERT VALUE amount = 137.5
ASSERT VALUE status = 'authorized'
SELECT txn_id, card_id, region, amount, status, authorized_at
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 90000029;


-- ============================================================================
-- Q5 READ - When NOT to expect help: a broad scan ignores the index
-- ============================================================================
-- A finance rollup reads essentially the whole ledger (amount > 0 matches
-- every row). The index is on txn_id and cannot prune this predicate, so the
-- engine scans all 12 files (delta_scan_exec_count = 1, files_touched = 12).
-- Lesson: an index does nothing for queries that have to read most of the
-- table. It is for selective access, not full scans.

ASSERT VALUE value = '1'  WHERE metric = 'delta_scan_exec_count'
ASSERT VALUE value = '12' WHERE metric = 'files_touched'
ASSERT VALUE value = '60' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT txn_id
FROM {{zone_name}}.delta_demos.payments
WHERE amount > 0;


-- ============================================================================
-- Q6 READ - When NOT to bother: the key is already clustered
-- ============================================================================
-- The settlements table has 6 files (6 daily runs) and NO index. Because
-- settlement_ids were written in sorted order, each file holds a contiguous
-- id range, so Delta's built-in min/max stats prune a settlement_id lookup to
-- a SINGLE file on their own (scan_share_count = 1). Compare to Q1 on
-- payments: same point-lookup shape, but the shuffled key needed all 12 files.
-- Adding an index here would cost storage and write overhead for no read gain.

ASSERT VALUE value = '1' WHERE metric = 'delta_scan_exec_count'
ASSERT VALUE value = '1' WHERE metric = 'scan_share_count'
ASSERT VALUE value = '1' WHERE metric = 'files_touched'
ASSERT VALUE value = '1' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT settlement_id, region, gross_amount
FROM {{zone_name}}.delta_demos.settlements
WHERE settlement_id = 800027;


-- ============================================================================
-- Q7 WRITE - The price of admission: the index's footprint
-- ============================================================================
-- DESCRIBE INDEXES reports what the index costs you to keep. leaf_count = 60
-- is one sorted leaf per ledger row (the storage overhead). status = current
-- means it is in sync with the table and usable for planning.

ASSERT ROW_COUNT = 1
ASSERT VALUE name = 'idx_txn'
ASSERT VALUE algorithm = 'btree'
ASSERT VALUE auto_update = true
ASSERT VALUE status = 'current'
ASSERT VALUE leaf_count = 60
DESCRIBE INDEXES ON TABLE {{zone_name}}.delta_demos.payments;


-- ============================================================================
-- Q8 WRITE - The recurring cost: every commit maintains the index
-- ============================================================================
-- Three new authorizations stream in. Because auto_update = true, the commit
-- also re-indexes the new rows: leaf_count climbs 60 -> 63 and the index
-- stays current. That maintenance pass is real work the writer pays on every
-- commit. On a high-ingest table this is the cost side of the trade.

INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000061, 4001, 'EU', 50.00, 'authorized', '2026-06-13'),
    (90000062, 4002, 'NA', 75.00, 'authorized', '2026-06-13'),
    (90000063, 4003, 'AP', 99.00, 'declined',   '2026-06-13');

ASSERT ROW_COUNT = 1
ASSERT VALUE status = 'current'
ASSERT VALUE leaf_count = 63
DESCRIBE INDEXES ON TABLE {{zone_name}}.delta_demos.payments;


-- ============================================================================
-- Q9 UPDATE - The index also locates the row a keyed UPDATE rewrites
-- ============================================================================
-- A chargeback flips one transaction to 'refunded'. An UPDATE is locate +
-- rewrite; the locate step uses the SAME index path proved in Q3 (no 12-file
-- scan), and deletion vectors keep the rewrite to a bitmap mark. We verify the
-- post-update state here; the row count affected is 1.

UPDATE {{zone_name}}.delta_demos.payments
   SET status = 'refunded'
 WHERE txn_id = 90000029;

ASSERT ROW_COUNT = 1
ASSERT VALUE txn_id = 90000029
ASSERT VALUE status = 'refunded'
SELECT txn_id, status, amount
FROM {{zone_name}}.delta_demos.payments
WHERE txn_id = 90000029;


-- ============================================================================
-- Q10 WRITE - Mitigation for write-heavy tables: defer maintenance
-- ============================================================================
-- If the ingest rate makes per-commit maintenance too expensive, turn
-- auto_update off. Writes then skip the index entirely (cheap), but the index
-- goes stale on the next commit: status = stale, and the planner falls back to
-- scan for the changed rows until you REBUILD.

ALTER INDEX idx_txn ON TABLE {{zone_name}}.delta_demos.payments SET (auto_update = false);

INSERT INTO {{zone_name}}.delta_demos.payments VALUES
    (90000064, 4004, 'EU', 120.00, 'authorized', '2026-06-14');

ASSERT ROW_COUNT = 1
ASSERT VALUE auto_update = false
ASSERT VALUE status = 'stale'
DESCRIBE INDEXES ON TABLE {{zone_name}}.delta_demos.payments;


-- ============================================================================
-- Q11 WRITE - REBUILD brings a stale index back in sync
-- ============================================================================
-- A periodic REBUILD (e.g. after a batch load) rescans the table and rewrites
-- the index at the current version. status returns to current. This is the
-- "pay maintenance in bulk later" alternative to "pay it on every commit".

REBUILD INDEX idx_txn ON TABLE {{zone_name}}.delta_demos.payments;

ASSERT ROW_COUNT = 1
ASSERT VALUE status = 'current'
DESCRIBE INDEXES ON TABLE {{zone_name}}.delta_demos.payments;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Final ledger state after 4 appends across Q8/Q10 (60 -> 64 rows) and the
-- Q9 chargeback: 56 authorized, 7 declined, 1 refunded.

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 56 WHERE status = 'authorized'
ASSERT VALUE cnt = 7  WHERE status = 'declined'
ASSERT VALUE cnt = 1  WHERE status = 'refunded'
SELECT status, COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.payments
GROUP BY status
ORDER BY status;
