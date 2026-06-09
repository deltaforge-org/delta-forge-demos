-- ============================================================================
-- Delta Bloom Filters — Educational Queries
-- ============================================================================
-- WHAT: Bloom filters are probabilistic indexes that accelerate point lookups
-- WHY:  Without bloom filters, an exact-match query (WHERE txn_id = 'X') must
--       scan every data file's min/max stats — useless for high-cardinality
--       columns where many files share overlapping ranges
-- HOW:  Delta stores a bloom filter per file per indexed column. The filter
--       is a compact bit array that can definitively say "this file does NOT
--       contain value X" (no false negatives) but may occasionally say "maybe"
--       (false positives). This enables data-skipping for point lookups.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Transaction Dataset Overview
-- ============================================================================
-- The table was configured with 'delta.dataSkippingNumIndexedCols' = '8',
-- which tells Delta to collect min/max statistics on all 8 columns.
-- Data was inserted in 3 batches (online purchases, in-store, refunds),
-- creating multiple Parquet files — ideal for demonstrating data skipping.

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 40 WHERE status = 'completed'
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE total_amount BETWEEN 5829.1 AND 5829.4 WHERE status = 'completed'
ASSERT VALUE txn_count = 5 WHERE status = 'disputed'
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE total_amount BETWEEN 1778.9 AND 1779.1 WHERE status = 'disputed'
ASSERT VALUE txn_count = 15 WHERE status = 'refunded'
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE total_amount BETWEEN -4226.5 AND -4226.1 WHERE status = 'refunded'
SELECT status,
       COUNT(*) AS txn_count,
       ROUND(SUM(amount), 2) AS total_amount,
       MIN(txn_date) AS first_date,
       MAX(txn_date) AS last_date
FROM {{zone_name}}.delta_demos.transaction_log
GROUP BY status
ORDER BY status;


-- ============================================================================
-- LEARN: Why Bloom Filters Matter for High-Cardinality Columns
-- ============================================================================
-- Each txn_id is unique (60 distinct values across 60 rows). Min/max stats
-- on txn_id are nearly useless: file 1 might have min='TXN-0001' max='TXN-0030'
-- and file 2 min='TXN-0031' max='TXN-0060'. A query for 'TXN-0025' falls in
-- file 1's range but also overlaps with the max. With bloom filters, the engine
-- checks the filter first and skips files that definitely lack the value.
--
-- Let's verify that all transaction IDs are unique — the ideal bloom filter use case.

-- Verify all 60 transaction IDs are unique (ideal for bloom filter indexing)
ASSERT VALUE total_rows = 60
ASSERT VALUE unique_txn_ids = 60
ASSERT VALUE assessment = 'All unique - ideal for bloom filters'
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT txn_id) AS unique_txn_ids,
       CASE WHEN COUNT(*) = COUNT(DISTINCT txn_id)
            THEN 'All unique - ideal for bloom filters'
            ELSE 'Duplicates exist' END AS assessment
FROM {{zone_name}}.delta_demos.transaction_log;


-- ============================================================================
-- LEARN: Point Lookup — The Query Bloom Filters Accelerate
-- ============================================================================
-- This exact-match query is where bloom filters shine. Without them, every
-- data file would need to be opened and scanned. With bloom filters,
-- the engine checks each file's filter and only reads files that *might*
-- contain 'TXN-0009'.

-- Verify specific transaction lookup returns the expected amount
ASSERT VALUE amount = 950.0
ASSERT VALUE txn_id = 'TXN-0009'
ASSERT ROW_COUNT = 1
SELECT id, txn_id, user_id, merchant, amount, category, status, txn_date
FROM {{zone_name}}.delta_demos.transaction_log
WHERE txn_id = 'TXN-0009';


-- ============================================================================
-- LEARN: EXECPLAN — Verify Bloom Filters Are Skipping Files
-- ============================================================================
-- EXECPLAN shows the execution plan and data-skipping statistics without
-- running the query. For a point lookup on txn_id, files that definitely
-- do not contain the value are skipped by bloom filter. The "Skip Breakdown"
-- section shows BLOOM entries when bloom filters are active.

EXECPLAN SELECT * FROM {{zone_name}}.delta_demos.transaction_log WHERE txn_id = 'TXN-0009';


-- ============================================================================
-- EXPLORE: Transaction Categories and Spending Patterns
-- ============================================================================
-- Breaking down transactions by category shows how different spending
-- categories contribute to the overall transaction volume.

ASSERT ROW_COUNT = 5
ASSERT VALUE txn_count = 11 WHERE category = 'travel'
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE net_amount BETWEEN 1539.9 AND 1540.1 WHERE category = 'travel'
ASSERT VALUE txn_count = 13 WHERE category = 'electronics'
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE net_amount BETWEEN 1137.9 AND 1138.1 WHERE category = 'electronics'
SELECT category,
       COUNT(*) AS txn_count,
       COUNT(*) FILTER (WHERE amount > 0) AS purchases,
       COUNT(*) FILTER (WHERE amount < 0) AS refunds,
       ROUND(SUM(amount), 2) AS net_amount,
       ROUND(AVG(ABS(amount)), 2) AS avg_txn_size
FROM {{zone_name}}.delta_demos.transaction_log
GROUP BY category
ORDER BY net_amount DESC;


-- ============================================================================
-- LEARN: Multi-File Data Layout and Data Skipping
-- ============================================================================
-- This table has data spread across 3 insertion batches (creating separate files).
-- The disputed transactions were UPDATEd in a 4th commit, creating additional
-- files. Let's look at the 5 disputed transactions.

ASSERT ROW_COUNT = 5
SELECT id, txn_id, merchant, amount, status, txn_date
FROM {{zone_name}}.delta_demos.transaction_log
WHERE status = 'disputed'
ORDER BY id;


-- ============================================================================
-- EXPLORE: Refund Analysis
-- ============================================================================
-- Refunds (batch 3) have negative amounts. Matching refunds back to
-- original purchases shows the user_id linkage pattern.

ASSERT ROW_COUNT = 15
SELECT t.txn_id, t.user_id, t.merchant, t.amount, t.status
FROM {{zone_name}}.delta_demos.transaction_log t
WHERE t.status = 'refunded'
ORDER BY t.amount;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Verification of transaction counts, batch distribution, disputes, and lookups.

-- Verify total row count
ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.delta_demos.transaction_log;

-- Verify online transaction count
ASSERT VALUE online_count = 30
SELECT COUNT(*) AS online_count FROM {{zone_name}}.delta_demos.transaction_log WHERE id BETWEEN 1 AND 30;

-- Verify in-store transaction count
ASSERT VALUE instore_count = 15
SELECT COUNT(*) AS instore_count FROM {{zone_name}}.delta_demos.transaction_log WHERE id BETWEEN 31 AND 45;

-- Verify refund count
ASSERT VALUE refund_count = 15
SELECT COUNT(*) AS refund_count FROM {{zone_name}}.delta_demos.transaction_log WHERE id BETWEEN 46 AND 60;

-- Verify disputed count
ASSERT VALUE disputed_count = 5
SELECT COUNT(*) AS disputed_count FROM {{zone_name}}.delta_demos.transaction_log WHERE status = 'disputed';

-- Verify unique transaction IDs
ASSERT VALUE unique_txn_ids = 60
SELECT COUNT(DISTINCT txn_id) AS unique_txn_ids FROM {{zone_name}}.delta_demos.transaction_log;

-- Verify specific transaction lookup
ASSERT VALUE amount = 950.0
SELECT amount FROM {{zone_name}}.delta_demos.transaction_log WHERE txn_id = 'TXN-0009';

-- Verify category count
ASSERT VALUE category_count = 5
SELECT COUNT(DISTINCT category) AS category_count FROM {{zone_name}}.delta_demos.transaction_log;
