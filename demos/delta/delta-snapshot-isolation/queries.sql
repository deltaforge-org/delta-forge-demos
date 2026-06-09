-- ============================================================================
-- Delta Snapshot Isolation — Educational Queries
-- ============================================================================
-- WHAT: Every Delta write is an atomic transaction producing a new version.
--       Readers querying an older version see a complete, consistent snapshot
--       even while new writes are in progress.
-- WHY:  In production, portfolio rebalancing (OPTIMIZE) or price adjustments
--       (UPDATE) can take minutes. Fund managers querying the portfolio during
--       that window must not see half-written data — they need snapshot isolation.
-- HOW:  Delta's transaction log guarantees that VERSION AS OF N always returns
--       exactly the data committed at version N, regardless of later writes.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline portfolio state (version 3 — after all 3 batch inserts)
-- ============================================================================
-- Three overnight fund loads created 60 positions across 4 funds. Each INSERT
-- was its own transaction (versions 1, 2, 3), each creating a separate data
-- file. This query captures the baseline state that later versions must preserve.

ASSERT VALUE total_positions = 60
ASSERT VALUE distinct_funds = 4
ASSERT VALUE distinct_sectors = 7
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_positions,
       COUNT(DISTINCT fund_id) AS distinct_funds,
       COUNT(DISTINCT sector) AS distinct_sectors
FROM {{zone_name}}.delta_demos.fund_holdings;


-- ============================================================================
-- EXPLORE: Fund-level portfolio values — the numbers managers rely on
-- ============================================================================
-- Each fund's total market value (shares × price) is the figure that portfolio
-- managers use for allocation decisions. These values must remain stable when
-- queried via VERSION AS OF, even after OPTIMIZE rewrites the underlying files.

ASSERT VALUE total_market_value = 476910.00 WHERE fund_id = 'GF01'
ASSERT VALUE total_market_value = 1015445.00 WHERE fund_id = 'VF02'
ASSERT ROW_COUNT = 4
SELECT fund_id,
       COUNT(*) AS positions,
       SUM(shares * price) AS total_market_value
FROM {{zone_name}}.delta_demos.fund_holdings
GROUP BY fund_id
ORDER BY fund_id;


-- ============================================================================
-- ACTION: OPTIMIZE — compact 3 batch files into 1 optimal file
-- ============================================================================
-- This is a physical-only operation: it reads all 3 data files, writes one
-- compacted file, and atomically swaps them in the transaction log (version 4).
-- The data content is unchanged — only the file layout improves.
-- During this operation, any reader at version 3 still sees the old 3 files.

OPTIMIZE {{zone_name}}.delta_demos.fund_holdings;


-- ============================================================================
-- PROVE: Post-OPTIMIZE data integrity — identical to baseline
-- ============================================================================
-- The latest version (4, post-OPTIMIZE) must have exactly the same row count
-- and portfolio values as version 3. If any number differs, OPTIMIZE corrupted
-- data — which should never happen.

ASSERT VALUE total_positions = 60
ASSERT VALUE total_market_value = 3802542.00
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_positions,
       SUM(shares * price) AS total_market_value
FROM {{zone_name}}.delta_demos.fund_holdings;


-- ============================================================================
-- PROVE: Version 3 snapshot is identical to version 4
-- ============================================================================
-- This is the snapshot isolation guarantee: querying version 3 (pre-OPTIMIZE)
-- returns the same data as version 4 (post-OPTIMIZE). The physical file
-- reorganization is invisible to readers at any version.

ASSERT VALUE total_positions = 60
ASSERT VALUE total_market_value = 3802542.00
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_positions,
       SUM(shares * price) AS total_market_value
FROM {{zone_name}}.delta_demos.fund_holdings VERSION AS OF 3;


-- ============================================================================
-- ACTION: UPDATE — 5% price adjustment on GF01 Technology holdings
-- ============================================================================
-- The Growth Fund's technology positions receive a 5% price mark-up reflecting
-- after-hours earnings beats. This UPDATE creates version 5 with new data files
-- via copy-on-write. Readers at versions 3 or 4 are unaffected.

ASSERT ROW_COUNT = 4
UPDATE {{zone_name}}.delta_demos.fund_holdings
SET price = ROUND(price * 1.05, 2)
WHERE fund_id = 'GF01' AND sector = 'Technology';


-- ============================================================================
-- PROVE: Current state reflects the price adjustment
-- ============================================================================
-- Version 5 (latest) shows the updated portfolio value. Only GF01's value
-- changed — the other three funds are untouched.

ASSERT VALUE total_market_value = 489751.50 WHERE fund_id = 'GF01'
ASSERT VALUE total_market_value = 1015445.00 WHERE fund_id = 'VF02'
ASSERT ROW_COUNT = 4
SELECT fund_id,
       COUNT(*) AS positions,
       SUM(shares * price) AS total_market_value
FROM {{zone_name}}.delta_demos.fund_holdings
GROUP BY fund_id
ORDER BY fund_id;


-- ============================================================================
-- PROVE: Version 3 snapshot — still shows the original pre-update values
-- ============================================================================
-- A fund manager who pinned their report to version 3 sees the original prices.
-- The UPDATE in version 5 is completely invisible. This is snapshot isolation:
-- each version is a self-contained, immutable view of the data.

ASSERT VALUE total_market_value = 476910.00 WHERE fund_id = 'GF01'
ASSERT VALUE total_market_value = 1015445.00 WHERE fund_id = 'VF02'
ASSERT ROW_COUNT = 4
SELECT fund_id,
       COUNT(*) AS positions,
       SUM(shares * price) AS total_market_value
FROM {{zone_name}}.delta_demos.fund_holdings VERSION AS OF 3
GROUP BY fund_id
ORDER BY fund_id;


-- ============================================================================
-- LEARN: Cross-version comparison — quantify the price adjustment impact
-- ============================================================================
-- By joining the current version against version 3, we can compute the exact
-- dollar impact of the price adjustment per fund. Only GF01 shows a difference.

ASSERT VALUE value_change = 12841.50 WHERE fund_id = 'GF01'
ASSERT ROW_COUNT = 4
SELECT curr.fund_id,
       old.total_value AS value_before,
       curr.total_value AS value_after,
       curr.total_value - old.total_value AS value_change
FROM (
    SELECT fund_id, SUM(shares * price) AS total_value
    FROM {{zone_name}}.delta_demos.fund_holdings
    GROUP BY fund_id
) curr
JOIN (
    SELECT fund_id, SUM(shares * price) AS total_value
    FROM {{zone_name}}.delta_demos.fund_holdings VERSION AS OF 3
    GROUP BY fund_id
) old ON curr.fund_id = old.fund_id
ORDER BY curr.fund_id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 60
ASSERT VALUE total_rows = 60
SELECT COUNT(*) AS total_rows FROM {{zone_name}}.delta_demos.fund_holdings;

-- Verify 4 funds
ASSERT VALUE fund_count = 4
SELECT COUNT(DISTINCT fund_id) AS fund_count FROM {{zone_name}}.delta_demos.fund_holdings;

-- Verify 7 sectors
ASSERT VALUE sector_count = 7
SELECT COUNT(DISTINCT sector) AS sector_count FROM {{zone_name}}.delta_demos.fund_holdings;

-- Verify GF01 has 10 holdings
ASSERT VALUE gf01_count = 10
SELECT COUNT(*) AS gf01_count FROM {{zone_name}}.delta_demos.fund_holdings WHERE fund_id = 'GF01';

-- Verify 4 GF01 tech rows were updated
ASSERT VALUE gf01_tech = 4
SELECT COUNT(*) AS gf01_tech FROM {{zone_name}}.delta_demos.fund_holdings WHERE fund_id = 'GF01' AND sector = 'Technology';

-- Verify non-GF01-tech value unchanged from original
ASSERT VALUE unchanged_value = 3545777.00
SELECT SUM(shares * price) AS unchanged_value FROM {{zone_name}}.delta_demos.fund_holdings WHERE NOT (fund_id = 'GF01' AND sector = 'Technology');

-- Verify AAPL new price after 5% adjustment
ASSERT VALUE price = 194.78
SELECT price FROM {{zone_name}}.delta_demos.fund_holdings WHERE id = 1;
