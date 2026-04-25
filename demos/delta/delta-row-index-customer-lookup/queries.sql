-- ============================================================================
-- Online Retail Customer Lookup with Row-Level Index
-- ============================================================================
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                    WHAT IS A ROW-LEVEL INDEX?                        │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │                                                                      │
--  │ A Delta table is stored as many parquet files. To answer             │
--  │   SELECT * FROM customers WHERE customer_id = 9005                   │
--  │ the engine must figure out which file holds customer_id 9005 — and   │
--  │ then read only that file.                                            │
--  │                                                                      │
--  │ Delta has BUILT-IN min/max statistics for the first ~32 columns of   │
--  │ a table. For those columns, file pruning is free and an index would  │
--  │ be redundant.                                                        │
--  │                                                                      │
--  │ A row-level index extends that pruning to columns Delta doesn't      │
--  │ already track, and refines it from "which file" down to "which       │
--  │ slice of which file". You can think of it as a sorted lookup table:  │
--  │                                                                      │
--  │     customer_id  →  (file_path, row_group_inside_that_file)          │
--  │                                                                      │
--  │ A query that filters on customer_id consults the index, gets the     │
--  │ list of slices to read, and skips everything else.                   │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                       WHEN TO USE AN INDEX                           │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ ✓ Predicates target a specific column repeatedly                     │
--  │   (point lookups, narrow ranges, IN lists, prefix LIKE)              │
--  │ ✓ The column is high-cardinality (most values are distinct)          │
--  │ ✓ Delta's built-in stats don't already prune well for this column    │
--  │ ✓ The column is filtered by UPDATE / DELETE / MERGE — these          │
--  │   operations spend most of their time *finding* the row before       │
--  │   rewriting it. The index turns the find step from a full scan       │
--  │   into a targeted read.                                              │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                    WHEN NOT TO USE AN INDEX                          │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ ✗ The column is in the first ~32 positions and Delta's stats         │
--  │   already prune it for free                                          │
--  │ ✗ Predicates are low-selectivity (matches most rows anyway)          │
--  │ ✗ The table is small (a handful of files) — overhead won't pay back  │
--  │ ✗ The table is heavily written by external tools that don't update   │
--  │   the index — it'll be stale most of the time and ignored            │
--  │ ✗ The column has very few distinct values (e.g. boolean, status)     │
--  │   — bloom filters or partition columns are usually a better fit      │
--  └──────────────────────────────────────────────────────────────────────┘
--
-- IMPORTANT: A stale or unused index NEVER produces wrong answers. If the
-- index can't be used, the engine falls back to ordinary file pruning.
-- Indexes only ever make things *faster*, never *different*.
--
-- This demo shows the four canonical lookup shapes a service desk runs
-- every day: point, range, IN list, and combined index-plus-residual.
-- ============================================================================


-- ============================================================================
-- BUILD: Create the Index
-- ============================================================================
-- Service desk lookups all funnel through customer_id. The index records
-- where each customer_id lives so a query can route straight to that
-- slice instead of scanning every file.
--
-- `auto_update = true` keeps the index in sync as new customers are
-- added — no manual REBUILD step required.

CREATE INDEX idx_customer_id
    ON TABLE {{zone_name}}.delta_demos.customers (customer_id)
    WITH (auto_update = true);


-- ============================================================================
-- EXPLORE: Customer Mix by Tier
-- ============================================================================
-- Standard customers came from a 50-row legacy import; VIPs were migrated
-- separately. The index covers all 60 customers regardless of batch.

ASSERT ROW_COUNT = 2
ASSERT VALUE customer_count = 50 WHERE tier = 'standard'
ASSERT VALUE customer_count = 10 WHERE tier = 'vip'
SELECT tier,
       COUNT(*)                              AS customer_count,
       COUNT(*) FILTER (WHERE active = false) AS inactive_count,
       MIN(customer_id)                      AS min_id,
       MAX(customer_id)                      AS max_id
FROM {{zone_name}}.delta_demos.customers
GROUP BY tier
ORDER BY tier;


-- ============================================================================
-- LEARN: Point Lookup — The Headline Index Use Case
-- ============================================================================
-- A support agent pulls up customer 9005. With the index this is a single
-- targeted read instead of opening every file. Identical SQL works
-- without the index (the index just makes it cheaper).

ASSERT ROW_COUNT = 1
ASSERT VALUE customer_id = 9005
ASSERT VALUE full_name = 'Serena Tachibana'
ASSERT VALUE tier = 'vip'
ASSERT VALUE region = 'AP'
ASSERT VALUE lifetime_spend = 31075.4
SELECT customer_id, email, full_name, tier, region, signup_date, lifetime_spend, active
FROM {{zone_name}}.delta_demos.customers
WHERE customer_id = 9005;


-- ============================================================================
-- LEARN: Range Scan — Index Narrows to a Contiguous Slice
-- ============================================================================
-- Fraud review: pull customer IDs 1010..1019 to check a recent batch.
-- The index narrows to the contiguous slice carrying those IDs;
-- everything outside the range is skipped without opening the file.

ASSERT ROW_COUNT = 10
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE total_spend BETWEEN 4475.6 AND 4475.8
ASSERT VALUE na_count = 4
SELECT COUNT(*)                                AS row_count,
       ROUND(SUM(lifetime_spend), 2)           AS total_spend,
       COUNT(*) FILTER (WHERE region = 'NA')   AS na_count
FROM {{zone_name}}.delta_demos.customers
WHERE customer_id BETWEEN 1010 AND 1019;


-- ============================================================================
-- LEARN: IN List — Reconciling a Ticket Batch
-- ============================================================================
-- Service desk reconciles four open tickets. An IN list against an
-- indexed column resolves to one targeted read per matching slice;
-- non-matching slices are skipped entirely.

ASSERT ROW_COUNT = 4
ASSERT VALUE customer_id IN (1002, 1015, 9003, 9007)
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE lifetime_spend BETWEEN 24355.7 AND 24355.8 WHERE customer_id = 9003
SELECT customer_id, full_name, tier, region, lifetime_spend
FROM {{zone_name}}.delta_demos.customers
WHERE customer_id IN (1002, 1015, 9003, 9007)
ORDER BY customer_id;


-- ============================================================================
-- LEARN: Combining Index Lookup with a Residual Filter
-- ============================================================================
-- The index narrows on customer_id; the residual `tier = 'vip'` and
-- `lifetime_spend >= 20000` filters are applied after the index has
-- selected which slices to read. This is how the planner combines
-- index pruning with regular predicate evaluation.

ASSERT ROW_COUNT = 5
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE total_spend BETWEEN 126767.1 AND 126767.3
SELECT COUNT(*)                       AS vip_count,
       ROUND(SUM(lifetime_spend), 2)  AS total_spend,
       MAX(lifetime_spend)            AS top_spender_amount
FROM {{zone_name}}.delta_demos.customers
WHERE customer_id BETWEEN 9001 AND 9010
  AND tier = 'vip'
  AND lifetime_spend >= 20000;


-- ============================================================================
-- LEARN: Index Metadata
-- ============================================================================
-- DESCRIBE INDEX exposes the index's status. After CREATE INDEX with
-- auto_update=true the status is `current` — usable for queries.
-- A status of `stale` would mean the planner is ignoring it and
-- falling back to ordinary file pruning.

DESCRIBE INDEX idx_customer_id ON TABLE {{zone_name}}.delta_demos.customers;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: counts, regional split, ID extremes,
-- aggregate spend across all 60 customers.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 60
ASSERT VALUE active_count = 55
ASSERT VALUE inactive_count = 5
ASSERT VALUE eu_count = 28
ASSERT VALUE na_count = 23
ASSERT VALUE ap_count = 9
ASSERT VALUE min_id = 1000
ASSERT VALUE max_id = 9010
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE total_spend BETWEEN 224251.3 AND 224251.5
SELECT COUNT(*)                                  AS total_rows,
       COUNT(*) FILTER (WHERE active = true)     AS active_count,
       COUNT(*) FILTER (WHERE active = false)    AS inactive_count,
       COUNT(*) FILTER (WHERE region = 'EU')     AS eu_count,
       COUNT(*) FILTER (WHERE region = 'NA')     AS na_count,
       COUNT(*) FILTER (WHERE region = 'AP')     AS ap_count,
       MIN(customer_id)                          AS min_id,
       MAX(customer_id)                          AS max_id,
       ROUND(SUM(lifetime_spend), 2)             AS total_spend
FROM {{zone_name}}.delta_demos.customers;
