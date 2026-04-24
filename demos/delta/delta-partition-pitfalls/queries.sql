-- ============================================================================
-- Delta Partition Pitfalls — Educational Queries
-- ============================================================================
-- WHAT: Over-partitioning occurs when a table is partitioned by a high-
--       cardinality column (like customer_id), creating many tiny partition
--       directories with very few rows each.
-- WHY:  Each Parquet file has fixed overhead (footer, metadata, column
--       chunks). When files contain only 3 rows, the metadata-to-data ratio
--       is terrible. The engine wastes time opening, seeking, and closing
--       hundreds of tiny files instead of reading a few large ones.
-- HOW:  This demo shows the problem, then fixes it by creating a properly
--       partitioned table (by month) and migrating data with INSERT INTO
--       ...SELECT.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Over-Partitioned Table
-- ============================================================================
-- The events_by_customer table is partitioned by customer_id. With 20 unique
-- customers, Delta created 20 separate partition directories. Each directory
-- holds only 3 rows (one event per month per customer). Let's see:

ASSERT ROW_COUNT = 20
ASSERT VALUE events = 3 WHERE customer_id = 'C05'
ASSERT VALUE events = 3 WHERE customer_id = 'C12'
SELECT customer_id,
       COUNT(*) AS events
FROM {{zone_name}}.delta_demos.events_by_customer
GROUP BY customer_id
ORDER BY customer_id;


-- ============================================================================
-- LEARN: The Small Files Problem — Full Table Scan
-- ============================================================================
-- Analytics queries almost always aggregate by time, not by individual
-- customer. To answer "revenue per month," the engine must open ALL 20
-- partition directories because event_month is a regular column, not the
-- partition column. That's 20 directory listings and 20 small Parquet files
-- just to read 60 rows:

ASSERT ROW_COUNT = 3
ASSERT VALUE monthly_revenue = 488.49 WHERE event_month = '2024-01'
ASSERT VALUE monthly_revenue = 308.95 WHERE event_month = '2024-02'
ASSERT VALUE monthly_revenue = 488.49 WHERE event_month = '2024-03'
SELECT event_month,
       COUNT(*) AS event_count,
       ROUND(SUM(amount), 2) AS monthly_revenue
FROM {{zone_name}}.delta_demos.events_by_customer
GROUP BY event_month
ORDER BY event_month;


-- ============================================================================
-- LEARN: When Over-Partitioning Helps — Single Customer Lookup
-- ============================================================================
-- The ONE query pattern that benefits from customer_id partitioning is a
-- single-customer lookup. Here, the engine prunes 19 of 20 partitions and
-- reads only the customer_id=C05 directory (3 files). But this narrow
-- use case rarely justifies the cost to every other query pattern:

ASSERT ROW_COUNT = 3
ASSERT VALUE event_type = 'page_view' WHERE id = 5
ASSERT VALUE event_type = 'page_view' WHERE id = 25
ASSERT VALUE event_type = 'page_view' WHERE id = 45
SELECT id, customer_id, event_type, page_url, amount, event_month
FROM {{zone_name}}.delta_demos.events_by_customer
WHERE customer_id = 'C05'
ORDER BY id;


-- ============================================================================
-- CONTRAST: Monthly Query Must Scan Everything
-- ============================================================================
-- The most common analytics pattern — time-range filtering — gets ZERO
-- partition pruning benefit. Filtering WHERE event_month = '2024-01' still
-- opens all 20 partition directories because event_month is not the partition
-- column. Every customer directory must be checked for January events:

ASSERT ROW_COUNT = 20
ASSERT VALUE amount = 89.99 WHERE id = 16
ASSERT VALUE amount = 149.50 WHERE id = 17
SELECT id, customer_id, event_type, page_url, amount, created_at
FROM {{zone_name}}.delta_demos.events_by_customer
WHERE event_month = '2024-01'
ORDER BY id;


-- ============================================================================
-- LEARN: Fix It — Create a Properly Partitioned Table
-- ============================================================================
-- The fix: create a new table partitioned by event_month (low cardinality —
-- only 3 distinct values). This produces 3 partition directories with 20
-- rows each, giving the engine large, efficient Parquet files to read.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.events_by_month (
    id           INT,
    customer_id  VARCHAR,
    event_type   VARCHAR,
    page_url     VARCHAR,
    amount       DOUBLE,
    created_at   VARCHAR,
    event_month  VARCHAR
) LOCATION 'events_by_month'
PARTITIONED BY (event_month);


-- Migrate all 60 rows from the over-partitioned table into the new one.
-- INSERT INTO...SELECT reads from events_by_customer and writes into
-- events_by_month, automatically routing each row to the correct
-- event_month partition directory:

ASSERT ROW_COUNT = 60
INSERT INTO {{zone_name}}.delta_demos.events_by_month
SELECT id, customer_id, event_type, page_url, amount, created_at, event_month
FROM {{zone_name}}.delta_demos.events_by_customer;


-- ============================================================================
-- EXPLORE: The Well-Partitioned Table
-- ============================================================================
-- Now we have only 3 partition directories instead of 20. Each directory
-- holds 20 rows — much better file sizes. Let's verify:

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 20 WHERE event_month = '2024-01'
ASSERT VALUE cnt = 20 WHERE event_month = '2024-02'
ASSERT VALUE cnt = 20 WHERE event_month = '2024-03'
SELECT event_month,
       COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.events_by_month
GROUP BY event_month
ORDER BY event_month;


-- ============================================================================
-- LEARN: OPTIMIZE — Compact the New Table
-- ============================================================================
-- After INSERT INTO...SELECT, each partition may contain multiple small
-- files (one per batch or per source partition read). OPTIMIZE compacts
-- these into fewer, larger files within each partition directory,
-- further improving read performance:

OPTIMIZE {{zone_name}}.delta_demos.events_by_month;


-- ============================================================================
-- CONTRAST: Same Monthly Query, Now With Pruning
-- ============================================================================
-- The exact same time-range query from Q4, but now on the properly
-- partitioned table. The engine reads ONLY the event_month=2024-01
-- partition directory — 1 of 3 partitions instead of scanning all 20.
-- Same results, dramatically less I/O:

ASSERT ROW_COUNT = 20
ASSERT VALUE amount = 89.99 WHERE id = 16
ASSERT VALUE amount = 149.50 WHERE id = 17
SELECT id, customer_id, event_type, page_url, amount, created_at
FROM {{zone_name}}.delta_demos.events_by_month
WHERE event_month = '2024-01'
ORDER BY id;


-- ============================================================================
-- LEARN: Partition-Scoped UPDATE on the New Table
-- ============================================================================
-- With proper partitioning, DML operations benefit too. This UPDATE applies
-- a 10% price correction to March purchases. Because event_month is the
-- partition column, only the event_month=2024-03 directory is rewritten.
-- The January and February partitions are untouched:
--
-- March purchases: id=56 ($129.99 -> $142.99), id=57 ($59.50 -> $65.45),
--                  id=58 ($299.00 -> $328.90)

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.events_by_month
SET amount = ROUND(amount * 1.10, 2)
WHERE event_month = '2024-03' AND event_type = 'purchase';


-- ============================================================================
-- EXPLORE: Cross-Partition Analytics — Event Funnel
-- ============================================================================
-- Full table aggregation across all 3 partitions. Even though we only need
-- to open 3 directories (vs 20 in the old table), a full scan is still
-- efficient because each file is well-sized:

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 0.0 WHERE event_type = 'page_view'
ASSERT VALUE total_revenue = 1334.78 WHERE event_type = 'purchase'
SELECT event_type,
       COUNT(*) AS event_count,
       ROUND(SUM(amount), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.events_by_month
GROUP BY event_type
ORDER BY event_count DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify original_total: events_by_customer still has 60 untouched rows
ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.delta_demos.events_by_customer;

-- Verify migrated_total: events_by_month also has 60 rows
ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.delta_demos.events_by_month;

-- Verify over_partitioned_count: 20 distinct customer_id partitions
ASSERT VALUE partition_count = 20
SELECT COUNT(DISTINCT customer_id) AS partition_count
FROM {{zone_name}}.delta_demos.events_by_customer;

-- Verify well_partitioned_count: 3 distinct event_month partitions
ASSERT VALUE partition_count = 3
SELECT COUNT(DISTINCT event_month) AS partition_count
FROM {{zone_name}}.delta_demos.events_by_month;

-- Verify march_update_56: id=56 updated from 129.99 to 142.99
ASSERT VALUE amount = 142.99
SELECT amount FROM {{zone_name}}.delta_demos.events_by_month WHERE id = 56;

-- Verify march_update_57: id=57 updated from 59.50 to 65.45
ASSERT VALUE amount = 65.45
SELECT amount FROM {{zone_name}}.delta_demos.events_by_month WHERE id = 57;

-- Verify march_update_58: id=58 updated from 299.00 to 328.90
ASSERT VALUE amount = 328.9
SELECT amount FROM {{zone_name}}.delta_demos.events_by_month WHERE id = 58;

-- Verify original_untouched: events_by_customer March purchase amounts unchanged
ASSERT VALUE amount = 129.99
SELECT amount FROM {{zone_name}}.delta_demos.events_by_customer WHERE id = 56;

-- Verify jan_revenue_matches: same January revenue in both tables
ASSERT VALUE revenue = 488.49
SELECT ROUND(SUM(amount), 2) AS revenue
FROM {{zone_name}}.delta_demos.events_by_month
WHERE event_month = '2024-01';
