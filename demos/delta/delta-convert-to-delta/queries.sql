-- ============================================================================
-- Delta Convert to Delta — Educational Queries
-- ============================================================================
-- WHAT: Converting to Delta adds a transaction log (_delta_log/) over data
--       files, enabling ACID transactions, schema enforcement, and time travel.
-- WHY:  Raw Parquet files have no transactional guarantees — concurrent writes
--       can corrupt data, there is no schema evolution, and you cannot UPDATE
--       or DELETE individual rows. Delta solves all of these problems.
-- HOW:  The Delta log records every change as a sequence of JSON "actions"
--       (add, remove, metadata). Each commit is an atomic transaction that
--       either fully succeeds or is rolled back.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Inspect the Migrated Data (Before DML Operations)
-- ============================================================================
-- The migrated_flag column distinguishes legacy records (migrated_flag=1)
-- from new records added after migration (migrated_flag=0). This is a
-- common pattern when converting to Delta: tag the origin of each row
-- for lineage tracking.
--
-- Notice the payment_method column still contains legacy abbreviations
-- 'cc' and 'pp' — we will fix these next using Delta UPDATE.

ASSERT ROW_COUNT = 10
SELECT id, customer_name, order_total, payment_method, migrated_flag
FROM {{zone_name}}.delta_demos.legacy_data
ORDER BY id
LIMIT 10;


-- ============================================================================
-- OBSERVE: Current payment method codes (before standardization)
-- ============================================================================
-- You can see 'cc' and 'pp' entries that need to be standardized.

ASSERT ROW_COUNT = 4
SELECT payment_method, COUNT(*) AS order_count
FROM {{zone_name}}.delta_demos.legacy_data
GROUP BY payment_method
ORDER BY order_count DESC;


-- ============================================================================
-- DML: UPDATE — Standardize legacy payment method codes
-- ============================================================================
-- The legacy system used abbreviations 'cc' and 'pp' for payment methods.
-- With flat Parquet, fixing this would require rewriting the entire dataset
-- to new files. With Delta, an UPDATE modifies only the affected rows by
-- writing new data files and recording "remove" (old) + "add" (new) actions
-- in the transaction log — all atomically.

ASSERT ROW_COUNT = 17
UPDATE {{zone_name}}.delta_demos.legacy_data
SET payment_method = 'credit_card'
WHERE payment_method = 'cc';

ASSERT ROW_COUNT = 14
UPDATE {{zone_name}}.delta_demos.legacy_data
SET payment_method = 'paypal'
WHERE payment_method = 'pp';


-- ============================================================================
-- OBSERVE: Payment methods after standardization
-- ============================================================================
-- All 'cc' entries are now 'credit_card' and all 'pp' entries are now
-- 'paypal'. No legacy abbreviations remain. 4 payment methods: credit_card,
-- paypal, bank_transfer, cash.

-- Verify no legacy abbreviations remain
ASSERT VALUE legacy_count = 0
SELECT COUNT(*) AS legacy_count FROM {{zone_name}}.delta_demos.legacy_data WHERE payment_method IN ('cc', 'pp');

ASSERT ROW_COUNT = 4
SELECT payment_method, COUNT(*) AS order_count
FROM {{zone_name}}.delta_demos.legacy_data
GROUP BY payment_method
ORDER BY order_count DESC;


-- ============================================================================
-- OBSERVE: Confirm duplicates exist before deletion
-- ============================================================================
-- During migration, 5 records were identified as duplicates from the legacy
-- system. Let's see them before we remove them.

ASSERT ROW_COUNT = 5
SELECT id, customer_name, order_total, product_category
FROM {{zone_name}}.delta_demos.legacy_data
WHERE id IN (3, 7, 15, 22, 31)
ORDER BY id;


-- ============================================================================
-- DML: DELETE — Remove 5 duplicate legacy records
-- ============================================================================
-- In raw Parquet, there is no way to delete individual rows — you would
-- have to read, filter, and rewrite the entire file. Delta's transaction
-- log records the deletion as a "remove" action, and the old data files
-- remain on disk until VACUUM cleans them up (enabling time travel).
-- Duplicate IDs: 3, 7, 15, 22, 31

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.legacy_data
WHERE id IN (3, 7, 15, 22, 31);


-- ============================================================================
-- OBSERVE: Confirm duplicates are gone
-- ============================================================================
-- This query should return zero rows — all 5 duplicates have been deleted.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 0
SELECT id, customer_name
FROM {{zone_name}}.delta_demos.legacy_data
WHERE id IN (3, 7, 15, 22, 31);


-- ============================================================================
-- DML: OPTIMIZE — Compact files after migration
-- ============================================================================
-- Multiple INSERTs, UPDATEs, and DELETEs created many small files.
-- OPTIMIZE compacts them for better read performance. The data itself
-- is unchanged — only the physical layout is optimized.

OPTIMIZE {{zone_name}}.delta_demos.legacy_data;


-- ============================================================================
-- EXPLORE: Legacy vs. Post-Migration Data Comparison
-- ============================================================================
-- Comparing legacy and new data shows that both coexist seamlessly in the
-- same Delta table. The migrated_flag lets analysts query just the old data,
-- just the new data, or both together.

ASSERT ROW_COUNT = 2
ASSERT VALUE total_revenue = 4665.17 WHERE data_origin = 'Legacy (migrated)'
ASSERT VALUE total_revenue = 1555.23 WHERE data_origin = 'New (post-migration)'
SELECT
    CASE WHEN migrated_flag = 1 THEN 'Legacy (migrated)' ELSE 'New (post-migration)' END AS data_origin,
    COUNT(*) AS row_count,
    ROUND(SUM(order_total), 2) AS total_revenue,
    ROUND(AVG(order_total), 2) AS avg_order_value,
    COUNT(DISTINCT product_category) AS categories
FROM {{zone_name}}.delta_demos.legacy_data
GROUP BY migrated_flag
ORDER BY migrated_flag DESC;


-- ============================================================================
-- EXPLORE: Revenue by Product Category After Migration
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE total_revenue = 3065.46 WHERE product_category = 'Electronics'
ASSERT VALUE total_revenue = 1379.50 WHERE product_category = 'Sports'
SELECT product_category,
       COUNT(*) AS order_count,
       ROUND(SUM(order_total), 2) AS total_revenue,
       ROUND(AVG(order_total), 2) AS avg_order
FROM {{zone_name}}.delta_demos.legacy_data
GROUP BY product_category
ORDER BY total_revenue DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count
ASSERT ROW_COUNT = 45
SELECT * FROM {{zone_name}}.delta_demos.legacy_data;

-- Verify migrated record count
ASSERT VALUE migrated_count = 35
SELECT COUNT(*) FILTER (WHERE migrated_flag = 1) AS migrated_count FROM {{zone_name}}.delta_demos.legacy_data;

-- Verify new data count
ASSERT VALUE new_data_count = 10
SELECT COUNT(*) FILTER (WHERE migrated_flag = 0) AS new_data_count FROM {{zone_name}}.delta_demos.legacy_data;

-- Verify all payments are standardized
ASSERT VALUE standardized_count = 45
SELECT COUNT(*) AS standardized_count FROM {{zone_name}}.delta_demos.legacy_data
WHERE payment_method IN ('credit_card', 'paypal', 'bank_transfer', 'cash');

-- Verify no legacy payment codes remain
ASSERT VALUE legacy_code_count = 0
SELECT COUNT(*) AS legacy_code_count FROM {{zone_name}}.delta_demos.legacy_data
WHERE payment_method IN ('cc', 'pp');

-- Verify category count
ASSERT VALUE category_count = 5
SELECT COUNT(DISTINCT product_category) AS category_count FROM {{zone_name}}.delta_demos.legacy_data;

-- Verify total revenue
ASSERT VALUE total_revenue = 6220.40
SELECT ROUND(SUM(order_total), 2) AS total_revenue FROM {{zone_name}}.delta_demos.legacy_data;

-- Verify duplicates are gone
ASSERT VALUE duplicate_count = 0
SELECT COUNT(*) AS duplicate_count FROM {{zone_name}}.delta_demos.legacy_data WHERE id IN (3, 7, 15, 22, 31);
