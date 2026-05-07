-- ============================================================================
-- Delta Maintenance Playbook — OPTIMIZE Then VACUUM — Educational Queries
-- ============================================================================
-- WHAT: The standard two-step Delta Lake maintenance sequence:
--       Step 1 — OPTIMIZE compacts fragmented small files into fewer large ones
--       Step 2 — VACUUM RETAIN 0 HOURS purges orphaned pre-compaction files
-- WHY:  Daily micro-batch pipelines create many small files (the "small files
--       problem"), degrading read performance. Mutations (UPDATE/DELETE) add
--       orphaned file versions. Without maintenance, both problems compound.
-- HOW:  DESCRIBE DETAIL at each stage shows the physical file state changing
--       while data integrity stays constant throughout.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Pre-maintenance — fragmented state after daily micro-batches
-- ============================================================================
-- Five daily INSERT batches created 5+ separate Parquet files. Three subsequent
-- mutations (ship, cancel, price fix) rewrote some of those files, adding
-- orphaned versions. DESCRIBE DETAIL reveals the current file count.

-- Non-deterministic: num_files depends on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.vacuum_order_pipeline;


-- ============================================================================
-- EXPLORE: Category revenue breakdown — the data integrity baseline
-- ============================================================================
-- Before any maintenance runs, capture the exact category-level metrics.
-- These numbers must be identical after OPTIMIZE and after VACUUM — both
-- are physical-only operations that never change logical data.

ASSERT VALUE order_count = 10 WHERE category = 'Electronics'
ASSERT VALUE order_count = 9 WHERE category = 'Clothing'
ASSERT VALUE order_count = 9 WHERE category = 'Home'
ASSERT VALUE order_count = 9 WHERE category = 'Books'
ASSERT ROW_COUNT = 4
SELECT category,
       COUNT(*) AS order_count,
       ROUND(SUM(price), 2) AS revenue
FROM {{zone_name}}.delta_demos.vacuum_order_pipeline
GROUP BY category
ORDER BY category;


-- ============================================================================
-- EXPLORE: Order status distribution — shipped vs pending
-- ============================================================================
-- Monday's batch was shipped (8 orders). The rest remain pending.
-- Three cancelled orders have already been deleted and won't appear.

ASSERT VALUE status_count = 8 WHERE status = 'shipped'
ASSERT VALUE status_count = 29 WHERE status = 'pending'
ASSERT ROW_COUNT = 2
SELECT status,
       COUNT(*) AS status_count,
       ROUND(SUM(price), 2) AS status_revenue
FROM {{zone_name}}.delta_demos.vacuum_order_pipeline
GROUP BY status
ORDER BY status;


-- ============================================================================
-- STEP 1: OPTIMIZE — compact fragmented files
-- ============================================================================
-- OPTIMIZE reads all the small Parquet files and rewrites them into fewer,
-- larger files. This dramatically improves read performance by reducing
-- file-open overhead. The old small files become orphaned — they're still
-- on disk but no longer referenced by the current table version.

OPTIMIZE {{zone_name}}.delta_demos.vacuum_order_pipeline;


-- ============================================================================
-- LEARN: Post-OPTIMIZE state — compacted but orphans remain
-- ============================================================================
-- After OPTIMIZE, the table references fewer, larger files. But the old
-- small files from the 5 daily batches and 3 mutations are still on disk
-- as orphaned files. DESCRIBE DETAIL shows the current (compacted) file
-- count, but storage still contains the orphans.

-- Non-deterministic: num_files depends on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.vacuum_order_pipeline;


-- ============================================================================
-- STEP 2: VACUUM RETAIN 0 HOURS — purge orphaned files
-- ============================================================================
-- The second step of maintenance. VACUUM scans storage for files not
-- referenced by the current table version and deletes them. RETAIN 0 HOURS
-- removes ALL orphans immediately (default retention is 7 days).
-- After this, storage contains only the compacted files from OPTIMIZE.

VACUUM {{zone_name}}.delta_demos.vacuum_order_pipeline RETAIN 0 HOURS;


-- ============================================================================
-- LEARN: Post-maintenance state — clean and compact
-- ============================================================================
-- DESCRIBE DETAIL now shows the final clean state. Only the compacted files
-- from OPTIMIZE remain on disk. No orphans, no small file fragments.
-- The version has advanced (OPTIMIZE added one), but VACUUM did not add
-- another — it's a physical-only operation.

-- Non-deterministic: num_files depends on engine write strategy
ASSERT WARNING ROW_COUNT >= 10
DESCRIBE DETAIL {{zone_name}}.delta_demos.vacuum_order_pipeline;


-- ============================================================================
-- LEARN: Post-maintenance data integrity — zero data change
-- ============================================================================
-- The critical proof: after OPTIMIZE + VACUUM, every aggregate is identical
-- to the pre-maintenance baseline. File compaction and orphan cleanup are
-- purely physical operations.

ASSERT VALUE total_orders = 37
ASSERT VALUE total_revenue = 2143.77
ASSERT VALUE categories = 4
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_orders,
       ROUND(SUM(price), 2) AS total_revenue,
       COUNT(DISTINCT category) AS categories
FROM {{zone_name}}.delta_demos.vacuum_order_pipeline;


-- ============================================================================
-- LEARN: Shipped orders survived compaction and cleanup
-- ============================================================================
-- Monday's 8 shipped orders span the first daily batch. OPTIMIZE merged
-- their file with other batches, and VACUUM removed the old small file.
-- Every shipped order is intact with correct prices and status.

ASSERT ROW_COUNT = 8
ASSERT VALUE shipped_revenue = 694.94
SELECT id, order_ref, product, price, status,
       ROUND(SUM(price) OVER (), 2) AS shipped_revenue
FROM {{zone_name}}.delta_demos.vacuum_order_pipeline
WHERE status = 'shipped'
ORDER BY id;


-- ============================================================================
-- LEARN: Cancelled orders remain deleted after maintenance
-- ============================================================================
-- Orders 11, 22, and 37 were deleted before maintenance. OPTIMIZE and
-- VACUUM do not resurrect deleted rows — they only affect physical files.

ASSERT VALUE cancelled_count = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS cancelled_count
FROM {{zone_name}}.delta_demos.vacuum_order_pipeline
WHERE id IN (11, 22, 37);


-- ============================================================================
-- LEARN: Price-fixed orders have correct surcharge
-- ============================================================================
-- Four orders received a $5.00 shipping surcharge correction. The corrected
-- prices survived both OPTIMIZE compaction and VACUUM cleanup.

ASSERT VALUE price = 44.99 WHERE id = 9
ASSERT VALUE price = 29.99 WHERE id = 17
ASSERT VALUE price = 24.99 WHERE id = 25
ASSERT VALUE price = 20.99 WHERE id = 33
ASSERT ROW_COUNT = 4
SELECT id, order_ref, product, price
FROM {{zone_name}}.delta_demos.vacuum_order_pipeline
WHERE id IN (9, 17, 25, 33)
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 37
ASSERT ROW_COUNT = 37
SELECT * FROM {{zone_name}}.delta_demos.vacuum_order_pipeline;

-- Verify total revenue
ASSERT VALUE total_revenue = 2143.77
SELECT ROUND(SUM(price), 2) AS total_revenue FROM {{zone_name}}.delta_demos.vacuum_order_pipeline;

-- Verify shipped count
ASSERT VALUE shipped_count = 8
SELECT COUNT(*) AS shipped_count FROM {{zone_name}}.delta_demos.vacuum_order_pipeline WHERE status = 'shipped';

-- Verify pending count
ASSERT VALUE pending_count = 29
SELECT COUNT(*) AS pending_count FROM {{zone_name}}.delta_demos.vacuum_order_pipeline WHERE status = 'pending';

-- Verify id=1 is shipped with original price
ASSERT VALUE status = 'shipped'
SELECT status FROM {{zone_name}}.delta_demos.vacuum_order_pipeline WHERE id = 1;

ASSERT VALUE price = 79.99
SELECT price FROM {{zone_name}}.delta_demos.vacuum_order_pipeline WHERE id = 1;

-- Verify id=9 has corrected price
ASSERT VALUE price = 44.99
SELECT price FROM {{zone_name}}.delta_demos.vacuum_order_pipeline WHERE id = 9;

-- Verify cancelled order id=11 is gone
ASSERT VALUE gone_count = 0
SELECT COUNT(*) AS gone_count FROM {{zone_name}}.delta_demos.vacuum_order_pipeline WHERE id = 11;

-- Verify Electronics count
ASSERT VALUE electronics_count = 10
SELECT COUNT(*) AS electronics_count FROM {{zone_name}}.delta_demos.vacuum_order_pipeline WHERE category = 'Electronics';

-- Verify 4 distinct categories
ASSERT VALUE category_count = 4
SELECT COUNT(DISTINCT category) AS category_count FROM {{zone_name}}.delta_demos.vacuum_order_pipeline;
