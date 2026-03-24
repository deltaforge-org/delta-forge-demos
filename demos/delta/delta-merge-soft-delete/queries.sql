-- ============================================================================
-- Delta MERGE — Soft Delete with BY SOURCE — Educational Queries
-- ============================================================================
-- WHAT: MERGE with WHEN NOT MATCHED BY SOURCE using UPDATE (not DELETE)
--       to soft-delete stale records and preserve them for audit.
-- WHY:  Production pipelines need audit trails. Hard-deleting records
--       when they disappear from a feed loses history. Soft-delete
--       marks them inactive with a timestamp, keeping the data queryable.
-- HOW:  Two NOT MATCHED BY SOURCE clauses with different predicates:
--       high-value vendors get flagged for manual review, while
--       low-value vendors are automatically deactivated.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Vendors Before MERGE
-- ============================================================================
-- 14 active vendors. The compliance feed only contains 6 existing vendors
-- (ids 1-6) plus 2 new ones. Vendors 7-14 are missing from the feed.

ASSERT ROW_COUNT = 14
SELECT id, vendor_name, category, annual_spend, is_active, status_note
FROM {{zone_name}}.delta_demos.vendors
ORDER BY id;

ASSERT ROW_COUNT = 8
SELECT id, vendor_name, category, annual_spend
FROM {{zone_name}}.delta_demos.vendor_feed
ORDER BY id;


-- ============================================================================
-- MERGE: Soft Delete with Tiered BY SOURCE Rules
-- ============================================================================
-- Three actions:
--   WHEN MATCHED → Refresh data and mark as verified
--   WHEN NOT MATCHED → Insert new vendors as active
--   WHEN NOT MATCHED BY SOURCE (high value) → Flag for review
--   WHEN NOT MATCHED BY SOURCE (low value) → Deactivate
--
-- Vendors 7-14 are NOT in the feed:
--   High value (>= 50000): ids 8(60K), 10(75K), 11(150K) → review
--   Low value (< 50000):   ids 7(18K), 9(22K), 12(12K), 13(8K), 14(35K) → deactivated
--
-- rows_affected: 6 updates + 2 inserts + 3 flagged + 5 deactivated = 16

ASSERT ROW_COUNT = 16
MERGE INTO {{zone_name}}.delta_demos.vendors AS target
USING {{zone_name}}.delta_demos.vendor_feed AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        annual_spend  = source.annual_spend,
        status_note   = 'verified',
        last_verified = '2025-02-15'
WHEN NOT MATCHED THEN
    INSERT (id, vendor_name, category, annual_spend, is_active, status_note, last_verified)
    VALUES (source.id, source.vendor_name, source.category, source.annual_spend,
            1, 'new_vendor', '2025-02-15')
WHEN NOT MATCHED BY SOURCE AND target.annual_spend >= 50000 THEN
    UPDATE SET
        status_note   = 'review_needed',
        last_verified = '2025-02-15'
WHEN NOT MATCHED BY SOURCE AND target.annual_spend < 50000 THEN
    UPDATE SET
        is_active     = 0,
        status_note   = 'deactivated',
        last_verified = '2025-02-15';


-- ============================================================================
-- EXPLORE: All Vendors After MERGE
-- ============================================================================
-- All 14 original + 2 new = 16 vendors. No rows were deleted — every
-- vendor is still in the table, but with updated status flags.

ASSERT ROW_COUNT = 16
SELECT id, vendor_name, annual_spend, is_active, status_note, last_verified
FROM {{zone_name}}.delta_demos.vendors
ORDER BY id;


-- ============================================================================
-- LEARN: Verified Vendors (WHEN MATCHED)
-- ============================================================================
-- The 6 matched vendors were refreshed with new spend figures and
-- marked as verified with the current date:

ASSERT ROW_COUNT = 6
ASSERT VALUE status_note = 'verified' WHERE id = 1
ASSERT VALUE annual_spend = 125000.0 WHERE id = 1
ASSERT VALUE annual_spend = 210000.0 WHERE id = 6
SELECT id, vendor_name, annual_spend, is_active, status_note, last_verified
FROM {{zone_name}}.delta_demos.vendors
WHERE id BETWEEN 1 AND 6
ORDER BY id;


-- ============================================================================
-- LEARN: High-Value Flagged for Review (BY SOURCE >= 50000)
-- ============================================================================
-- Vendors 8, 10, 11 have annual_spend >= 50000 but were missing from
-- the feed. Instead of deactivating them, the MERGE flagged them
-- for manual review. They remain active (is_active = 1):

ASSERT ROW_COUNT = 3
ASSERT VALUE status_note = 'review_needed' WHERE id = 8
ASSERT VALUE is_active = 1 WHERE id = 8
ASSERT VALUE is_active = 1 WHERE id = 10
ASSERT VALUE is_active = 1 WHERE id = 11
SELECT id, vendor_name, annual_spend, is_active, status_note
FROM {{zone_name}}.delta_demos.vendors
WHERE id IN (8, 10, 11)
ORDER BY annual_spend DESC;


-- ============================================================================
-- LEARN: Low-Value Deactivated (BY SOURCE < 50000)
-- ============================================================================
-- Vendors 7, 9, 12, 13, 14 have annual_spend < 50000 and were missing
-- from the feed. They were automatically deactivated (is_active = 0):

ASSERT ROW_COUNT = 5
ASSERT VALUE is_active = 0 WHERE id = 7
ASSERT VALUE is_active = 0 WHERE id = 13
ASSERT VALUE status_note = 'deactivated' WHERE id = 12
SELECT id, vendor_name, annual_spend, is_active, status_note
FROM {{zone_name}}.delta_demos.vendors
WHERE id IN (7, 9, 12, 13, 14)
ORDER BY annual_spend DESC;


-- ============================================================================
-- EXPLORE: New Vendors Inserted
-- ============================================================================
-- Two new vendors joined via WHEN NOT MATCHED:

ASSERT ROW_COUNT = 2
ASSERT VALUE status_note = 'new_vendor' WHERE id = 15
ASSERT VALUE is_active = 1 WHERE id = 16
SELECT id, vendor_name, category, annual_spend, is_active, status_note
FROM {{zone_name}}.delta_demos.vendors
WHERE id IN (15, 16)
ORDER BY id;


-- ============================================================================
-- EXPLORE: Active vs Inactive Summary
-- ============================================================================
-- The soft-delete pattern preserves all records. Active vendors can be
-- queried with WHERE is_active = 1; deactivated ones are still available
-- for historical analysis:

ASSERT ROW_COUNT = 2
ASSERT VALUE vendor_count = 11 WHERE is_active = 1
ASSERT VALUE vendor_count = 5 WHERE is_active = 0
SELECT is_active,
       COUNT(*) AS vendor_count,
       ROUND(SUM(annual_spend), 2) AS total_spend
FROM {{zone_name}}.delta_demos.vendors
GROUP BY is_active
ORDER BY is_active DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify no_rows_deleted: all 14 original + 2 new = 16
ASSERT ROW_COUNT = 16
SELECT * FROM {{zone_name}}.delta_demos.vendors;

-- Verify active_count: 11 active vendors (6 verified + 3 review + 2 new)
ASSERT VALUE cnt = 11
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.vendors WHERE is_active = 1;

-- Verify deactivated_count: 5 deactivated vendors
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.vendors WHERE is_active = 0;

-- Verify review_count: 3 vendors flagged for review
ASSERT VALUE cnt = 3
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.vendors WHERE status_note = 'review_needed';

-- Verify verified_count: 6 verified vendors
ASSERT VALUE cnt = 6
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.vendors WHERE status_note = 'verified';

-- Verify lambda_active: Lambda Cloud (150K) still active
ASSERT VALUE is_active = 1 WHERE id = 11
SELECT id, is_active FROM {{zone_name}}.delta_demos.vendors WHERE id = 11;

-- Verify lambda_status: Lambda Cloud flagged for review
ASSERT VALUE status_note = 'review_needed' WHERE id = 11
SELECT id, status_note FROM {{zone_name}}.delta_demos.vendors WHERE id = 11;

-- Verify eta_inactive: Eta Packaging (18K) deactivated
ASSERT VALUE is_active = 0 WHERE id = 7
SELECT id, is_active FROM {{zone_name}}.delta_demos.vendors WHERE id = 7;

-- Verify eta_status: Eta Packaging marked as deactivated
ASSERT VALUE status_note = 'deactivated' WHERE id = 7
SELECT id, status_note FROM {{zone_name}}.delta_demos.vendors WHERE id = 7;

-- Verify all_dates_updated: every vendor has last_verified = 2025-02-15
ASSERT VALUE cnt = 16
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.vendors WHERE last_verified = '2025-02-15';
