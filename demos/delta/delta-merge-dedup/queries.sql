-- ============================================================================
-- Delta MERGE — Deduplication (Keep Latest) — Educational Queries
-- ============================================================================
-- WHAT: MERGE a table against a deduplicated subquery of itself to collapse
--       duplicate event_ids into a single row per business key.
-- WHY:  At-least-once delivery, pipeline retries, and overlapping batch
--       windows produce duplicates. Dedup via MERGE is atomic and
--       idempotent — safe to re-run without side effects.
-- HOW:  The source is a subquery using ROW_NUMBER() OVER (PARTITION BY
--       event_id ORDER BY version DESC) to select the latest version
--       of each event. The MERGE matches on event_id and either updates
--       existing rows or inserts new ones.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Raw Events with Duplicates
-- ============================================================================
-- 20 rows with 12 unique event_ids. Some events have up to 3 versions.

ASSERT ROW_COUNT = 20
SELECT event_id, user_id, action, amount, version, received_at
FROM {{zone_name}}.delta_demos.events
ORDER BY event_id, version;


-- ============================================================================
-- EXPLORE: Duplicate Distribution
-- ============================================================================
-- How many copies does each event_id have?

ASSERT ROW_COUNT = 12
ASSERT VALUE copy_count = 3 WHERE event_id = 'E001'
ASSERT VALUE copy_count = 1 WHERE event_id = 'E004'
SELECT event_id,
       COUNT(*) AS copy_count,
       MAX(version) AS latest_version
FROM {{zone_name}}.delta_demos.events
GROUP BY event_id
ORDER BY event_id;


-- ============================================================================
-- MERGE: Deduplicate into Clean Table
-- ============================================================================
-- The source subquery selects the latest version of each event_id
-- using ROW_NUMBER(). The MERGE inserts all 12 unique events into
-- the empty events_deduped table.
--
-- On the first run, all rows go through WHEN NOT MATCHED (INSERT)
-- since events_deduped is empty. On subsequent runs, matching rows
-- would be updated — making this pattern idempotent.

ASSERT ROW_COUNT = 12
MERGE INTO {{zone_name}}.delta_demos.events_deduped AS target
USING (
    SELECT event_id, user_id, action, amount, version, received_at
    FROM (
        SELECT event_id, user_id, action, amount, version, received_at,
               ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY version DESC) AS rn
        FROM {{zone_name}}.delta_demos.events
    )
    WHERE rn = 1
) AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN
    UPDATE SET
        user_id     = source.user_id,
        action      = source.action,
        amount      = source.amount,
        version     = source.version,
        received_at = source.received_at
WHEN NOT MATCHED THEN
    INSERT (event_id, user_id, action, amount, version, received_at)
    VALUES (source.event_id, source.user_id, source.action,
            source.amount, source.version, source.received_at);


-- ============================================================================
-- EXPLORE: Deduplicated Events
-- ============================================================================
-- Exactly 12 rows — one per event_id, each showing the latest version.

ASSERT ROW_COUNT = 12
SELECT event_id, user_id, action, amount, version, received_at
FROM {{zone_name}}.delta_demos.events_deduped
ORDER BY event_id;


-- ============================================================================
-- LEARN: Latest Version Kept
-- ============================================================================
-- For events with multiple versions, only the highest version survived:
--   E001: version 3 (confirm, not click or purchase)
--   E007: version 3 (delivered, not order or shipped)
--   E003: version 2 (corrected refund amount -30, not -25)

ASSERT VALUE version = 3 WHERE event_id = 'E001'
ASSERT VALUE action = 'confirm' WHERE event_id = 'E001'
ASSERT VALUE version = 3 WHERE event_id = 'E007'
ASSERT VALUE action = 'delivered' WHERE event_id = 'E007'
ASSERT VALUE amount = -30.0 WHERE event_id = 'E003'
SELECT event_id, action, amount, version
FROM {{zone_name}}.delta_demos.events_deduped
WHERE event_id IN ('E001', 'E003', 'E007')
ORDER BY event_id;


-- ============================================================================
-- LEARN: Single Events Unchanged
-- ============================================================================
-- Events with only one version passed through unchanged:

ASSERT ROW_COUNT = 4
ASSERT VALUE action = 'purchase' WHERE event_id = 'E004'
ASSERT VALUE action = 'signup' WHERE event_id = 'E009'
SELECT event_id, user_id, action, amount, version
FROM {{zone_name}}.delta_demos.events_deduped
WHERE event_id IN ('E004', 'E006', 'E009', 'E010')
ORDER BY event_id;


-- ============================================================================
-- MERGE: Re-Run to Prove Idempotency
-- ============================================================================
-- Running the same MERGE again should update all 12 rows with identical
-- values. The result is the same — proving the pattern is safe to retry.

ASSERT ROW_COUNT = 12
MERGE INTO {{zone_name}}.delta_demos.events_deduped AS target
USING (
    SELECT event_id, user_id, action, amount, version, received_at
    FROM (
        SELECT event_id, user_id, action, amount, version, received_at,
               ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY version DESC) AS rn
        FROM {{zone_name}}.delta_demos.events
    )
    WHERE rn = 1
) AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN
    UPDATE SET
        user_id     = source.user_id,
        action      = source.action,
        amount      = source.amount,
        version     = source.version,
        received_at = source.received_at
WHEN NOT MATCHED THEN
    INSERT (event_id, user_id, action, amount, version, received_at)
    VALUES (source.event_id, source.user_id, source.action,
            source.amount, source.version, source.received_at);


-- ============================================================================
-- EXPLORE: Still 12 Rows After Re-Run
-- ============================================================================
-- The idempotent MERGE did not create duplicates — still exactly 12 rows:

ASSERT ROW_COUNT = 12
SELECT event_id, action, amount, version
FROM {{zone_name}}.delta_demos.events_deduped
ORDER BY event_id;


-- ============================================================================
-- EXPLORE: Action Summary After Dedup
-- ============================================================================
-- Distribution of event types in the clean dataset:

ASSERT ROW_COUNT = 6
SELECT action,
       COUNT(*) AS event_count,
       ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.delta_demos.events_deduped
GROUP BY action
ORDER BY event_count DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify deduped_count: exactly 12 unique events
ASSERT ROW_COUNT = 12
SELECT * FROM {{zone_name}}.delta_demos.events_deduped;

-- Verify no_duplicates: each event_id appears exactly once
ASSERT VALUE max_copies = 1
SELECT MAX(cnt) AS max_copies FROM (SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.events_deduped GROUP BY event_id);

-- Verify e001_latest: E001 kept version 3 (confirm)
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.events_deduped WHERE event_id = 'E001' AND version = 3 AND action = 'confirm';

-- Verify e003_corrected: E003 kept corrected refund (-30)
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.events_deduped WHERE event_id = 'E003' AND amount = -30.0;

-- Verify e007_delivered: E007 kept final status (delivered)
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.events_deduped WHERE event_id = 'E007' AND action = 'delivered';

-- Verify e008_price_update: E008 kept updated price (54.99)
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.events_deduped WHERE event_id = 'E008' AND amount = 54.99;

-- Verify original_intact: raw events table still has 20 rows
ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.delta_demos.events;
