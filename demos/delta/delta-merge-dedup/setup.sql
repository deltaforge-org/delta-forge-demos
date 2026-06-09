-- ============================================================================
-- Delta MERGE — Deduplication (Keep Latest) — Setup Script
-- ============================================================================
-- Creates tables for the deduplication MERGE demo.
--
-- Tables:
--   1. events         — 20 raw events with duplicates (source of truth)
--   2. events_deduped — starts empty, will hold the deduplicated result
--
-- The events table has duplicate event_ids from at-least-once delivery:
--   - event_id E001: 3 copies (versions 1, 2, 3)
--   - event_id E002: 2 copies (versions 1, 2)
--   - event_id E003: 2 copies (versions 1, 2)
--   - event_id E004: 1 copy
--   - event_id E005: 2 copies (versions 1, 2)
--   - event_id E006: 1 copy
--   - event_id E007: 3 copies (versions 1, 2, 3)
--   - event_id E008: 2 copies (versions 1, 2)
--   - event_id E009: 1 copy
--   - event_id E010: 1 copy
--   - event_id E011: 1 copy
--   - event_id E012: 1 copy
--   Total: 20 rows, 12 unique event_ids
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: events — 20 raw events with duplicates
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.events (
    event_id    VARCHAR,
    user_id     INT,
    action      VARCHAR,
    amount      DOUBLE,
    version     INT,
    received_at VARCHAR
) LOCATION 'delta-merge-dedup/events';


INSERT INTO {{zone_name}}.delta_demos.events VALUES
    -- E001: 3 copies (user clicked, then purchased, then confirmed)
    ('E001', 101, 'click',     0.00,   1, '2025-03-01 08:00:00'),
    ('E001', 101, 'purchase', 49.99,   2, '2025-03-01 08:05:00'),
    ('E001', 101, 'confirm',  49.99,   3, '2025-03-01 08:06:00'),
    -- E002: 2 copies (duplicate delivery)
    ('E002', 102, 'signup',    0.00,   1, '2025-03-01 09:00:00'),
    ('E002', 102, 'signup',    0.00,   2, '2025-03-01 09:00:01'),
    -- E003: 2 copies (amount corrected)
    ('E003', 103, 'refund',  -25.00,   1, '2025-03-01 10:00:00'),
    ('E003', 103, 'refund',  -30.00,   2, '2025-03-01 10:01:00'),
    -- E004: 1 copy (no duplicate)
    ('E004', 104, 'purchase', 79.99,   1, '2025-03-01 11:00:00'),
    -- E005: 2 copies (retry after timeout)
    ('E005', 105, 'purchase', 19.99,   1, '2025-03-01 12:00:00'),
    ('E005', 105, 'purchase', 19.99,   2, '2025-03-01 12:00:05'),
    -- E006: 1 copy
    ('E006', 106, 'click',     0.00,   1, '2025-03-01 13:00:00'),
    -- E007: 3 copies (progressive updates)
    ('E007', 107, 'order',    99.99,   1, '2025-03-01 14:00:00'),
    ('E007', 107, 'shipped',  99.99,   2, '2025-03-01 14:30:00'),
    ('E007', 107, 'delivered', 99.99,  3, '2025-03-01 15:00:00'),
    -- E008: 2 copies (price update)
    ('E008', 108, 'purchase', 59.99,   1, '2025-03-01 16:00:00'),
    ('E008', 108, 'purchase', 54.99,   2, '2025-03-01 16:01:00'),
    -- E009-E012: single events (no duplicates)
    ('E009', 109, 'signup',    0.00,   1, '2025-03-01 17:00:00'),
    ('E010', 110, 'purchase', 29.99,   1, '2025-03-01 18:00:00'),
    ('E011', 111, 'click',     0.00,   1, '2025-03-01 19:00:00'),
    ('E012', 112, 'refund',  -15.00,   1, '2025-03-01 20:00:00');


-- ============================================================================
-- TABLE 2: events_deduped — empty target for deduplicated events
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.events_deduped (
    event_id    VARCHAR,
    user_id     INT,
    action      VARCHAR,
    amount      DOUBLE,
    version     INT,
    received_at VARCHAR
) LOCATION 'delta-merge-dedup/events_deduped';

