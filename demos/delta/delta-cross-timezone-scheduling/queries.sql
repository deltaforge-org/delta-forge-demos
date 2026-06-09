-- ============================================================================
-- QUERIES: Delta Cross-Timezone Scheduling — Global Conference Planner
-- ============================================================================
-- Cross-timezone scheduling queries demonstrate UTC as the single ordering
-- dimension for global events. Without UTC normalization, a Sydney meeting at
-- "08:00 local" appears later than a New York meeting at "09:00 local" — even
-- though Sydney's meeting actually starts 14 hours earlier in absolute time.
--
-- These queries show conflict detection via UTC overlap, collaboration window
-- discovery, and chronological ordering across 6 timezones.
-- ============================================================================

-- ============================================================================
-- EXPLORE: Today's global schedule ordered by true chronological time
-- ============================================================================
-- Sydney's Board Presentation Dry Run (id=23) starts at UTC 22:00 the
-- previous day — the earliest meeting globally despite being "08:00 local".
-- UTC ordering reveals the true sequence invisible to local wall clocks.
-- ============================================================================
ASSERT VALUE office = 'Sydney' WHERE id = 23
ASSERT ROW_COUNT = 10
SELECT id, meeting_title, office, start_local, start_utc, duration_minutes, room
FROM {{zone_name}}.delta_demos.conference_schedule
ORDER BY start_utc
LIMIT 10;

-- ============================================================================
-- LEARN: How many meetings per office?
-- ============================================================================
-- Dubai has 5 meetings (one was deleted). ORDER BY utc_offset_hours shows
-- the west-to-east progression from New York (UTC-4) to Sydney (UTC+10).
-- ============================================================================
ASSERT VALUE meeting_count = 8 WHERE office = 'New York'
ASSERT VALUE meeting_count = 5 WHERE office = 'Dubai'
ASSERT ROW_COUNT = 6
SELECT office, timezone_label, utc_offset_hours, COUNT(*) AS meeting_count
FROM {{zone_name}}.delta_demos.conference_schedule
GROUP BY office, timezone_label, utc_offset_hours
ORDER BY utc_offset_hours;

-- ============================================================================
-- LEARN: Detect scheduling conflicts (same room, overlapping UTC times)
-- ============================================================================
-- The CORE query — a self-JOIN detecting temporal overlaps via UTC comparison.
-- Two meetings conflict when they share a room and their UTC intervals overlap:
-- a.start_utc < b.end_utc AND b.start_utc < a.end_utc.
-- ============================================================================
ASSERT ROW_COUNT = 2
SELECT a.id AS meeting_a, b.id AS meeting_b, a.office, a.room,
       a.start_utc AS a_start, a.end_utc AS a_end,
       b.start_utc AS b_start, b.end_utc AS b_end
FROM {{zone_name}}.delta_demos.conference_schedule a
JOIN {{zone_name}}.delta_demos.conference_schedule b
  ON a.office = b.office AND a.room = b.room AND a.id < b.id
WHERE a.start_utc < b.end_utc AND b.start_utc < a.end_utc
ORDER BY a.office, a.id;

-- ============================================================================
-- LEARN: Cross-office collaboration windows (UTC hours with 3+ offices active)
-- ============================================================================
-- Finds UTC hours where at least 3 offices have meetings simultaneously,
-- revealing the best windows for cross-office collaboration calls.
-- ============================================================================
ASSERT ROW_COUNT = 6
ASSERT VALUE total_meetings = 4 WHERE utc_hour = '13'
SELECT SUBSTRING(start_utc, 12, 2) AS utc_hour, COUNT(DISTINCT office) AS offices_active, COUNT(*) AS total_meetings
FROM {{zone_name}}.delta_demos.conference_schedule
GROUP BY SUBSTRING(start_utc, 12, 2)
HAVING COUNT(DISTINCT office) >= 3
ORDER BY utc_hour;

-- ============================================================================
-- LEARN: Priority distribution after DML
-- ============================================================================
-- Three meetings were upgraded to high priority (ids 3, 18, 35), four remain
-- low priority, and the rest are normal. Confirms DML mutations applied.
-- ============================================================================
ASSERT VALUE meeting_count = 3 WHERE priority = 'high'
ASSERT VALUE meeting_count = 30 WHERE priority = 'normal'
ASSERT ROW_COUNT = 3
SELECT priority, COUNT(*) AS meeting_count
FROM {{zone_name}}.delta_demos.conference_schedule
GROUP BY priority
ORDER BY priority;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total meetings (40 inserted - 1 deleted = 39)
ASSERT ROW_COUNT = 39
SELECT * FROM {{zone_name}}.delta_demos.conference_schedule;

-- Verify per-office counts
ASSERT VALUE ny_count = 8
SELECT COUNT(*) AS ny_count FROM {{zone_name}}.delta_demos.conference_schedule WHERE office = 'New York';

ASSERT VALUE lon_count = 7
SELECT COUNT(*) AS lon_count FROM {{zone_name}}.delta_demos.conference_schedule WHERE office = 'London';

ASSERT VALUE dubai_count = 5
SELECT COUNT(*) AS dubai_count FROM {{zone_name}}.delta_demos.conference_schedule WHERE office = 'Dubai';

-- Verify conflict count via self-join
ASSERT VALUE conflict_count = 2
SELECT COUNT(*) AS conflict_count
FROM {{zone_name}}.delta_demos.conference_schedule a
JOIN {{zone_name}}.delta_demos.conference_schedule b
  ON a.office = b.office AND a.room = b.room AND a.id < b.id
WHERE a.start_utc < b.end_utc AND b.start_utc < a.end_utc;

-- Verify total duration minutes
ASSERT VALUE total_duration = 2175
SELECT SUM(duration_minutes) AS total_duration FROM {{zone_name}}.delta_demos.conference_schedule;
