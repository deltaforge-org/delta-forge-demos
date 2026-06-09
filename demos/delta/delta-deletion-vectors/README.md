# Delta Deletion Vectors — Efficient Soft Deletes

Demonstrates Delta deletion vectors (DVs) for efficient soft deletes,
showing how DELETE and UPDATE operations use lightweight bitmaps instead
of rewriting entire data files.

## Data Story

A web analytics platform tracks user sessions across three regions
(us-east, eu-west, ap-south) with 20 sessions each. Bounced sessions
(single page view) are purged first, then stale expired sessions from
January 2024 are cleaned up — both operations create deletion vectors
instead of rewriting Parquet files. Five active sessions that just
finished are upgraded to completed status with extended duration via
UPDATE (which internally marks the old row with a DV and writes a new
row). Finally, OPTIMIZE materializes all DVs by rewriting compacted files.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `web_sessions` | Delta Table | 42 (final) | Web analytics session tracking with DV-enabled deletes |

## Schema

**web_sessions:** `id INT, session_id VARCHAR, user_agent VARCHAR, page_views INT, duration_ms INT, status VARCHAR, region VARCHAR, started_at VARCHAR`

## Operations

- CREATE DELTA TABLE web_sessions
- INSERT 60 sessions across 3 regions (us-east, eu-west, ap-south)
- DELETE 10 bounced sessions (creates deletion vectors)
- DELETE 8 expired sessions before 2024-02-01 (accumulates more DVs)
- UPDATE 5 active sessions to completed with +5000ms duration (DV + new row)
- OPTIMIZE — materializes all DVs into compacted files

## Final State

- **Total rows:** 42 (60 - 10 bounced - 8 old expired)
- **active:** 15 (20 original - 5 upgraded)
- **completed:** 21 (16 original + 5 upgraded)
- **expired:** 6 (14 original - 8 deleted)
- **bounced:** 0 (all 10 deleted)

## Per-Region Counts

- **us-east:** 14 rows (20 - 4 bounced - 2 expired)
- **eu-west:** 14 rows (20 - 3 bounced - 3 expired)
- **ap-south:** 14 rows (20 - 3 bounced - 3 expired)

## Verification

8 automated PASS/FAIL checks verify: 42 total rows, no bounced sessions
remain, no pre-February expired sessions remain, 21 completed sessions,
15 active sessions, us-east region count of 14, updated duration for
session id=1 (17000ms), and 42 distinct session IDs (no duplicates from
DV operations).
