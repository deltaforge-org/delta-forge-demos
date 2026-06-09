# Delta MERGE — Deduplication (Keep Latest)

Use MERGE with a ROW_NUMBER subquery source to deduplicate a table, keeping only the latest version of each business key.

## Data Story

An event pipeline receives user actions with at-least-once delivery guarantees. Events arrive with duplicate event_ids from retries, replays, and progressive status updates. The MERGE collapses duplicates into a clean table, keeping only the highest-version row per event_id.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| events | Delta | 20 | Raw events with duplicates (source of truth) |
| events_deduped | Delta | 0→12 | Clean deduplicated events (target) |

## Operations Demonstrated

1. **Subquery source** — ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY version DESC)
2. **Self-merge pattern** — Source is a deduplicated view of the raw table
3. **WHEN MATCHED** — Update existing rows with latest version
4. **WHEN NOT MATCHED** — Insert first-seen events
5. **Idempotency** — Re-running the MERGE produces identical results (demonstrated with two runs)

## Verification

- Exactly 12 unique events in deduplicated table
- No event_id appears more than once
- Latest version kept for each event (version 3 for E001, E007)
- Corrected values preserved (E003 refund amount, E008 price)
- Original raw table untouched (still 20 rows)
