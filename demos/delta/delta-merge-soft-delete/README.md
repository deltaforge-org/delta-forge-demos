# Delta MERGE — Soft Delete with BY SOURCE

Use WHEN NOT MATCHED BY SOURCE with UPDATE (not DELETE) to soft-delete stale records by marking them inactive, preserving audit trails and historical data.

## Data Story

A procurement team manages a vendor list. When vendors disappear from the quarterly compliance feed, they should not be hard-deleted — high-value vendors get flagged for manual review while low-value vendors are automatically deactivated. All records are preserved for audit.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| vendors | Delta | 14 | Active vendor list with status flags (target) |
| vendor_feed | Delta | 8 | Latest compliance feed (source) |

## Operations Demonstrated

1. **WHEN MATCHED** — Refresh data and mark as verified
2. **WHEN NOT MATCHED** — Insert new vendors as active
3. **BY SOURCE with UPDATE (high value)** — Flag for manual review (`status_note = 'review_needed'`)
4. **BY SOURCE with UPDATE (low value)** — Auto-deactivate (`is_active = 0`)
5. **Multiple BY SOURCE clauses** — Different predicates route to different actions
6. Zero rows deleted — all records preserved for audit

## Verification

- No rows deleted (16 total = 14 original + 2 new)
- 11 active, 5 deactivated
- High-value vendors flagged, not deactivated
- All dates updated to current verification date
