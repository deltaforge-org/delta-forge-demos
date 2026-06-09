# Delta Schema Evolution — Add Columns & NULL Filling

Demonstrates how Delta tables handle schema changes gracefully using
ALTER TABLE ADD COLUMN with automatic NULL filling for existing rows.

## Data Story

A CRM contacts table starts with basic info (name, email). As the business
grows, new fields are added (phone, city, signup date). Existing contacts
get NULL for new columns, new contacts are fully populated, and a backfill
operation fills phone numbers for the first 10 contacts.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `contacts` | Delta Table | 50 | Contacts with evolving schema |

## Schema Evolution

| Phase | Columns | Rows |
|-------|---------|------|
| 1 | id, first_name, last_name, email | 30 |
| 2 | + phone, city, signup_date (ALTER TABLE) | 30 (NULLs) |
| 3 | all 7 columns | 50 (20 new) |
| 4 | backfill phone for ids 1-10 | 50 |

## Verification

8 automated PASS/FAIL checks verify NULL patterns, backfill correctness,
and fully populated new rows.
