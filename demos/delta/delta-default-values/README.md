# Delta Column Default Values

Demonstrates column default value patterns using CTEs with COALESCE in
INSERT statements, simulating DEFAULT constraints for Delta tables.

## Data Story

An audit logging system uses column defaults to simplify log entry creation.
When severity is not specified, it defaults to 'info'. When user_name is
missing, it defaults to 'system'. The retry_count defaults to 0, and
is_archived to 0 (false). Notes default to 'N/A' when not provided.

Three batches of inserts demonstrate the pattern: fully explicit values,
fully defaulted values via COALESCE, and a mix of both. An UPDATE then
archives the oldest entries.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `audit_log` | Delta Table | 45 | Audit entries with default value patterns |

## Default Values

| Column | Default | Applied Via |
|--------|---------|-------------|
| user_name | 'system' | COALESCE in CTE |
| severity | 'info' | COALESCE in CTE |
| retry_count | 0 | COALESCE in CTE |
| is_archived | 0 | Explicit in INSERT |
| notes | 'N/A' | COALESCE in CTE |

## Verification

8 automated PASS/FAIL checks verify default value application across all
insert batches, including row counts, default distributions, and archive
status after the UPDATE operation.
