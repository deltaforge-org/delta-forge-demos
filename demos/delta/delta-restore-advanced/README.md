# Delta RESTORE — Advanced Recovery Scenarios

Demonstrates advanced RESTORE operations for recovering a Delta table
from complex multi-step changes including deployments and bulk deletes.

## Data Story

A configuration management system stores 35 application settings across
five categories. After database tuning (V1), a config deployment adds new
settings and deactivates old ones (V2). A dangerous cleanup script then
deletes all inactive entries (V3), removing 8 rows. The team uses RESTORE
TO VERSION to roll back to the known-good post-tuning state, undoing both
the deployment and the cleanup. A follow-up update marks key settings as
reviewed by the restore administrator.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `config_settings` | Delta Table | 35 (final) | Application config with advanced restore |

## Schema

**config_settings:** `id INT, key VARCHAR, value VARCHAR, category VARCHAR, is_active INT, updated_by VARCHAR, updated_at VARCHAR`

## Version History

- **V0:** INSERT 35 config settings across 5 categories (5 inactive legacy entries)
- **V1:** UPDATE 10 database settings (connection pool, timeouts)
- **V2:** INSERT 10 new settings + UPDATE 5 existing (config deployment)
- **V3:** DELETE all is_active = 0 (dangerous cleanup removes 8 rows)
- **V4:** RESTORE TO VERSION 1 (undo deployment + cleanup)
- **V5:** UPDATE 3 settings marked by restored_admin

## Verification

8 automated PASS/FAIL checks verify complete recovery: 35 rows restored,
V2 inserts removed, V1 database tuning preserved, deleted rows recovered,
restored admin markings applied, all 5 categories present, all original
IDs intact, and auth settings unchanged.
