# Delta VACUUM — Cleanup Orphaned Files

Demonstrates VACUUM operations for cleaning up orphaned data files
created by multiple DML operations.

## Data Story

An HR department manages 50 employees. Over time, salaries are adjusted,
employees transfer departments, some are terminated, and new hires join.
Each DML operation creates new data files while leaving old versions behind.
VACUUM cleans up these orphaned files to reclaim storage without affecting
current data.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `hr_employees` | Delta Table | 53 (final) | HR workforce with DML history |

## Schema

**hr_employees:** `id INT, name VARCHAR, department VARCHAR, salary DOUBLE, status VARCHAR, hire_date VARCHAR`

## Operations Demonstrated

1. **INSERT** — 50 initial employees
2. **UPDATE** — salary increases for Engineering (+15%)
3. **UPDATE** — 3 department transfers
4. **DELETE** — 2 terminated employees removed
5. **INSERT** — 5 new hires added
6. **VACUUM** — cleanup orphaned data files

## Verification

8 automated PASS/FAIL checks verify post-vacuum data integrity: row count,
salary updates, department transfers, terminated employee removal, active
status, new hires, department distribution, and unchanged salaries.
