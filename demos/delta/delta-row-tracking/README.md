# Delta Row Tracking — Stable Row IDs & Audit Trail

Demonstrates the pattern of row-level tracking using explicit audit columns
that simulate what Delta's row tracking feature provides at the protocol
level. Version tags increment when rows are modified, providing stable
row-level identity for audit and lineage use cases.

## Data Story

A financial compliance system tracks every change to regulated entities.
Each row in the audit table represents an auditable action — create, update,
delete, or review — with before/after values. The version_tag column
simulates row-level version tracking, incrementing when a row is modified
or re-reviewed. This pattern mirrors Delta's row tracking feature for
audit and lineage scenarios.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `compliance_audit` | Delta Table | 50 (final) | Financial compliance audit trail with row-level versioning |

## Schema

**compliance_audit:** `id INT, record_id VARCHAR, entity_type VARCHAR, entity_name VARCHAR, action VARCHAR, old_value VARCHAR, new_value VARCHAR, actor VARCHAR, version_tag INT, audit_timestamp VARCHAR`

## Setup Steps

- **STEP 3:** INSERT 30 initial create entries (version_tag=1)
- **STEP 5:** INSERT 10 update entries with old/new values (version_tag=2)
- **STEP 6:** INSERT 5 review entries (version_tag=1)
- **STEP 7:** INSERT 5 delete entries (version_tag=1)
- **STEP 8:** UPDATE version_tag to 2 for 8 re-reviewed entries

## Verification

8 automated PASS/FAIL checks verify: 50 total rows, 30 creates, 10 updates,
5 reviews, 5 deletes, 30 unique record IDs, 18 entries with version_tag >= 2,
and 4 distinct entity types (account, transaction, customer, policy).
