# Delta In-Commit Timestamps — Reliable Version Timing

Demonstrates in-commit timestamps for reliable version timing in Delta tables
using a deployment tracking system.

## Data Story

A deployment tracking system uses in-commit timestamps to ensure each table
version has a reliable, monotonically increasing timestamp. This is critical
for TIMESTAMP AS OF time travel queries, ensuring accurate point-in-time
recovery regardless of clock skew between writers. Deployments span production,
staging, and development environments with rollbacks and failures.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `deployment_log` | Delta Table | 40 | Deployment records with in-commit timestamps enabled |

## Schema

**deployment_log:** `id INT, service VARCHAR, environment VARCHAR, version_str VARCHAR, deployer VARCHAR, status VARCHAR, duration_seconds INT, deploy_timestamp VARCHAR, commit_hash VARCHAR`

## Table Properties

- `delta.enableInCommitTimestamps` = `true` — ensures monotonically increasing timestamps per version

## Deployment Distribution

- **Production (ids 1-20):** 20 deployments across 5 services (api-gateway, user-service, payment-engine, notification-hub, search-indexer)
- **Staging (ids 21-30):** 10 release candidate deployments
- **Development (ids 31-40):** 10 dev branch deployments

## Operations

1. INSERT 20 rows — production deployments across 5 services
2. INSERT 10 rows — staging deployments (release candidates)
3. INSERT 10 rows — development deployments (dev branches)
4. UPDATE — 5 deployments rolled back (status='rollback')
5. UPDATE — 3 deployments failed (status='failed')

## Verification

8 automated PASS/FAIL checks verify total row count (40), production count (20),
staging count (10), development count (10), rollback count (5), failed count (3),
success count (32), and distinct services (5).
