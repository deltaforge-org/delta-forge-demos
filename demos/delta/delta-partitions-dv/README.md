# Delta Partitions & Deletion Vectors

Demonstrates partitioned Delta tables with deletion vectors, severity
escalation, and file compaction through OPTIMIZE.

## Data Story

A cloud infrastructure platform monitors events across three regions
(us-east, us-west, eu-west) with 30 events each. Low-value informational
events are purged (triggering deletion vectors), high-latency events get
severity escalation, and 10 new critical incidents flood the us-east region
during an outage. OPTIMIZE compacts the fragmented partition files.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `cloud_events` | Delta Table (Partitioned) | 85 (final) | Cloud monitoring events partitioned by region |

## Schema

**cloud_events:** `id INT, service VARCHAR, region VARCHAR, severity VARCHAR, message VARCHAR, latency_ms INT, event_time VARCHAR`

## Partition Layout

- **us-east:** 35 events (30 - 5 deleted + 10 critical)
- **us-west:** 25 events (30 - 5 deleted)
- **eu-west:** 25 events (30 - 5 deleted)

## Operations

- CREATE DELTA TABLE PARTITIONED BY (region)
- INSERT 90 events across 3 regions
- DELETE 15 low-severity events (triggers deletion vectors)
- UPDATE severity escalation for high-latency events
- INSERT 10 critical events to us-east
- OPTIMIZE — compact files within partitions

## Verification

8 automated PASS/FAIL checks verify: 85 total rows, per-region counts
(35/25/25), deleted events absent, 10 new critical events, severity
escalation applied, and 3 distinct regions present.
