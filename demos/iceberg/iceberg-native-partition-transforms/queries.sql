-- ============================================================================
-- Iceberg Native Partition Transforms — Queries
-- ============================================================================
-- Demonstrates native Iceberg format-version 2 table reading with
-- Iceberg-native partition transforms: bucket(8, source_ip) and
-- days(capture_time). These transforms are Iceberg-specific and cannot
-- be produced by Delta Lake. All queries are read-only.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Total Row Count
-- ============================================================================
-- Verifies that Delta Forge discovered all 78 partitioned Parquet data files
-- via the Iceberg v2 manifest chain across bucket and day partitions.

ASSERT ROW_COUNT = 480
SELECT * FROM {{zone_name}}.iceberg.network_traffic;


-- ============================================================================
-- Query 2: Per-Region Breakdown
-- ============================================================================
-- Three regions with exactly 160 packets each (deterministic round-robin).

ASSERT ROW_COUNT = 3
ASSERT VALUE packet_count = 160 WHERE region = 'asia-pacific'
ASSERT VALUE packet_count = 160 WHERE region = 'europe'
ASSERT VALUE packet_count = 160 WHERE region = 'north-america'
SELECT
    region,
    COUNT(*) AS packet_count
FROM {{zone_name}}.iceberg.network_traffic
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 3: Per-Protocol Breakdown
-- ============================================================================
-- Four protocols with exactly 120 packets each (deterministic round-robin).

ASSERT ROW_COUNT = 4
ASSERT VALUE packet_count = 120 WHERE protocol = 'DNS'
ASSERT VALUE packet_count = 120 WHERE protocol = 'ICMP'
ASSERT VALUE packet_count = 120 WHERE protocol = 'TCP'
ASSERT VALUE packet_count = 120 WHERE protocol = 'UDP'
SELECT
    protocol,
    COUNT(*) AS packet_count
FROM {{zone_name}}.iceberg.network_traffic
GROUP BY protocol
ORDER BY protocol;


-- ============================================================================
-- Query 4: Threat Level Distribution
-- ============================================================================
-- Weighted random distribution: ~50% low, ~30% medium, ~15% high, ~5% critical.

ASSERT ROW_COUNT = 4
ASSERT VALUE packet_count = 22 WHERE threat_level = 'critical'
ASSERT VALUE packet_count = 51 WHERE threat_level = 'high'
ASSERT VALUE packet_count = 248 WHERE threat_level = 'low'
ASSERT VALUE packet_count = 159 WHERE threat_level = 'medium'
SELECT
    threat_level,
    COUNT(*) AS packet_count
FROM {{zone_name}}.iceberg.network_traffic
GROUP BY threat_level
ORDER BY threat_level;


-- ============================================================================
-- Query 5: Bytes Transferred Aggregations
-- ============================================================================
-- Total, average, min, and max bytes across all packets.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_bytes = 251040311
ASSERT VALUE avg_bytes = 523000.65
ASSERT VALUE min_bytes = 1837
ASSERT VALUE max_bytes = 1047983
SELECT
    SUM(bytes_transferred) AS total_bytes,
    ROUND(AVG(bytes_transferred), 2) AS avg_bytes,
    MIN(bytes_transferred) AS min_bytes,
    MAX(bytes_transferred) AS max_bytes
FROM {{zone_name}}.iceberg.network_traffic;


-- ============================================================================
-- Query 6: Per-Region Bytes Transferred
-- ============================================================================
-- Bytes aggregation by region exercises numeric computation across partitions.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_bytes = 85974312 WHERE region = 'asia-pacific'
ASSERT VALUE total_bytes = 80023887 WHERE region = 'europe'
ASSERT VALUE total_bytes = 85042112 WHERE region = 'north-america'
SELECT
    region,
    SUM(bytes_transferred) AS total_bytes
FROM {{zone_name}}.iceberg.network_traffic
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 7: Date-Range Query (Days Partition Benefit)
-- ============================================================================
-- Filters to the first 3 days (March 1-3, 2025) in UTC. With days(capture_time)
-- partitioning, Iceberg can prune partitions for non-matching days.
-- Note: The data uses Europe/Oslo timezone (UTC+1). TIMESTAMP literals are
-- compared in UTC, so 2 Oslo-midnight rows fall before UTC midnight on Feb 28,
-- while 1 Oslo-Mar-3 row extends into UTC Mar 4, netting 139 rows.

ASSERT ROW_COUNT = 139
SELECT
    packet_id,
    source_ip,
    protocol,
    threat_level,
    capture_time,
    region
FROM {{zone_name}}.iceberg.network_traffic
WHERE capture_time >= TIMESTAMP '2025-03-01 00:00:00'
  AND capture_time < TIMESTAMP '2025-03-04 00:00:00'
ORDER BY capture_time;


-- ============================================================================
-- Query 8: High Port Traffic (> 8000)
-- ============================================================================
-- Exercises integer predicate filtering on the port column.

ASSERT ROW_COUNT = 122
SELECT
    packet_id,
    source_ip,
    dest_ip,
    protocol,
    port,
    bytes_transferred
FROM {{zone_name}}.iceberg.network_traffic
WHERE port > 8000
ORDER BY port DESC, packet_id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, region/protocol/threat counts,
-- and key aggregations. A single query that validates the full Iceberg
-- partition-transform reader pipeline.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 480
ASSERT VALUE region_count = 3
ASSERT VALUE protocol_count = 4
ASSERT VALUE threat_level_count = 4
ASSERT VALUE total_bytes = 251040311
ASSERT VALUE critical_count = 22
ASSERT VALUE high_port_count = 122
ASSERT VALUE first_3_days_count = 139
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT protocol) AS protocol_count,
    COUNT(DISTINCT threat_level) AS threat_level_count,
    SUM(bytes_transferred) AS total_bytes,
    SUM(CASE WHEN threat_level = 'critical' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN port > 8000 THEN 1 ELSE 0 END) AS high_port_count,
    SUM(CASE WHEN capture_time >= TIMESTAMP '2025-03-01 00:00:00'
              AND capture_time < TIMESTAMP '2025-03-04 00:00:00' THEN 1 ELSE 0 END) AS first_3_days_count
FROM {{zone_name}}.iceberg.network_traffic;
