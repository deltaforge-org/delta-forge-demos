-- ============================================================================
-- Delta OPTIMIZE — Cross-Version Data Integrity Proof — Educational Queries
-- ============================================================================
-- WHAT: Proves that OPTIMIZE preserves every row, every value, and every
--       aggregate — the only thing that changes is the physical file layout.
-- WHY:  Compliance teams and DBAs need confidence that maintenance operations
--       do not alter data. A version-comparison proof satisfies this.
-- HOW:  Snapshot aggregates before OPTIMIZE, run OPTIMIZE, snapshot the same
--       aggregates after, then compare pre/post versions via time travel.
-- ============================================================================


-- ============================================================================
-- SNAPSHOT: Pre-OPTIMIZE Carrier Breakdown
-- ============================================================================
-- Before compaction, capture the exact carrier breakdown. After OPTIMIZE,
-- we will run the identical query and the results must match perfectly.

ASSERT ROW_COUNT = 1
ASSERT VALUE shipments = 25
ASSERT VALUE carriers = 3
ASSERT VALUE statuses = 2
SELECT COUNT(*) AS shipments,
       COUNT(DISTINCT carrier) AS carriers,
       COUNT(DISTINCT status) AS statuses
FROM {{zone_name}}.delta_demos.shipments;


-- ============================================================================
-- SNAPSHOT: Carrier Weight Totals (Pre-OPTIMIZE)
-- ============================================================================
-- This is the reference fingerprint. If any row were lost or altered during
-- OPTIMIZE, these sums would drift.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_weight = 151.2 WHERE carrier = 'FastFreight'
ASSERT VALUE total_weight = 102.3 WHERE carrier = 'CargoLine'
ASSERT VALUE total_weight = 80.7 WHERE carrier = 'SwiftShip'
SELECT carrier,
       COUNT(*) AS shipments,
       ROUND(SUM(weight_kg), 2) AS total_weight
FROM {{zone_name}}.delta_demos.shipments
GROUP BY carrier
ORDER BY total_weight DESC;


-- ============================================================================
-- ACTION: Run OPTIMIZE to Compact Data Files
-- ============================================================================
-- The table has 4+ small Parquet files from the DML history. OPTIMIZE merges
-- them into fewer, larger files. This creates a new version (V5) in the Delta
-- log. The data content is unchanged — only the physical layout improves.

OPTIMIZE {{zone_name}}.delta_demos.shipments;


-- ============================================================================
-- PROOF: Post-OPTIMIZE Carrier Breakdown — Must Match Pre-OPTIMIZE Exactly
-- ============================================================================
-- The identical query on the post-OPTIMIZE table. Every value must match
-- the pre-OPTIMIZE snapshot above. Any difference means data loss or
-- corruption — which should never happen with OPTIMIZE.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_weight = 151.2 WHERE carrier = 'FastFreight'
ASSERT VALUE total_weight = 102.3 WHERE carrier = 'CargoLine'
ASSERT VALUE total_weight = 80.7 WHERE carrier = 'SwiftShip'
SELECT carrier,
       COUNT(*) AS shipments,
       ROUND(SUM(weight_kg), 2) AS total_weight
FROM {{zone_name}}.delta_demos.shipments
GROUP BY carrier
ORDER BY total_weight DESC;


-- ============================================================================
-- PROOF: Version Row Counts — Full History Preserved
-- ============================================================================
-- OPTIMIZE created V5, but all prior versions remain accessible via time
-- travel. Row counts at each version match the expected DML history:
--   V1: 20 (initial load), V2: 20 (update, same count),
--   V3: 17 (3 deleted), V4: 25 (8 added), V5: 25 (optimize, same data)

ASSERT ROW_COUNT = 5
ASSERT VALUE row_count = 20 WHERE version = 'V1 (initial load)'
ASSERT VALUE row_count = 20 WHERE version = 'V2 (recalibrated)'
ASSERT VALUE row_count = 17 WHERE version = 'V3 (cancelled removed)'
ASSERT VALUE row_count = 25 WHERE version = 'V4 (new batch)'
ASSERT VALUE row_count = 25 WHERE version = 'V5 (optimized)'
SELECT 'V1 (initial load)' AS version, COUNT(*) AS row_count
FROM {{zone_name}}.delta_demos.shipments VERSION AS OF 1
UNION ALL
SELECT 'V2 (recalibrated)', COUNT(*)
FROM {{zone_name}}.delta_demos.shipments VERSION AS OF 2
UNION ALL
SELECT 'V3 (cancelled removed)', COUNT(*)
FROM {{zone_name}}.delta_demos.shipments VERSION AS OF 3
UNION ALL
SELECT 'V4 (new batch)', COUNT(*)
FROM {{zone_name}}.delta_demos.shipments VERSION AS OF 4
UNION ALL
SELECT 'V5 (optimized)', COUNT(*)
FROM {{zone_name}}.delta_demos.shipments;


-- ============================================================================
-- PROOF: Cross-Version Weight Sum — V4 vs V5 Identical
-- ============================================================================
-- The total weight across all shipments must be identical at V4 (pre-OPTIMIZE)
-- and V5 (post-OPTIMIZE). This is the strongest integrity proof: OPTIMIZE
-- is purely a physical reorganization with zero logical impact.

ASSERT ROW_COUNT = 2
ASSERT VALUE total_weight = 334.2 WHERE version = 'V4 (pre-optimize)'
ASSERT VALUE total_weight = 334.2 WHERE version = 'V5 (post-optimize)'
SELECT 'V4 (pre-optimize)' AS version, ROUND(SUM(weight_kg), 2) AS total_weight
FROM {{zone_name}}.delta_demos.shipments VERSION AS OF 4
UNION ALL
SELECT 'V5 (post-optimize)', ROUND(SUM(weight_kg), 2)
FROM {{zone_name}}.delta_demos.shipments;


-- ============================================================================
-- EXPLORE: Recalibrated Shipments — Weight Corrections Survived
-- ============================================================================
-- Five shipments had +0.5 kg weight corrections in V2. Verify the corrected
-- values are intact after OPTIMIZE. If OPTIMIZE corrupted data, these
-- precision values would be the first to show drift.

ASSERT ROW_COUNT = 5
ASSERT VALUE weight_kg = 22.5 WHERE id = 3
ASSERT VALUE weight_kg = 3.9 WHERE id = 8
ASSERT VALUE weight_kg = 15.3 WHERE id = 12
ASSERT VALUE weight_kg = 9.6 WHERE id = 16
ASSERT VALUE weight_kg = 5.1 WHERE id = 19
SELECT id, tracking_code, carrier, weight_kg
FROM {{zone_name}}.delta_demos.shipments
WHERE id IN (3, 8, 12, 16, 19)
ORDER BY id;


-- ============================================================================
-- EXPLORE: New Batch Shipments — V4 Additions Intact
-- ============================================================================
-- The 8 shipments added in V4 must still be present and unaltered.

ASSERT ROW_COUNT = 8
SELECT id, tracking_code, carrier, origin, destination, weight_kg, status
FROM {{zone_name}}.delta_demos.shipments
WHERE id BETWEEN 21 AND 28
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total shipments: 25
ASSERT VALUE total = 25
SELECT COUNT(*) AS total FROM {{zone_name}}.delta_demos.shipments;

-- Verify 3 carriers
ASSERT VALUE carrier_count = 3
SELECT COUNT(DISTINCT carrier) AS carrier_count FROM {{zone_name}}.delta_demos.shipments;

-- Verify cancelled shipments were removed (ids 6, 14, 20)
ASSERT VALUE cancelled_count = 0
SELECT COUNT(*) FILTER (WHERE id IN (6, 14, 20)) AS cancelled_count FROM {{zone_name}}.delta_demos.shipments;

-- Verify only delivered and in_transit remain
ASSERT VALUE status_count = 2
SELECT COUNT(DISTINCT status) AS status_count FROM {{zone_name}}.delta_demos.shipments;

-- Verify total weight matches pre-OPTIMIZE
ASSERT VALUE total_weight = 334.2
SELECT ROUND(SUM(weight_kg), 2) AS total_weight FROM {{zone_name}}.delta_demos.shipments;

-- Verify recalibrated weight (id=3: 22.0 + 0.5 = 22.5)
ASSERT VALUE weight_kg = 22.5
SELECT weight_kg FROM {{zone_name}}.delta_demos.shipments WHERE id = 3;
