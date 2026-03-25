-- ============================================================================
-- Delta Duration Arithmetic — Maritime Canal Transit — Educational Queries
-- ============================================================================
-- WHAT: Duration arithmetic computes time intervals from VARCHAR timestamps
--       without relying on native TIMESTAMP or INTERVAL types.
-- WHY:  Many real-world systems store timestamps as strings (VARCHAR NTZ).
--       Computing durations requires extracting hour/minute components via
--       SUBSTRING, casting them to integers, and performing arithmetic.
-- HOW:  SUBSTRING extracts the HH and MM portions from the timestamp string,
--       CAST converts them to INT, then simple math yields total minutes.
--       Cross-midnight transits are detected by comparing date substrings.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Current transit overview
-- ============================================================================
-- A quick look at the vessel transit data. Each row records a vessel passing
-- through one of three canal locks with entry and exit timestamps as VARCHAR.
-- Notice the status column — most are 'completed', some are 'in_transit' or
-- 'delayed' after the UPDATE statements in setup.sql.
ASSERT VALUE status = 'completed' WHERE id = 1
ASSERT ROW_COUNT = 10
SELECT id, vessel_name, vessel_type, lock_name, entry_time, exit_time, status
FROM {{zone_name}}.delta_demos.vessel_transit
ORDER BY id
LIMIT 10;


-- ============================================================================
-- LEARN: Compute transit duration in minutes using string math
-- ============================================================================
-- This is the CORE technique: extract hours and minutes from VARCHAR timestamps
-- using SUBSTRING, CAST to INT, convert to total minutes, then subtract.
-- Formula: (exit_hour * 60 + exit_min) - (entry_hour * 60 + entry_min)
-- We filter to same-day transits (date portions match) so the subtraction
-- stays positive. Cross-midnight cases need separate handling (see next query).
ASSERT VALUE transit_minutes = 300 WHERE id = 21
ASSERT ROW_COUNT = 10
SELECT id, vessel_name, lock_name, entry_time, exit_time,
       (CAST(SUBSTRING(exit_time, 12, 2) AS INT) * 60 + CAST(SUBSTRING(exit_time, 15, 2) AS INT))
       - (CAST(SUBSTRING(entry_time, 12, 2) AS INT) * 60 + CAST(SUBSTRING(entry_time, 15, 2) AS INT))
       AS transit_minutes
FROM {{zone_name}}.delta_demos.vessel_transit
WHERE status = 'completed'
  AND SUBSTRING(exit_time, 1, 10) = SUBSTRING(entry_time, 1, 10)
ORDER BY transit_minutes DESC
LIMIT 10;


-- ============================================================================
-- LEARN: Vessel type statistics (completed transits only)
-- ============================================================================
-- Aggregating by vessel_type shows the fleet composition and cargo range.
-- Only completed transits are counted — in_transit and delayed are excluded.
-- Tankers carry the heaviest loads; passenger vessels carry the lightest.
ASSERT VALUE vessel_count = 9 WHERE vessel_type = 'tanker'
ASSERT VALUE vessel_count = 8 WHERE vessel_type = 'bulk_carrier'
ASSERT ROW_COUNT = 4
SELECT vessel_type, COUNT(*) AS vessel_count,
       MIN(cargo_tons) AS min_cargo, MAX(cargo_tons) AS max_cargo
FROM {{zone_name}}.delta_demos.vessel_transit
WHERE status = 'completed'
GROUP BY vessel_type
ORDER BY vessel_type;


-- ============================================================================
-- LEARN: Cross-midnight transits (date boundary crossing)
-- ============================================================================
-- When a vessel enters before midnight and exits after midnight, the date
-- portion of entry_time and exit_time differ. The simple same-day duration
-- formula would produce a negative number, so these must be handled separately.
-- SUBSTRING(timestamp, 1, 10) extracts the date portion for comparison.
ASSERT ROW_COUNT = 1
SELECT id, vessel_name, vessel_type, lock_name, entry_time, exit_time
FROM {{zone_name}}.delta_demos.vessel_transit
WHERE status = 'completed'
  AND SUBSTRING(exit_time, 1, 10) != SUBSTRING(entry_time, 1, 10)
ORDER BY id;


-- ============================================================================
-- LEARN: Lock throughput statistics
-- ============================================================================
-- Aggregating by lock_name shows how traffic and cargo distribute across the
-- three canal locks. SUM(cargo_tons) reveals which lock handles the heaviest
-- freight volume. MIN/MAX of entry/exit times show the operational window.
ASSERT VALUE total_vessels = 12 WHERE lock_name = 'North Lock'
ASSERT VALUE total_cargo = 717800 WHERE lock_name = 'Central Lock'
ASSERT ROW_COUNT = 3
SELECT lock_name, COUNT(*) AS total_vessels,
       SUM(cargo_tons) AS total_cargo,
       MIN(entry_time) AS first_entry, MAX(exit_time) AS last_exit
FROM {{zone_name}}.delta_demos.vessel_transit
GROUP BY lock_name
ORDER BY lock_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total vessel count is 35
ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.delta_demos.vessel_transit;

-- Verify status counts
ASSERT VALUE completed_count = 30
SELECT COUNT(*) AS completed_count FROM {{zone_name}}.delta_demos.vessel_transit WHERE status = 'completed';

ASSERT VALUE in_transit_count = 3
SELECT COUNT(*) AS in_transit_count FROM {{zone_name}}.delta_demos.vessel_transit WHERE status = 'in_transit';

ASSERT VALUE delayed_count = 2
SELECT COUNT(*) AS delayed_count FROM {{zone_name}}.delta_demos.vessel_transit WHERE status = 'delayed';

-- Verify direction split
ASSERT VALUE inbound_count = 18
SELECT COUNT(*) AS inbound_count FROM {{zone_name}}.delta_demos.vessel_transit WHERE direction = 'inbound';

ASSERT VALUE outbound_count = 17
SELECT COUNT(*) AS outbound_count FROM {{zone_name}}.delta_demos.vessel_transit WHERE direction = 'outbound';

-- Verify total cargo tonnage
ASSERT VALUE total_cargo = 2088800
SELECT SUM(cargo_tons) AS total_cargo FROM {{zone_name}}.delta_demos.vessel_transit;
