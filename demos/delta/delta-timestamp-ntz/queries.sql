-- ============================================================================
-- QUERIES: Hospital Shift Handover — Timestamp NTZ Demo
-- ============================================================================
--
-- Timestamp NTZ (No TimeZone) preserves wall-clock times exactly as entered.
-- In healthcare, "07:00" means 7 AM at THAT hospital — it must never shift
-- when viewed from another timezone. This is critical for patient safety,
-- HIPAA compliance, and accurate shift handover records.
--
-- Local times (VARCHAR) stay human-readable for clinical staff.
-- UTC times (VARCHAR) enable true chronological ordering across hospitals.
-- The NTZ concept: store the literal clock reading, not a UTC instant.
-- ============================================================================

-- ============================================================================
-- Query 1: EXPLORE — What does the shift schedule look like?
-- ============================================================================
-- First look at the data: staff, hospitals, local vs UTC times, and statuses.
-- Notice that id=2 (Maria Santos) shows 'on_break' from our UPDATE.
-- ============================================================================
ASSERT VALUE status = 'on_break' WHERE id = 2
ASSERT ROW_COUNT = 10
SELECT id, staff_name, hospital, shift_start_local, shift_start_utc, role, status
FROM {{zone_name}}.delta_demos.shift_handover
ORDER BY id
LIMIT 10;

-- ============================================================================
-- Query 2: LEARN — NTZ means local times are preserved exactly
-- ============================================================================
-- The core NTZ insight: earliest_local is IDENTICAL across all 5 hospitals
-- (07:00:00) because each hospital recorded "7 AM" in their own wall-clock.
-- But earliest_utc differs — NYC 11:00, Chicago 12:00, Denver 13:00, LA 14:00,
-- Honolulu 17:00. Same local time, different absolute instants.
-- ============================================================================
ASSERT ROW_COUNT = 5
ASSERT VALUE staff_count = 6 WHERE hospital = 'Metro General'
SELECT hospital, timezone_offset, COUNT(*) AS staff_count,
       MIN(shift_start_local) AS earliest_local,
       MIN(shift_start_utc) AS earliest_utc
FROM {{zone_name}}.delta_demos.shift_handover
GROUP BY hospital, timezone_offset
ORDER BY timezone_offset;

-- ============================================================================
-- Query 3: LEARN — Night shifts cross date boundaries
-- ============================================================================
-- Night shift staff (23:00-07:00) have shift_end on a different date than
-- shift_start. NTZ preserves both dates exactly — no timezone math needed
-- to determine whether a shift spans midnight.
-- ============================================================================
ASSERT ROW_COUNT = 10
SELECT id, staff_name, hospital, shift_start_local, shift_end_local, role
FROM {{zone_name}}.delta_demos.shift_handover
WHERE SUBSTRING(shift_end_local, 1, 10) != SUBSTRING(shift_start_local, 1, 10)
ORDER BY hospital;

-- ============================================================================
-- Query 4: LEARN — UTC ordering reveals true chronological sequence
-- ============================================================================
-- All doctors start at "07:00" or "15:00" locally, but UTC reveals NYC
-- doctors start earliest (UTC-4 is closest to UTC). Sorting by UTC gives
-- the real-world sequence of events across all hospitals — essential for
-- coordinating cross-facility transfers and incident timelines.
-- ============================================================================
ASSERT ROW_COUNT = 10
ASSERT VALUE staff_name = 'James Chen' WHERE id = 1
SELECT id, staff_name, hospital, shift_start_local, shift_start_utc, timezone_offset
FROM {{zone_name}}.delta_demos.shift_handover
WHERE role = 'doctor'
ORDER BY shift_start_utc
LIMIT 10;

-- ============================================================================
-- Query 5: LEARN — Status distribution after DML
-- ============================================================================
-- Our UPDATEs changed 3 staff to on_break and 2 to completed. Delta Lake
-- recorded these as new versions while preserving the original timestamps
-- exactly — no timezone drift from the read-modify-write cycle.
-- ============================================================================
ASSERT VALUE staff_count = 25 WHERE status = 'active'
ASSERT VALUE staff_count = 2 WHERE status = 'completed'
ASSERT VALUE staff_count = 3 WHERE status = 'on_break'
ASSERT ROW_COUNT = 3
SELECT status, COUNT(*) AS staff_count
FROM {{zone_name}}.delta_demos.shift_handover
GROUP BY status
ORDER BY status;

-- ============================================================================
-- Query 6: VERIFY — All Checks
-- ============================================================================
-- Comprehensive validation of data integrity: total count, per-hospital
-- distribution, role balance, and night shift identification.
-- ============================================================================

-- Verify total staff count is 30
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.shift_handover;

-- Verify each hospital has exactly 6 staff
ASSERT VALUE hospital_count = 6 WHERE hospital = 'Metro General'
SELECT hospital, COUNT(*) AS hospital_count FROM {{zone_name}}.delta_demos.shift_handover
WHERE hospital = 'Metro General'
GROUP BY hospital;

ASSERT VALUE hospital_count = 6 WHERE hospital = 'Island Hospital'
SELECT hospital, COUNT(*) AS hospital_count FROM {{zone_name}}.delta_demos.shift_handover
WHERE hospital = 'Island Hospital'
GROUP BY hospital;

-- Verify role distribution: 10 each
ASSERT VALUE doctor_count = 10
SELECT COUNT(*) AS doctor_count FROM {{zone_name}}.delta_demos.shift_handover WHERE role = 'doctor';

ASSERT VALUE nurse_count = 10
SELECT COUNT(*) AS nurse_count FROM {{zone_name}}.delta_demos.shift_handover WHERE role = 'nurse';

ASSERT VALUE tech_count = 10
SELECT COUNT(*) AS tech_count FROM {{zone_name}}.delta_demos.shift_handover WHERE role = 'technician';

-- Verify night shift count (cross-date boundary)
ASSERT VALUE night_shift_count = 10
SELECT COUNT(*) AS night_shift_count FROM {{zone_name}}.delta_demos.shift_handover
WHERE SUBSTRING(shift_end_local, 1, 10) != SUBSTRING(shift_start_local, 1, 10);
