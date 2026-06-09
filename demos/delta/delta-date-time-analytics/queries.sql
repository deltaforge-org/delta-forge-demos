-- ============================================================================
-- Employee Attendance Tracking — Date/Time Analytics — Educational Queries
-- ============================================================================
-- WHAT: Date/time functions analyze employee attendance patterns across two
--       weeks of clock-in/clock-out data for 5 employees in 4 departments.
-- WHY:  HR teams need to compute hours worked, identify late arrivals, group
--       by pay periods (weeks/months), and spot attendance trends by day.
-- HOW:  EXTRACT(EPOCH FROM ...) computes precise durations. DATE_TRUNC groups
--       by week/month. EXTRACT(DOW FROM ...) and DATE_PART pull components.
--       CASE expressions classify arrival windows and flag thresholds.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Alice Chen's attendance with computed hours worked
-- ============================================================================
-- Epoch-based duration: subtract clock_in epoch from clock_out epoch to get
-- seconds, then divide by 3600 to get decimal hours. ROUND to 2 places.
-- This is the CORE technique for precise time-difference calculations when
-- the column type is TIMESTAMP (not VARCHAR).
-- ============================================================================
ASSERT VALUE hours_worked = 9.00 WHERE record_id = 1
ASSERT ROW_COUNT = 10
SELECT record_id, employee_name, department, record_date,
       clock_in, clock_out, is_remote,
       ROUND((EXTRACT(EPOCH FROM clock_out) - EXTRACT(EPOCH FROM clock_in)) / 3600.0, 2) AS hours_worked
FROM {{zone_name}}.delta_demos.attendance_records
WHERE employee_id = 1
ORDER BY record_date;


-- ============================================================================
-- LEARN: DATE_TRUNC — Weekly hours per employee for payroll periods
-- ============================================================================
-- DATE_TRUNC('week', record_date) truncates each date to its Monday,
-- grouping all five workdays into a single pay-period bucket. SUM of
-- epoch-based hours gives total hours worked that week per employee.
-- Filtered to Alice (employee_id = 1) to show her two weekly buckets.
-- ============================================================================
ASSERT VALUE weekly_hours = 43.00 WHERE week_start = '2024-03-04'
ASSERT ROW_COUNT = 2
SELECT employee_id, employee_name,
       CAST(DATE_TRUNC('week', record_date) AS DATE) AS week_start,
       ROUND(SUM((EXTRACT(EPOCH FROM clock_out) - EXTRACT(EPOCH FROM clock_in)) / 3600.0), 2) AS weekly_hours
FROM {{zone_name}}.delta_demos.attendance_records
WHERE employee_id = 1
GROUP BY employee_id, employee_name, DATE_TRUNC('week', record_date)
ORDER BY employee_id, week_start;


-- ============================================================================
-- LEARN: EXTRACT day-of-week — Attendance and late arrivals by weekday
-- ============================================================================
-- EXTRACT(DOW FROM record_date) returns 0=Sunday through 6=Saturday.
-- Since all records are Mon–Fri, we see DOW values 1–5.
-- Each day has 10 records (5 employees × 2 weeks).
-- Late arrival = clock_in hour >= 9. Wednesday has the most late arrivals (5).
-- ============================================================================
ASSERT VALUE attendance_count = 10 WHERE day_of_week = 1
ASSERT VALUE late_arrivals = 5 WHERE day_of_week = 3
ASSERT ROW_COUNT = 5
SELECT EXTRACT(DOW FROM record_date) AS day_of_week,
       COUNT(*) AS attendance_count,
       SUM(CASE WHEN EXTRACT(HOUR FROM clock_in) >= 9 THEN 1 ELSE 0 END) AS late_arrivals
FROM {{zone_name}}.delta_demos.attendance_records
GROUP BY EXTRACT(DOW FROM record_date)
ORDER BY day_of_week;


-- ============================================================================
-- LEARN: DATE_PART hour — Clock-in time distribution analysis
-- ============================================================================
-- Classifies each clock-in into arrival windows using DATE_PART('hour', ...)
-- and DATE_PART('minute', ...):
--   early   = 08:00–08:29 (25 employees)
--   on_time = 08:30–08:59 (13 employees)
--   late    = 09:00+      (12 employees)
-- This helps HR identify what percentage of the workforce arrives on time.
-- ============================================================================
ASSERT VALUE employee_count = 25 WHERE arrival_window = 'early'
ASSERT VALUE employee_count = 12 WHERE arrival_window = 'late'
ASSERT ROW_COUNT = 3
SELECT CASE
         WHEN EXTRACT(HOUR FROM clock_in) = 8 AND EXTRACT(MINUTE FROM clock_in) < 30 THEN 'early'
         WHEN EXTRACT(HOUR FROM clock_in) = 8 AND EXTRACT(MINUTE FROM clock_in) >= 30 THEN 'on_time'
         ELSE 'late'
       END AS arrival_window,
       COUNT(*) AS employee_count
FROM {{zone_name}}.delta_demos.attendance_records
GROUP BY CASE
           WHEN EXTRACT(HOUR FROM clock_in) = 8 AND EXTRACT(MINUTE FROM clock_in) < 30 THEN 'early'
           WHEN EXTRACT(HOUR FROM clock_in) = 8 AND EXTRACT(MINUTE FROM clock_in) >= 30 THEN 'on_time'
           ELSE 'late'
         END
ORDER BY arrival_window;


-- ============================================================================
-- LEARN: Date arithmetic — Average daily hours by department
-- ============================================================================
-- Computes average hours worked per day for each department using epoch math.
-- A CASE expression flags departments averaging below 8 hours — useful for
-- HR compliance checks. All four departments average above 8 hours here.
-- Engineering leads with 8.80 hours (two employees who tend to work longer).
-- ============================================================================
ASSERT VALUE avg_daily_hours = 8.80 WHERE department = 'Engineering'
ASSERT VALUE avg_daily_hours = 8.45 WHERE department = 'Marketing'
ASSERT ROW_COUNT = 4
SELECT department,
       COUNT(*) AS total_records,
       ROUND(AVG((EXTRACT(EPOCH FROM clock_out) - EXTRACT(EPOCH FROM clock_in)) / 3600.0), 2) AS avg_daily_hours,
       CASE
         WHEN AVG((EXTRACT(EPOCH FROM clock_out) - EXTRACT(EPOCH FROM clock_in)) / 3600.0) < 8.0 THEN 'YES'
         ELSE 'NO'
       END AS below_threshold
FROM {{zone_name}}.delta_demos.attendance_records
GROUP BY department
ORDER BY department;


-- ============================================================================
-- LEARN: Epoch conversion — Precise minutes worked per day
-- ============================================================================
-- Converts the epoch difference to minutes instead of hours for payroll
-- systems that track to the minute. ROUND(..., 0) removes fractional seconds.
-- Alice's first day: 08:15 to 17:15 = exactly 540 minutes (9 hours).
-- ============================================================================
ASSERT VALUE minutes_worked = 540 WHERE record_id = 1
ASSERT ROW_COUNT = 10
SELECT record_id, employee_name, record_date,
       clock_in, clock_out,
       ROUND((EXTRACT(EPOCH FROM clock_out) - EXTRACT(EPOCH FROM clock_in)) / 60.0, 0) AS minutes_worked
FROM {{zone_name}}.delta_demos.attendance_records
WHERE employee_id = 1
ORDER BY record_date;


-- ============================================================================
-- LEARN: Combined — Monthly attendance summary per employee
-- ============================================================================
-- Combines DATE_TRUNC('month', ...) grouping with epoch duration math and
-- late-arrival counting. Since all data falls in March 2024, there is one
-- month bucket per employee. Shows total days worked, average daily hours,
-- and late arrival count — a typical HR monthly report.
-- ============================================================================
ASSERT VALUE days_worked = 10 WHERE employee_id = 1
ASSERT VALUE avg_daily_hours = 8.70 WHERE employee_id = 1
ASSERT VALUE late_arrival_count = 2 WHERE employee_id = 1
ASSERT ROW_COUNT = 5
SELECT employee_id, employee_name,
       CAST(DATE_TRUNC('month', record_date) AS DATE) AS month_start,
       COUNT(*) AS days_worked,
       ROUND(AVG((EXTRACT(EPOCH FROM clock_out) - EXTRACT(EPOCH FROM clock_in)) / 3600.0), 2) AS avg_daily_hours,
       SUM(CASE WHEN EXTRACT(HOUR FROM clock_in) >= 9 THEN 1 ELSE 0 END) AS late_arrival_count
FROM {{zone_name}}.delta_demos.attendance_records
GROUP BY employee_id, employee_name, DATE_TRUNC('month', record_date)
ORDER BY employee_id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 50
ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.delta_demos.attendance_records;

-- Verify total late arrivals (clock_in hour >= 9)
ASSERT VALUE late_total = 12
SELECT COUNT(*) AS late_total
FROM {{zone_name}}.delta_demos.attendance_records
WHERE EXTRACT(HOUR FROM clock_in) >= 9;

-- Verify remote vs onsite counts
ASSERT VALUE remote_count = 16
SELECT COUNT(*) AS remote_count
FROM {{zone_name}}.delta_demos.attendance_records
WHERE is_remote = true;

ASSERT VALUE onsite_count = 34
SELECT COUNT(*) AS onsite_count
FROM {{zone_name}}.delta_demos.attendance_records
WHERE is_remote = false;

-- Verify department distribution
ASSERT VALUE eng_count = 20
SELECT COUNT(*) AS eng_count
FROM {{zone_name}}.delta_demos.attendance_records
WHERE department = 'Engineering';

ASSERT VALUE sales_count = 10
SELECT COUNT(*) AS sales_count
FROM {{zone_name}}.delta_demos.attendance_records
WHERE department = 'Sales';

-- Verify total hours across all employees
ASSERT VALUE total_hours = 430.50
SELECT ROUND(SUM((EXTRACT(EPOCH FROM clock_out) - EXTRACT(EPOCH FROM clock_in)) / 3600.0), 2) AS total_hours
FROM {{zone_name}}.delta_demos.attendance_records;
