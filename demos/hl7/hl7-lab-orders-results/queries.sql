-- ============================================================================
-- HL7 Lab Orders & Results — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge unifies ORM (order) and ORU (result)
-- messages from multiple LIS/EHR systems into queryable lab data.
--
-- Two tables are available:
--   lab_orders   — Compact view: ORM messages only (via orm*.hl7 glob)
--   lab_results  — Enriched view: All messages with materialized OBX fields
--                  (use msh_9 LIKE 'ORU%' to filter to results only)
--
-- Column reference (always available — MSH header fields):
--   msh_3  = Sending Application     msh_4  = Sending Facility
--   msh_7  = Message Date/Time       msh_9  = Message Type
--   msh_10 = Message Control ID      msh_12 = Version ID
--
-- Materialized columns (lab_results table only):
--   pid_3  = Patient ID              pid_5  = Patient Name
--   obr_4  = Universal Service ID    obx_2  = Value Type (NM/ST/TX/SN/HD)
--   obx_3  = Observation Identifier  obx_5  = Observation Value
--   obx_6  = Units                   obx_7  = Reference Range
--   obx_8  = Abnormal Flags (H/L/N)
-- ============================================================================


-- ============================================================================
-- 1. All Lab Orders — Order Overview
-- ============================================================================
-- Shows the 3 ORM (Order Message) files. These are orders placed by
-- clinicians requesting lab tests or radiology procedures.
--
-- What you'll see:
--   - df_file_name:     Source .hl7 file
--   - sending_app:      The EHR system that placed the order
--   - sending_facility: The hospital or clinic
--   - message_type:     "ORM^O01" (General Order Message)
--   - hl7_version:      Protocol version (2.3 or 2.5/2.5.1)

ASSERT ROW_COUNT = 3
ASSERT VALUE sending_app = 'MESA_OP' WHERE df_file_name LIKE '%interfaceware%'
ASSERT VALUE sending_app = 'EHR' WHERE df_file_name LIKE '%orm_o01_order%'
ASSERT VALUE sending_app = 'CARDIAC' WHERE df_file_name LIKE '%ritten%'
ASSERT VALUE hl7_version = '2.5.1' WHERE df_file_name LIKE '%orm_o01_order%'
ASSERT VALUE hl7_version = '2.3' WHERE df_file_name LIKE '%interfaceware%'
ASSERT VALUE hl7_version = '2.5' WHERE df_file_name LIKE '%ritten%'
SELECT
    df_file_name,
    msh_3 AS sending_app,
    msh_4 AS sending_facility,
    msh_9 AS message_type,
    msh_12 AS hl7_version
FROM {{zone_name}}.hl7_demos.lab_orders
ORDER BY df_file_name;


-- ============================================================================
-- 2. Lab Results — ORU Observation Overview
-- ============================================================================
-- Shows the 5 ORU (Observation Result) messages with their source system
-- and first observation. The materialized table extracts OBX fields as
-- first-class columns.
--
-- What you'll see:
--   - sending_app:      The lab system that generated results
--   - patient_name:     Patient name from PID-5 (LAST^FIRST^MIDDLE format)
--   - test_ordered:     Test name from OBR-4 (e.g. "80053^COMP METABOLIC PANEL")
--   - first_obs_id:     Observation ID from the first OBX-3 (e.g. "2345-7^GLUCOSE^LN")
--   - first_obs_value:  Result value from the first OBX-5 (e.g. "95")
--   - units:            Measurement units from OBX-6 (e.g. "mg/dL")
--   - reference_range:  Normal range from OBX-7 (e.g. "70-100")
--   - abnormal_flag:    Flag from OBX-8: H=High, L=Low, N=Normal, empty=none
--
-- Note: Only the FIRST OBX segment is materialized per message (default
-- repeating_segment_mode). Use df_message_json for all OBX segments.
-- Filtered to ORU messages only (ORM rows have NULL OBX fields).

ASSERT ROW_COUNT = 5
ASSERT VALUE first_obs_value = '95' WHERE df_file_name LIKE '%lab_result%'
ASSERT VALUE units = 'mg/dL' WHERE df_file_name LIKE '%lab_result%'
ASSERT VALUE reference_range = '70-100' WHERE df_file_name LIKE '%lab_result%'
ASSERT VALUE abnormal_flag = 'N' WHERE df_file_name LIKE '%lab_result%'
ASSERT VALUE abnormal_flag = 'H' WHERE df_file_name LIKE '%glucose%'
ASSERT VALUE first_obs_value = 'given' WHERE df_file_name LIKE '%immunization%'
ASSERT VALUE first_obs_value = 'Positive' WHERE df_file_name LIKE '%ritten%'
SELECT
    df_file_name,
    msh_3 AS sending_app,
    msh_12 AS hl7_version,
    pid_5 AS patient_name,
    obr_4 AS test_ordered,
    obx_3 AS first_obs_id,
    obx_5 AS first_obs_value,
    obx_6 AS units,
    obx_7 AS reference_range,
    obx_8 AS abnormal_flag
FROM {{zone_name}}.hl7_demos.lab_results
WHERE msh_9 LIKE 'ORU%'
ORDER BY df_file_name;


-- ============================================================================
-- 3. Observation Value Types
-- ============================================================================
-- OBX-2 indicates the data type of the observation value. Different tests
-- use different value types:
--   NM = Numeric (glucose level, metabolic analytes)
--   ST = String (immunization status "given")
--   TX = Text (radiology narrative report)
--   SN = Structured Numeric (e.g. "^182" in glucose with comparator)
--   HD = Hierarchic Designator (DICOM UID in radiology)
--
-- What you'll see:
--   - value_type:    The OBX-2 code
--   - result_count:  How many messages use that value type
--
-- Note: Only the first OBX per message is materialized, so this shows
-- the value type of each message's primary observation.

ASSERT ROW_COUNT = 4
ASSERT VALUE result_count = 1 WHERE value_type = 'NM'
ASSERT VALUE result_count = 1 WHERE value_type = 'SN'
ASSERT VALUE result_count = 1 WHERE value_type = 'ST'
ASSERT VALUE result_count = 1 WHERE value_type = 'HD'
SELECT
    obx_2 AS value_type,
    COUNT(*) AS result_count
FROM {{zone_name}}.hl7_demos.lab_results
WHERE obx_2 IS NOT NULL AND obx_2 <> ''
GROUP BY obx_2
ORDER BY result_count DESC;


-- ============================================================================
-- 4. Abnormal Results — Flagged Observations
-- ============================================================================
-- OBX-8 contains abnormal flags set by the lab system:
--   H = High (above reference range)
--   L = Low (below reference range)
--   N = Normal
--   (empty) = not flagged
--
-- What you'll see:
--   - patient_name:    Who the abnormal result belongs to
--   - test_id:         Observation identifier from OBX-3
--   - value:           The actual result value
--   - units:           Measurement units
--   - reference_range: What the normal range is
--   - abnormal_flag:   H or L

ASSERT ROW_COUNT = 1
ASSERT VALUE abnormal_flag = 'H'
ASSERT VALUE units = 'mg/dl'
ASSERT VALUE reference_range = '70-105'
SELECT
    df_file_name,
    pid_5 AS patient_name,
    obx_3 AS test_id,
    obx_5 AS value,
    obx_6 AS units,
    obx_7 AS reference_range,
    obx_8 AS abnormal_flag
FROM {{zone_name}}.hl7_demos.lab_results
WHERE obx_8 IS NOT NULL AND obx_8 <> '' AND obx_8 <> 'N';


-- ============================================================================
-- 5. Orders vs Results — Message Type Counts
-- ============================================================================
-- Verifies the expected number of messages in each table and category.
-- lab_orders only loads ORM files (via orm*.hl7 glob pattern).
-- lab_results loads all 8 files (via *.hl7 glob) — filter by msh_9
-- to separate ORM orders from ORU results.
--
-- What you'll see:
--   - lab_orders:          3 (ORM messages only)
--   - lab_results_total:   8 (all messages — both ORM and ORU)
--   - lab_results_oru_only: 5 (filtered to ORU observation results)

ASSERT ROW_COUNT = 3
ASSERT VALUE row_count = 3 WHERE table_name = 'lab_orders'
ASSERT VALUE row_count = 8 WHERE table_name = 'lab_results_total'
ASSERT VALUE row_count = 5 WHERE table_name = 'lab_results_oru_only'
SELECT 'lab_orders' AS table_name, COUNT(*) AS row_count
FROM {{zone_name}}.hl7_demos.lab_orders
UNION ALL
SELECT 'lab_results_total' AS table_name, COUNT(*) AS row_count
FROM {{zone_name}}.hl7_demos.lab_results
UNION ALL
SELECT 'lab_results_oru_only' AS table_name, COUNT(*) AS row_count
FROM {{zone_name}}.hl7_demos.lab_results
WHERE msh_9 LIKE 'ORU%';


-- ============================================================================
-- 6. Full Order Details via JSON — Multi-Test Order
-- ============================================================================
-- The df_message_json column contains the complete parsed message as JSON.
-- The multi-test ORM order (orm_o01_order.hl7) has 3 OBR segments
-- ordering CBC, Basic Metabolic Panel, and Urine Culture simultaneously.
-- Only the first OBR is visible in materialized columns — all 3 are
-- in the JSON.
--
-- What you'll see:
--   - df_file_name:     The order file
--   - message_type:     "ORM^O01^ORM_O01"
--   - df_message_json:  Full order including all 3 OBR and ORC segments

ASSERT ROW_COUNT = 1
ASSERT VALUE message_type = 'ORM^O01^ORM_O01'
SELECT
    df_file_name,
    msh_9 AS message_type,
    df_message_json
FROM {{zone_name}}.hl7_demos.lab_orders
WHERE df_file_name LIKE '%orm_o01_order%';


-- ============================================================================
-- 7. Full Result Details via JSON — Comprehensive Metabolic Panel
-- ============================================================================
-- The CMP result (oru_r01_lab_result.hl7) has 14 OBX segments — one
-- per analyte (glucose, BUN, creatinine, sodium, potassium, chloride,
-- bicarbonate, calcium, total protein, albumin, bilirubin, alk phos,
-- AST, ALT). Only the first OBX (glucose) is materialized.
--
-- What you'll see:
--   - patient_name:     "SMITH^JOHN^DAVID^JR^MR"
--   - first_obs:        "2345-7^GLUCOSE^LN" (first OBX-3)
--   - first_value:      "95" (first OBX-5, glucose in mg/dL)
--   - df_message_json:  All 14 OBX segments with values, units, and ranges

ASSERT ROW_COUNT = 1
ASSERT VALUE patient_name = 'SMITH^JOHN^DAVID^JR^MR'
ASSERT VALUE first_value = '95'
SELECT
    df_file_name,
    pid_5 AS patient_name,
    obx_3 AS first_obs,
    obx_5 AS first_value,
    df_message_json
FROM {{zone_name}}.hl7_demos.lab_results
WHERE df_file_name LIKE '%lab_result%';


-- ============================================================================
-- 8. Source Systems — LIS/EHR Analysis
-- ============================================================================
-- Shows which lab and EHR systems generated all messages in the
-- lab_results table. msh_9 distinguishes ORM orders from ORU results,
-- letting you see the full picture of order-to-result workflow.
--
-- What you'll see:
--   - df_file_name:     Source file
--   - sending_app:      System name (EHR, LAB, CARDIAC, MESA_OP, GHH LAB, etc.)
--   - sending_facility: Hospital/lab name
--   - message_type:     ORM^O01 (order) or ORU^R01 (result)
--   - hl7_version:      Protocol version

ASSERT ROW_COUNT = 8
ASSERT VALUE sending_app = 'LAB' WHERE df_file_name LIKE '%oru_r01_lab_result%'
ASSERT VALUE sending_app = 'GHH LAB' WHERE df_file_name LIKE '%glucose%'
ASSERT VALUE sending_app = 'EHR' WHERE df_file_name LIKE '%orm_o01_order%'
ASSERT VALUE message_type = 'ORU^R01^ORU_R01' WHERE df_file_name LIKE '%oru_r01_lab_result%'
ASSERT VALUE hl7_version = '2.4' WHERE df_file_name LIKE '%glucose%'
ASSERT VALUE hl7_version = '2.3' WHERE df_file_name LIKE '%immunization%'
SELECT
    df_file_name,
    msh_3 AS sending_app,
    msh_4 AS sending_facility,
    msh_9 AS message_type,
    msh_12 AS hl7_version
FROM {{zone_name}}.hl7_demos.lab_results
ORDER BY msh_9, df_file_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: message counts, HL7 version diversity, and
-- metadata population across both tables. All checks should return PASS.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'order_count_3'
ASSERT VALUE result = 'PASS' WHERE check_name = 'results_total_8'
ASSERT VALUE result = 'PASS' WHERE check_name = 'results_oru_5'
ASSERT VALUE result = 'PASS' WHERE check_name = 'multi_version'
ASSERT VALUE result = 'PASS' WHERE check_name = 'orders_json_populated'
ASSERT VALUE result = 'PASS' WHERE check_name = 'results_pid5_populated'
SELECT check_name, result FROM (

    -- Check 1: 3 ORM order messages in lab_orders
    SELECT 'order_count_3' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.lab_orders) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 8 total messages in lab_results (3 ORM + 5 ORU)
    SELECT 'results_total_8' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.lab_results) = 8
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: 5 ORU messages when filtered by message type
    SELECT 'results_oru_5' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.lab_results
                       WHERE msh_9 LIKE 'ORU%') = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: At least 3 distinct HL7 versions (actual: 4)
    SELECT 'multi_version' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT msh_12) FROM {{zone_name}}.hl7_demos.lab_results) >= 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: df_message_json populated for all 3 orders
    SELECT 'orders_json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.lab_orders
                       WHERE df_message_json IS NOT NULL) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: PID_5 populated in at least one ORU result
    SELECT 'results_pid5_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.lab_results
                       WHERE msh_9 LIKE 'ORU%' AND pid_5 IS NOT NULL AND pid_5 <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
