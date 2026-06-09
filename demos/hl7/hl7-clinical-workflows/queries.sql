-- ============================================================================
-- HL7 Clinical Workflows — Demo Queries
-- ============================================================================
-- Queries showcasing MDM clinical documents, SIU scheduling, and HL7 parser
-- robustness with edge-case messages.
--
-- Two tables are available:
--   clinical_messages      — Compact view: MSH header + full JSON
--   clinical_materialized  — Enriched view: MSH + PID/TXA/SCH/OBX columns
--
-- Column reference (always available — MSH header fields):
--   MSH_3  = Sending Application     MSH_4  = Sending Facility
--   MSH_7  = Message Date/Time       MSH_9  = Message Type
--   MSH_10 = Message Control ID      MSH_12 = Version ID
--
-- Materialized columns (clinical_materialized table only):
--   PID_5  = Patient Name            TXA_2  = Document Type
--   TXA_12 = Document Unique ID      TXA_14 = Document Status
--   SCH_1  = Appointment ID          SCH_7  = Appointment Reason
--   SCH_10 = Duration/Units          SCH_25 = Filler Status Code
--   OBX_3  = Observation Identifier  OBX_5  = Observation Value
-- ============================================================================


-- ============================================================================
-- 1. All Clinical Messages — Overview
-- ============================================================================
-- Shows every message in this demo — one MDM document, two SIU
-- appointments (from different systems and HL7 versions), and one
-- edge-case ADT that tests parser robustness.
--
-- What you'll see:
--   - df_file_name: Source .hl7 file
--   - sending_app:  System that sent the message (DOCS, SCHEDULING, MESA_OP, TEST)
--   - message_type: MDM^T02, SIU^S12, or ADT^A01
--   - hl7_version:  2.3 or 2.5.1

ASSERT ROW_COUNT = 4
ASSERT VALUE sending_app = 'DOCS' WHERE df_file_name LIKE '%mdm%'
ASSERT VALUE sending_app = 'TEST' WHERE df_file_name LIKE '%edge%'
ASSERT VALUE message_type = 'ADT^A01' WHERE df_file_name LIKE '%edge%'
ASSERT VALUE hl7_version = '2.3' WHERE df_file_name LIKE '%v2.3%'
ASSERT VALUE hl7_version = '2.5.1' WHERE df_file_name LIKE '%mdm%'
SELECT
    df_file_name,
    msh_3 AS sending_app,
    msh_9 AS message_type,
    msh_12 AS hl7_version
FROM {{zone_name}}.hl7_demos.clinical_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Clinical Document — MDM Metadata
-- ============================================================================
-- The MDM^T02 message represents an authenticated History & Physical
-- document. The TXA segment contains document metadata, and OBX segments
-- hold the narrative sections.
--
-- What you'll see:
--   - patient_name:      "SMITH^JOHN^DAVID" from PID-5
--   - document_type:     "HP^History and Physical^L" from TXA-2
--   - document_id:       "DOC00001" from TXA-12
--   - document_status:   "AU^Authenticated^HL70271" from TXA-14
--   - first_section_id:  "HP-CHIEF^Chief Complaint^L" from the first OBX-3
--   - first_section_text: "Chest pain and shortness of breath" from OBX-5
--
-- Note: Only the first OBX is materialized. The full H&P has 7 sections
-- (Chief Complaint, HPI, PMH, Medications, Exam, Assessment, Plan) —
-- all accessible via df_message_json.

ASSERT ROW_COUNT = 1
ASSERT VALUE patient_name = 'SMITH^JOHN^DAVID'
ASSERT VALUE document_type = 'HP^History and Physical^L'
ASSERT VALUE document_id = 'DOC00001'
ASSERT VALUE document_status = 'AU^Authenticated^HL70271'
ASSERT VALUE first_section_text = 'Chest pain and shortness of breath'
SELECT
    df_file_name,
    msh_9 AS message_type,
    pid_5 AS patient_name,
    txa_2 AS document_type,
    txa_12 AS document_id,
    txa_14 AS document_status,
    obx_3 AS first_section_id,
    obx_5 AS first_section_text
FROM {{zone_name}}.hl7_demos.clinical_materialized
WHERE msh_9 LIKE 'MDM%';


-- ============================================================================
-- 3. Clinical Document — Full JSON for H&P Sections
-- ============================================================================
-- The df_message_json contains all 7 OBX sections of the History &
-- Physical narrative:
--   OBX 1: HP-CHIEF — Chief Complaint
--   OBX 2: HP-HPI   — History of Present Illness
--   OBX 3: HP-PMH   — Past Medical History
--   OBX 4: HP-MEDS  — Current Medications
--   OBX 5: HP-EXAM  — Physical Examination
--   OBX 6: HP-ASSESS — Assessment
--   OBX 7: HP-PLAN  — Plan
--
-- What you'll see:
--   - df_message_json: Full message as JSON — expand to see all 7 OBX
--     sections plus TXA document metadata, PID patient info, and PV1 visit

ASSERT ROW_COUNT = 1
ASSERT VALUE message_type = 'MDM^T02^MDM_T02'
SELECT
    df_file_name,
    msh_9 AS message_type,
    df_message_json
FROM {{zone_name}}.hl7_demos.clinical_messages
WHERE df_file_name LIKE '%mdm%';


-- ============================================================================
-- 4. Appointments — Scheduling Overview
-- ============================================================================
-- Two SIU^S12 (New Appointment Booking) messages from different
-- scheduling systems — one v2.5.1 and one v2.3.
--
-- What you'll see:
--   - scheduling_system: MSH-3 (SCHEDULING or MESA_OP)
--   - hl7_version:       2.5.1 or 2.3
--   - patient_name:      Patient from PID-5
--   - appointment_id:    Placer appointment ID from SCH-1
--   - appointment_reason: Reason text from SCH-7 (content varies by system)
--   - duration:          Duration or units from SCH-10 (varies by system)
--   - status:            Filler status from SCH-25 (may be NULL if the
--                        message has fewer than 25 SCH fields)
--
-- Note: Real-world HL7 systems structure SCH segments differently.
-- The v2.5.1 message has "SCHEDULED" at SCH-18 (not SCH-25), while
-- the v2.3 message puts "Scheduled" at SCH-25. This is normal HL7
-- variability — use df_message_json for reliable access to all fields.

ASSERT ROW_COUNT = 2
ASSERT VALUE scheduling_system = 'MESA_OP' WHERE df_file_name LIKE '%v2.3%'
ASSERT VALUE scheduling_system = 'SCHEDULING' WHERE df_file_name LIKE '%siu_s12_schedule%'
ASSERT VALUE hl7_version = '2.3' WHERE scheduling_system = 'MESA_OP'
ASSERT VALUE hl7_version = '2.5.1' WHERE scheduling_system = 'SCHEDULING'
ASSERT VALUE patient_name = 'SMITH^JOHN^DAVID' WHERE scheduling_system = 'SCHEDULING'
ASSERT VALUE patient_name = 'SMITH^PAUL' WHERE scheduling_system = 'MESA_OP'
SELECT
    df_file_name,
    msh_3 AS scheduling_system,
    msh_12 AS hl7_version,
    pid_5 AS patient_name,
    sch_1 AS appointment_id,
    sch_7 AS appointment_reason,
    sch_10 AS duration,
    sch_25 AS status
FROM {{zone_name}}.hl7_demos.clinical_materialized
WHERE msh_9 LIKE 'SIU%'
ORDER BY df_file_name;


-- ============================================================================
-- 5. Appointments — Full Resource Details via JSON
-- ============================================================================
-- The df_message_json contains the full scheduling resource model that
-- goes beyond the materialized SCH fields:
--   RGS — Resource Group
--   AIS — Service (what service is being provided)
--   AIG — General Resource (which provider is assigned)
--   AIL — Location (which room/clinic)
--   AIP — Personnel (which staff members)
--
-- What you'll see:
--   - system:           Scheduling system name
--   - df_message_json:  Full message — expand to see SCH, RGS, AIS, AIG,
--                       AIL, AIP segments with all appointment details

ASSERT ROW_COUNT = 2
ASSERT VALUE system = 'MESA_OP' WHERE df_file_name LIKE '%v2.3%'
ASSERT VALUE system = 'SCHEDULING' WHERE df_file_name LIKE '%siu_s12_schedule%'
SELECT
    df_file_name,
    msh_3 AS system,
    df_message_json
FROM {{zone_name}}.hl7_demos.clinical_messages
WHERE msh_9 LIKE 'SIU%'
ORDER BY df_file_name;


-- ============================================================================
-- 6. Edge Cases — Escape Sequence Handling
-- ============================================================================
-- The edge-case file tests HL7 parser robustness with escape sequences
-- embedded in patient names and observation values:
--   \F\ → | (field separator)    \T\ → & (subcomponent separator)
--   \S\ → ^ (component separator) \R\ → ~ (repetition separator)
--   \E\ → \ (escape character)    \X0D\ → carriage return
--
-- What you'll see:
--   - patient_name_decoded: PID-5 with decoded escapes — original was
--     "LAST\T\NAME^FIRST\S\NAME^MIDDLE\R\NAME" which decodes to
--     "LAST&NAME^FIRST^NAME^MIDDLE~NAME"
--   - first_obs_id:   "NOTES" (from first OBX-3)
--   - first_obs_value: Multi-line text with \X0D\ decoded to line breaks

ASSERT ROW_COUNT = 1
ASSERT VALUE patient_name_decoded = 'LAST&NAME^FIRST^NAME^MIDDLE~NAME'
ASSERT VALUE first_obs_id = 'NOTES'
SELECT
    df_file_name,
    pid_5 AS patient_name_decoded,
    obx_3 AS first_obs_id,
    obx_5 AS first_obs_value
FROM {{zone_name}}.hl7_demos.clinical_materialized
WHERE df_file_name LIKE '%edge%';


-- ============================================================================
-- 7. Edge Cases — Full Message for Inspection
-- ============================================================================
-- The complete edge-case message as JSON. This file tests:
--   - Escape sequences in PID (patient name) and NK1 (contact) fields
--   - Multi-line text in OBX-5 using \X0D\ carriage returns
--   - Numeric values in OBX (OBX-2=NM, value=123.456, with reference range)
--   - Empty OBX values (OBX-3=EMPTY with no OBX-5)
--   - Multiple special chars in a single field (all escape types combined)
--
-- What you'll see:
--   - df_message_json: Full parsed message — verify all segments decoded
--     correctly despite escape sequences and edge-case formatting

ASSERT ROW_COUNT = 1
ASSERT VALUE message_type = 'ADT^A01'
SELECT
    df_file_name,
    msh_9 AS message_type,
    df_message_json
FROM {{zone_name}}.hl7_demos.clinical_messages
WHERE df_file_name LIKE '%edge%';


-- ============================================================================
-- 8. Message Type Distribution
-- ============================================================================
-- Overview of all message types in this demo, grouped and counted.
--
-- What you'll see:
--   - message_type:   MDM^T02^MDM_T02, SIU^S12^SIU_S12, SIU^S12, or ADT^A01
--   - message_count:  1 each (except SIU which may group differently
--                     depending on whether the 3rd component is present)

ASSERT ROW_COUNT = 4
ASSERT VALUE message_count = 1 WHERE message_type = 'MDM^T02^MDM_T02'
ASSERT VALUE message_count = 1 WHERE message_type = 'ADT^A01'
ASSERT VALUE message_count = 1 WHERE message_type = 'SIU^S12'
ASSERT VALUE message_count = 1 WHERE message_type = 'SIU^S12^SIU_S12'
SELECT
    msh_9 AS message_type,
    COUNT(*) AS message_count
FROM {{zone_name}}.hl7_demos.clinical_messages
GROUP BY msh_9
ORDER BY message_count DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Automated pass/fail verification that the demo loaded correctly.
-- All checks should return PASS.

ASSERT ROW_COUNT = 5
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_messages_4'
ASSERT VALUE result = 'PASS' WHERE check_name = 'mdm_count_1'
ASSERT VALUE result = 'PASS' WHERE check_name = 'siu_count_2'
ASSERT VALUE result = 'PASS' WHERE check_name = 'edge_case_count_1'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
SELECT check_name, result FROM (

    -- Check 1: 4 total messages (1 MDM + 2 SIU + 1 edge-case ADT)
    SELECT 'total_messages_4' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.clinical_messages) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Exactly 1 MDM document message
    SELECT 'mdm_count_1' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.clinical_messages
                       WHERE msh_9 LIKE 'MDM%') = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Exactly 2 SIU scheduling messages
    SELECT 'siu_count_2' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.clinical_messages
                       WHERE msh_9 LIKE 'SIU%') = 2
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Exactly 1 edge-case message (identified by filename)
    SELECT 'edge_case_count_1' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.clinical_messages
                       WHERE df_file_name LIKE '%edge%') = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: df_message_json populated for all 4 messages
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.clinical_messages
                       WHERE df_message_json IS NOT NULL) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
