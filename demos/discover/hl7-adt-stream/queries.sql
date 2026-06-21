-- ============================================================================
-- Demo: HL7 v2 ADT Auto-Onboarding, Queries
-- ============================================================================
-- The landing folder holds 6 HL7 v2 ADT messages, one message per .hl7 file,
-- from three hospital EHRs feeding a regional Health Information Exchange:
--   adt_a01_harborview.hl7 : admit    (HARBORVIEW_EHR / HARBORVIEW, v2.5.1)
--   adt_a01_stmarys.hl7    : admit    (CERNER_PCC / STMARYS,        v2.6)
--   adt_a01_evergreen.hl7  : admit    (MEDITECH_ADT / EVERGREEN,    v2.3)
--   adt_a03_harborview.hl7 : discharge(HARBORVIEW_EHR / HARBORVIEW, v2.5.1)
--   adt_a03_stmarys.hl7    : discharge(CERNER_PCC / STMARYS,        v2.6)
--   adt_a08_evergreen.hl7  : update   (MEDITECH_ADT / EVERGREEN,    v2.3)
--
-- DISCOVER registers ONE external table USING HL7 over all six. Every
-- assertion value below was precomputed from the data files, not the engine.
--
-- Column reference (engine default HL7 mapping, always available):
--   msh_3  = Sending Application     msh_4  = Sending Facility
--   msh_9  = Message Type            msh_10 = Message Control ID
--   msh_12 = Version ID              df_message_json = full message as JSON
--   df_message_id = unique message hash
--   df_file_name / df_row_number = provenance from FILE_METADATA = true
--
-- Note on msh_9 formatting: HL7 v2.5+ emits the full structure component
-- (e.g. "ADT^A01^ADT_A01"); v2.3 emits only "ADT^A01". The exact-value
-- asserts below are therefore gated on a specific file, and the event-mix
-- query groups by the "ADT^A0n" family via a LIKE prefix so both formats
-- count together.
-- ============================================================================


-- ============================================================================
-- Query 1: DISCOVER in PRINT mode, review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the landed
-- files, classifies the text shape (the MSH|^~\& header marks HL7 v2), and
-- registers USING HL7 with the engine default mapping. FILE_METADATA adds
-- the df_file_name / df_row_number provenance columns to the generated table.

DISCOVER {{zone_name}}.discover_demos.adt_feed
    PATH 'discover-hl7-adt-stream'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode, auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). The
-- evidence column records the HL7 classification across the .hl7 files. This
-- single statement replaces a hand-written CREATE EXTERNAL TABLE ... USING
-- HL7 + hl7_config block.

DISCOVER {{zone_name}}.discover_demos.adt_feed
    PATH 'discover-hl7-adt-stream'
    WITH (FILE_METADATA = true);


-- ============================================================================
-- Query 3: The unified ADT feed, every message and its MSH header
-- ============================================================================
-- One table spans all three EHRs and all three HL7 versions. Each row is one
-- HL7 message file. The MSH header fields are first-class columns from the
-- engine default mapping.

ASSERT ROW_COUNT = 6
ASSERT VALUE sending_app = 'HARBORVIEW_EHR' WHERE df_file_name LIKE '%adt_a01_harborview%'
ASSERT VALUE sending_facility = 'STMARYS' WHERE df_file_name LIKE '%adt_a01_stmarys%'
ASSERT VALUE hl7_version = '2.3' WHERE df_file_name LIKE '%adt_a01_evergreen%'
ASSERT VALUE message_control_id = 'HVADM10001' WHERE df_file_name LIKE '%adt_a01_harborview%'
SELECT
    df_file_name,
    msh_3 AS sending_app,
    msh_4 AS sending_facility,
    msh_9 AS message_type,
    msh_12 AS hl7_version,
    msh_10 AS message_control_id
FROM {{zone_name}}.discover_demos.adt_feed
ORDER BY df_file_name;


-- ============================================================================
-- Query 4: Event mix, admits vs discharges vs updates
-- ============================================================================
-- Groups messages by the ADT trigger-event family. Because msh_9 carries the
-- full type string and its format varies by HL7 version (v2.5+ appends the
-- structure component), we group by the "ADT^A0n" prefix so each event family
-- counts together regardless of version. Counts are exact and precomputed.
--   A01 = admission (3),  A03 = discharge (2),  A08 = update (1)

ASSERT ROW_COUNT = 3
ASSERT VALUE message_count = 3 WHERE event_family = 'ADT^A01'
ASSERT VALUE message_count = 2 WHERE event_family = 'ADT^A03'
ASSERT VALUE message_count = 1 WHERE event_family = 'ADT^A08'
SELECT
    SUBSTRING(msh_9 FROM 1 FOR 7) AS event_family,
    COUNT(*) AS message_count
FROM {{zone_name}}.discover_demos.adt_feed
GROUP BY SUBSTRING(msh_9 FROM 1 FOR 7)
ORDER BY message_count DESC, event_family;


-- ============================================================================
-- Query 5: HL7 version distribution
-- ============================================================================
-- Shows how many messages came from each HL7 protocol version. This proves
-- DeltaForge parsing multiple HL7 versions in one table with no
-- version-specific configuration, all from the default DISCOVER mapping.
--   v2.3 (2),  v2.5.1 (2),  v2.6 (2)

ASSERT ROW_COUNT = 3
ASSERT VALUE message_count = 2 WHERE hl7_version = '2.3'
ASSERT VALUE message_count = 2 WHERE hl7_version = '2.5.1'
ASSERT VALUE message_count = 2 WHERE hl7_version = '2.6'
SELECT
    msh_12 AS hl7_version,
    COUNT(*) AS message_count
FROM {{zone_name}}.discover_demos.adt_feed
GROUP BY msh_12
ORDER BY msh_12;


-- ============================================================================
-- Query 6: Sending systems, cross-EHR analysis
-- ============================================================================
-- Groups messages by sending application and facility to show how many
-- distinct EHR systems feed this single unified HIE table. Each EHR
-- contributes exactly two messages (one admit-style and one later event).

ASSERT ROW_COUNT = 3
ASSERT VALUE messages = 2 WHERE sending_application = 'HARBORVIEW_EHR'
ASSERT VALUE messages = 2 WHERE sending_application = 'CERNER_PCC'
ASSERT VALUE messages = 2 WHERE sending_application = 'MEDITECH_ADT'
ASSERT VALUE sending_facility = 'EVERGREEN' WHERE sending_application = 'MEDITECH_ADT'
SELECT
    msh_3 AS sending_application,
    msh_4 AS sending_facility,
    COUNT(*) AS messages
FROM {{zone_name}}.discover_demos.adt_feed
GROUP BY msh_3, msh_4
ORDER BY msh_3;


-- ============================================================================
-- Query 7: Full message JSON, deep access via df_message_json
-- ============================================================================
-- Every row includes df_message_json containing the complete parsed HL7
-- message as JSON. This enables access to ANY segment/field beyond the MSH
-- header (PID patient identity, PV1 visit, EVN event) without a hand-written
-- materialized_paths config. df_message_id is a stable per-message hash.

ASSERT ROW_COUNT = 1
ASSERT VALUE row_count = 6
ASSERT VALUE populated = 6
SELECT
    COUNT(*)                                   AS row_count,
    COUNT(df_message_json)                     AS populated,
    COUNT(DISTINCT df_message_id)              AS distinct_message_ids
FROM {{zone_name}}.discover_demos.adt_feed;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total message volume, the
-- distinct-source-file count, the multi-version spread, and the admit count.
-- All checks should return PASS.

ASSERT ROW_COUNT = 4
ASSERT VALUE result = 'PASS' WHERE check_name = 'admit_count_3'
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count_6'
ASSERT VALUE result = 'PASS' WHERE check_name = 'multi_version_3'
ASSERT VALUE result = 'PASS' WHERE check_name = 'source_files_6'
SELECT check_name, result FROM (

    -- Check 1: Total message count = 6 (one per .hl7 file)
    SELECT 'message_count_6' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.discover_demos.adt_feed) = 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 6 distinct source files in df_file_name
    SELECT 'source_files_6' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.discover_demos.adt_feed) = 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Exactly 3 distinct HL7 versions (2.3, 2.5.1, 2.6)
    SELECT 'multi_version_3' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT msh_12) FROM {{zone_name}}.discover_demos.adt_feed) = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: 3 admit (A01) messages across all versions
    SELECT 'admit_count_3' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.discover_demos.adt_feed
                       WHERE msh_9 LIKE 'ADT^A01%') = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
