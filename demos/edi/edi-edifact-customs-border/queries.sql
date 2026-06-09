-- ============================================================================
-- EDIFACT Customs & Border Control — Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge parses UN/EDIFACT messages from a port
-- authority customs system into queryable tables for border security, trade
-- compliance, and vessel operations.
--
-- Two tables are available:
--   customs_messages      — Compact view: UNB/UNH headers + full JSON
--   customs_materialized  — Enriched view: headers + BGM/TDT/NAD/LOC/CNI/GID/EQD/DOC
--
-- Column reference (always available — UNB interchange envelope):
--   UNB_1  = Syntax identifier (UNOA, IATA)
--   UNB_2  = Interchange sender (SID, LOCK, MSC, AM)
--   UNB_3  = Interchange recipient (RID, CBP-ACE-TEST, ECA, MXPNRGOV)
--   UNB_4  = Date/time of preparation
--   UNB_5  = Interchange control reference
--
-- Column reference (always available — UNH message header):
--   UNH_1  = Message reference number
--   UNH_2  = Message type (CUSCAR, PAXLST, PNRGOV, BAPLIE — or NULL if malformed)
--
-- NOTE: The D95B CUSCAR file has a malformed UNH segment where the message
-- type field is empty. Queries use COALESCE or df_file_name LIKE patterns
-- to handle this gracefully.
--
-- Materialized columns (customs_materialized table only):
--   BGM_1  = Document name code      BGM_2  = Document number
--   TDT_1  = Transport stage         TDT_2  = Conveyance reference
--   NAD_1  = Party qualifier         NAD_2  = Party identification
--   LOC_1  = Location qualifier      LOC_2  = Location identification
--   CNI_1  = Consignment item no.    CNI_2  = Consignment reference
--   GID_1  = Goods item number       GID_2  = Package type
--   EQD_1  = Equipment qualifier     EQD_2  = Equipment identification
--   DOC_1  = Document name code      DOC_2  = Document identifier
-- ============================================================================


-- ============================================================================
-- 1. Message Overview — All 5 EDIFACT Messages
-- ============================================================================
-- Shows the UNB/UNH header of every EDIFACT message parsed from the 5
-- source files. Each row is one message from one file.
--
-- What you'll see:
--   - df_file_name:  The source .edi file this row came from
--   - syntax_id:     UNB_1 — syntax identifier (UNOA or IATA)
--   - sender:        UNB_2 — interchange sender
--   - recipient:     UNB_3 — interchange recipient
--   - msg_type:      UNH_2 — message type (CUSCAR, PAXLST, PNRGOV, BAPLIE, or NULL)

ASSERT ROW_COUNT = 5
ASSERT VALUE syntax_id = 'UNOA' WHERE df_file_name = 'edifact_BAPLIE_bayplan_stowage.edi'
ASSERT VALUE sender = 'AM' WHERE df_file_name = 'edifact_PNRGOV_passenger_data.edi'
ASSERT VALUE syntax_id = 'IATA' WHERE df_file_name = 'edifact_PNRGOV_passenger_data.edi'
ASSERT VALUE msg_type = 'PAXLST' WHERE df_file_name = 'edifact_PAXLST_passenger_list.edi'
ASSERT VALUE msg_type = 'BAPLIE' WHERE df_file_name = 'edifact_BAPLIE_bayplan_stowage.edi'
SELECT
    df_file_name,
    unb_1 AS syntax_id,
    unb_2 AS sender,
    unb_3 AS recipient,
    unh_2 AS msg_type
FROM {{zone_name}}.edi_demos.customs_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Cargo Manifests (CUSCAR) — Consignment Details
-- ============================================================================
-- Filters for CUSCAR messages from the materialized table. The D95B file
-- has a malformed UNH (empty message type) so we also match by file name.
-- Shows BGM document codes, consignment references (CNI), and goods items.
--
-- What you'll see:
--   - df_file_name:  Source file name
--   - doc_code:      BGM_1 — document name code (85 = Customs declaration)
--   - doc_number:    BGM_2 — document/manifest reference number
--   - cni_seq:       CNI_1 — consignment item sequence number
--   - cni_ref:       CNI_2 — consignment reference (bill of lading number)
--   - goods_item:    GID_1 — goods item sequence number
--   - goods_pkg:     GID_2 — package type/count description

ASSERT ROW_COUNT = 2
ASSERT VALUE doc_code = '85:::STANDARD' WHERE df_file_name = 'edifact_CUSCAR_cargo_report.edi'
ASSERT VALUE doc_number = 'LOCKKH04112206' WHERE df_file_name = 'edifact_CUSCAR_cargo_report.edi'
ASSERT VALUE cni_seq = '3741' WHERE df_file_name = 'edifact_CUSCAR_cargo_report.edi'
ASSERT VALUE doc_number = '201701191AB652' WHERE df_file_name = 'edifact_D95B_CUSCAR_customs_cargo.edi'
ASSERT VALUE cni_ref = 'MSCUEK569969' WHERE df_file_name = 'edifact_D95B_CUSCAR_customs_cargo.edi'
SELECT
    df_file_name,
    bgm_1 AS doc_code,
    bgm_2 AS doc_number,
    cni_1 AS cni_seq,
    cni_2 AS cni_ref,
    gid_1 AS goods_item,
    gid_2 AS goods_pkg
FROM {{zone_name}}.edi_demos.customs_materialized
WHERE unh_2 = 'CUSCAR' OR df_file_name LIKE '%D95B%CUSCAR%'
ORDER BY df_file_name;


-- ============================================================================
-- 3. Transport Details — Vessel & Flight Identification
-- ============================================================================
-- Extracts TDT (Transport Information) segments for identifying vessels and
-- flights. TDT_1 is the transport stage qualifier (20 = main carriage) and
-- TDT_2 is the conveyance reference (voyage number or flight number).
--
-- What you'll see:
--   - df_file_name:      Source file name
--   - transport_stage:   TDT_1 — transport stage qualifier (20 = main carriage)
--   - conveyance_ref:    TDT_2 — voyage/flight number (123W45, AB652A, or NULL)
--   - msg_type:          UNH_2 — message type (or COALESCE for malformed UNH)
--
-- 4 messages have TDT segments (all except PNRGOV)

ASSERT ROW_COUNT = 4
ASSERT VALUE conveyance_ref = '123W45' WHERE df_file_name = 'edifact_BAPLIE_bayplan_stowage.edi'
ASSERT VALUE conveyance_ref = 'AB652A' WHERE df_file_name = 'edifact_D95B_CUSCAR_customs_cargo.edi'
SELECT
    df_file_name,
    tdt_1 AS transport_stage,
    tdt_2 AS conveyance_ref,
    COALESCE(unh_2, 'CUSCAR') AS msg_type
FROM {{zone_name}}.edi_demos.customs_materialized
WHERE tdt_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 4. Location Analysis — LOC Qualifiers Across Messages
-- ============================================================================
-- Groups messages by their LOC_1 (location qualifier) to show the variety
-- of location types referenced across customs and border messages. Each
-- qualifier identifies a different role for the location in the transaction.
--
-- What you'll see:
--   - loc_qualifier:  LOC_1 — location qualifier code
--   - qualifier_name: Human-readable description of the qualifier
--   - msg_count:      Number of messages with that qualifier
--
-- 4 distinct qualifiers from 4 messages (PNRGOV has no LOC):
--   9   = Place of loading       (CUSCAR cargo report)
--   28  = Place of arrival       (D95B CUSCAR)
--   91  = Transport means        (PAXLST)
--   147 = Stowage cell           (BAPLIE)

ASSERT ROW_COUNT = 4
ASSERT VALUE qualifier_name = 'Place of loading' WHERE loc_qualifier = '9'
ASSERT VALUE qualifier_name = 'Place of arrival' WHERE loc_qualifier = '28'
ASSERT VALUE qualifier_name = 'Transport means' WHERE loc_qualifier = '91'
ASSERT VALUE qualifier_name = 'Stowage cell' WHERE loc_qualifier = '147'
SELECT
    loc_1 AS loc_qualifier,
    CASE loc_1
        WHEN '9'   THEN 'Place of loading'
        WHEN '28'  THEN 'Place of arrival'
        WHEN '91'  THEN 'Transport means'
        WHEN '147' THEN 'Stowage cell'
        ELSE loc_1
    END AS qualifier_name,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi_demos.customs_materialized
WHERE loc_1 IS NOT NULL
GROUP BY loc_1
ORDER BY loc_1;


-- ============================================================================
-- 5. Equipment & Stowage — Container Details
-- ============================================================================
-- Extracts EQD (Equipment Detail) segments for container and equipment
-- information. Only 2 messages have EQD: the CUSCAR cargo report (bill of
-- lading equipment) and the BAPLIE (container stowage).
--
-- What you'll see:
--   - df_file_name:    Source file name
--   - equip_qualifier: EQD_1 — equipment qualifier (CN=Container, BI=Bill of lading)
--   - equip_id:        EQD_2 — equipment identification number
--   - msg_type:        Message type (COALESCE for malformed UNH)

ASSERT ROW_COUNT = 2
ASSERT VALUE equip_qualifier = 'CN' WHERE df_file_name = 'edifact_BAPLIE_bayplan_stowage.edi'
ASSERT VALUE equip_id = 'SUDU1234569:6346:5' WHERE df_file_name = 'edifact_BAPLIE_bayplan_stowage.edi'
ASSERT VALUE equip_qualifier = 'BI' WHERE df_file_name = 'edifact_CUSCAR_cargo_report.edi'
ASSERT VALUE equip_id = '10000325:109' WHERE df_file_name = 'edifact_CUSCAR_cargo_report.edi'
SELECT
    df_file_name,
    eqd_1 AS equip_qualifier,
    eqd_2 AS equip_id,
    COALESCE(unh_2, 'CUSCAR') AS msg_type
FROM {{zone_name}}.edi_demos.customs_materialized
WHERE eqd_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 6. Passenger Records — Travel Document Verification
-- ============================================================================
-- Extracts DOC (Document/Message Details) segments for travel document
-- and cargo document verification. PAXLST has passport data (DOC 39)
-- and CUSCAR has cargo document references (DOC 714).
--
-- What you'll see:
--   - df_file_name:  Source file name
--   - msg_type:      UNH_2 — PAXLST or CUSCAR
--   - doc_code:      DOC_1 — document name code (39 = Passport, 714:::61 = cargo doc)
--   - doc_number:    DOC_2 — passport/document number
--   - bgm_code:      BGM_1 — document name code from BGM segment
--   - bgm_ref:       BGM_2 — manifest reference number

ASSERT ROW_COUNT = 2
ASSERT VALUE doc_code = '39' WHERE df_file_name = 'edifact_PAXLST_passenger_list.edi'
ASSERT VALUE doc_number = '15504141' WHERE df_file_name = 'edifact_PAXLST_passenger_list.edi'
ASSERT VALUE bgm_code = '10' WHERE df_file_name = 'edifact_PAXLST_passenger_list.edi'
ASSERT VALUE bgm_ref = 'LOCKKH04103101' WHERE df_file_name = 'edifact_PAXLST_passenger_list.edi'
SELECT
    df_file_name,
    unh_2 AS msg_type,
    doc_1 AS doc_code,
    doc_2 AS doc_number,
    bgm_1 AS bgm_code,
    bgm_2 AS bgm_ref
FROM {{zone_name}}.edi_demos.customs_materialized
WHERE doc_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 7. Party Classification — NAD Qualifiers Across Messages
-- ============================================================================
-- Groups messages by their NAD_1 (party qualifier) to show the different
-- party roles across customs and border messages. Each qualifier identifies
-- a different role in the supply chain or border process.
--
-- What you'll see:
--   - nad_qualifier:   NAD_1 — party function qualifier code
--   - qualifier_name:  Human-readable description
--   - msg_count:       Number of messages with that party qualifier
--
-- NAD is a repeating segment — materialized nad_1 reflects the FIRST
-- occurrence per message. 4 distinct first-NAD qualifiers from 4 messages
-- (PNRGOV has no NAD):
--   CA = Carrier agent                 (CUSCAR cargo report)
--   MS = Document/message issuer       (D95B CUSCAR)
--   VW = Vessel master                 (PAXLST)
--   WZ = Consignment routing party     (BAPLIE)

ASSERT ROW_COUNT = 4
ASSERT VALUE qualifier_name = 'Vessel master' WHERE nad_qualifier = 'VW'
ASSERT VALUE qualifier_name = 'Carrier agent' WHERE nad_qualifier = 'CA'
ASSERT VALUE qualifier_name = 'Consignment routing party' WHERE nad_qualifier = 'WZ'
ASSERT VALUE qualifier_name = 'Document/message issuer' WHERE nad_qualifier = 'MS'
SELECT
    nad_1 AS nad_qualifier,
    CASE nad_1
        WHEN 'CA' THEN 'Carrier agent'
        WHEN 'MS' THEN 'Document/message issuer'
        WHEN 'VW' THEN 'Vessel master'
        WHEN 'WZ' THEN 'Consignment routing party'
        ELSE nad_1
    END AS qualifier_name,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi_demos.customs_materialized
WHERE nad_1 IS NOT NULL
GROUP BY nad_1
ORDER BY nad_1;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, distinct files, CUSCAR detection,
-- and JSON population. All checks should return PASS.

ASSERT ROW_COUNT = 4
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'source_files_5'
ASSERT VALUE result = 'PASS' WHERE check_name = 'cargo_reports'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
SELECT check_name, result FROM (

    -- Check 1: Total message count = 5 (one per .edi file)
    SELECT 'message_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.customs_messages) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 5 distinct source files in df_file_name
    SELECT 'source_files_5' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi_demos.customs_messages) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: At least 2 CUSCAR cargo reports (detected by file name pattern)
    SELECT 'cargo_reports' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.customs_messages
                       WHERE df_file_name LIKE '%CUSCAR%') >= 2
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: df_transaction_json is populated for all 5 messages
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.customs_messages
                       WHERE df_transaction_json IS NOT NULL) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
