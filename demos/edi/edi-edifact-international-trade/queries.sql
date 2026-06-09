-- ============================================================================
-- EDI EDIFACT International Trade — Demo Queries
-- ============================================================================
-- Overview of how DeltaForge unifies EDIFACT and EANCOM messages from
-- shipping lines, customs authorities, airlines, and retail supply chains
-- into a single queryable table.
--
-- One table is available:
--   edifact_messages — UNB/UNH headers + full JSON for every message
--
-- Column reference (UNB interchange envelope):
--   UNB_1  = Syntax identifier (UNOA, UNOB, UNOC, UNOL, IATB, IATA)
--   UNB_2  = Interchange sender
--   UNB_3  = Interchange recipient
--   UNB_4  = Date/time of preparation
--   UNB_5  = Interchange control reference
--
-- Column reference (UNH message header):
--   UNH_1  = Message reference number
--   UNH_2  = Message type code (ORDERS, INVOIC, CUSCAR, etc.)
-- ============================================================================


-- ============================================================================
-- 1. All Messages — Header Overview
-- ============================================================================
-- Shows the UNB/UNH header of every EDIFACT message parsed from 22 source
-- files. Some files contain multiple messages (CONTRL has 2, multi_message
-- has 2), so total rows exceed the file count.

ASSERT ROW_COUNT >= 22
ASSERT VALUE syntax_id = 'UNOB' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE sender = 'SENDER1' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE msg_type = 'ORDERS' WHERE df_file_name = 'edifact_ORDERS_purchase_order.edi'
ASSERT VALUE syntax_id = 'IATB' WHERE df_file_name = 'edifact_wikipedia_example.edi'
ASSERT VALUE msg_type = 'PAORES' WHERE df_file_name = 'edifact_wikipedia_example.edi'
ASSERT VALUE msg_type = 'DESADV' WHERE df_file_name = 'eancom_DESADV_despatch_advice.edi'
SELECT
    df_file_name,
    unb_1 AS syntax_id,
    unh_2 AS msg_type,
    unh_1 AS msg_ref,
    unb_2 AS sender
FROM {{zone_name}}.edi_demos.edifact_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Message Type Distribution
-- ============================================================================
-- Groups messages by UNH_2 to show the diversity of EDIFACT message types.
-- Each type represents a different business process: ordering, invoicing,
-- transport, customs clearance, or acknowledgment.

ASSERT ROW_COUNT >= 10
ASSERT VALUE msg_count = 2 WHERE msg_type = 'CONTRL'
ASSERT VALUE msg_count = 1 WHERE msg_type = 'ORDERS'
ASSERT VALUE msg_count = 1 WHERE msg_type = 'PAORES'
SELECT
    unh_2 AS msg_type,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi_demos.edifact_messages
GROUP BY unh_2
ORDER BY msg_count DESC, unh_2;


-- ============================================================================
-- 3. Syntax Version Distribution
-- ============================================================================
-- Shows how many messages came from each UN/EDIFACT syntax identifier.
-- Demonstrates DeltaForge parsing six syntax variants in a single table
-- without version-specific configuration.

ASSERT ROW_COUNT >= 4
ASSERT VALUE msg_count >= 1 WHERE syntax_id = 'IATB'
ASSERT VALUE msg_count >= 1 WHERE syntax_id = 'UNOB'
ASSERT VALUE msg_count >= 1 WHERE syntax_id = 'UNOC'
SELECT
    unb_1 AS syntax_id,
    CASE unb_1
        WHEN 'UNOA' THEN 'Basic Latin (Level A)'
        WHEN 'UNOB' THEN 'Latin with lowercase (Level B)'
        WHEN 'UNOC' THEN 'Latin-1 extended (Level C)'
        WHEN 'UNOL' THEN 'Latin-2 extended (Level L)'
        WHEN 'IATB' THEN 'IATA variant B'
        WHEN 'IATA' THEN 'IATA variant A'
        ELSE unb_1
    END AS syntax_name,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi_demos.edifact_messages
GROUP BY unb_1
ORDER BY msg_count DESC;


-- ============================================================================
-- 4. Commerce vs Transport vs Customs — Domain Classification
-- ============================================================================
-- Categorizes each message into a business domain using CASE on the UNH_2
-- message type. Shows the breadth of international trade processes in a
-- single EDIFACT feed.

ASSERT ROW_COUNT >= 15
ASSERT VALUE domain = 'Commerce' WHERE msg_type = 'ORDERS'
ASSERT VALUE domain = 'Transport' WHERE msg_type = 'DESADV'
ASSERT VALUE domain = 'Acknowledgment' WHERE msg_type = 'CONTRL'
ASSERT VALUE domain = 'Other' WHERE msg_type = 'PAORES'
SELECT
    CASE
        WHEN unh_2 LIKE 'ORDERS%' OR unh_2 LIKE 'ORDRSP%' OR unh_2 LIKE 'INVOIC%'
             OR unh_2 LIKE 'PRICAT%' OR unh_2 LIKE 'QUOTES%'
            THEN 'Commerce'
        WHEN unh_2 LIKE 'IFCSUM%' OR unh_2 LIKE 'IFTSTA%' OR unh_2 LIKE 'IFTMIN%'
             OR unh_2 LIKE 'BAPLIE%' OR unh_2 LIKE 'DESADV%'
            THEN 'Transport'
        WHEN unh_2 LIKE 'CUSCAR%' OR unh_2 LIKE 'PAXLST%' OR unh_2 LIKE 'PNRGOV%'
            THEN 'Border'
        WHEN unh_2 LIKE 'APERAK%' OR unh_2 LIKE 'CONTRL%'
            THEN 'Acknowledgment'
        ELSE 'Other'
    END AS domain,
    unh_2 AS msg_type,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi_demos.edifact_messages
GROUP BY
    CASE
        WHEN unh_2 LIKE 'ORDERS%' OR unh_2 LIKE 'ORDRSP%' OR unh_2 LIKE 'INVOIC%'
             OR unh_2 LIKE 'PRICAT%' OR unh_2 LIKE 'QUOTES%'
            THEN 'Commerce'
        WHEN unh_2 LIKE 'IFCSUM%' OR unh_2 LIKE 'IFTSTA%' OR unh_2 LIKE 'IFTMIN%'
             OR unh_2 LIKE 'BAPLIE%' OR unh_2 LIKE 'DESADV%'
            THEN 'Transport'
        WHEN unh_2 LIKE 'CUSCAR%' OR unh_2 LIKE 'PAXLST%' OR unh_2 LIKE 'PNRGOV%'
            THEN 'Border'
        WHEN unh_2 LIKE 'APERAK%' OR unh_2 LIKE 'CONTRL%'
            THEN 'Acknowledgment'
        ELSE 'Other'
    END,
    unh_2
ORDER BY domain, msg_type;


-- ============================================================================
-- 5. EANCOM vs Pure EDIFACT — Standard Classification
-- ============================================================================
-- Classifies messages by whether they came from EANCOM files (GS1 retail
-- supply chain subset) or pure EDIFACT files.

ASSERT ROW_COUNT = 2
ASSERT VALUE file_count = 6 WHERE standard = 'EANCOM'
ASSERT VALUE file_count = 16 WHERE standard = 'EDIFACT'
ASSERT VALUE msg_count = 6 WHERE standard = 'EANCOM'
ASSERT VALUE msg_count = 18 WHERE standard = 'EDIFACT'
SELECT
    CASE
        WHEN df_file_name LIKE 'eancom%' THEN 'EANCOM'
        ELSE 'EDIFACT'
    END AS standard,
    COUNT(DISTINCT df_file_name) AS file_count,
    COUNT(*) AS msg_count
FROM {{zone_name}}.edi_demos.edifact_messages
GROUP BY
    CASE
        WHEN df_file_name LIKE 'eancom%' THEN 'EANCOM'
        ELSE 'EDIFACT'
    END
ORDER BY standard;


-- ============================================================================
-- 6. Full Transaction JSON — Deep Access via df_transaction_json
-- ============================================================================
-- Every row includes df_transaction_json containing the complete parsed
-- EDIFACT message as a JSON object. This enables access to ANY segment
-- and element without needing materialized_paths.

ASSERT ROW_COUNT = 3
SELECT
    df_file_name,
    unh_2 AS msg_type,
    df_transaction_json
FROM {{zone_name}}.edi_demos.edifact_messages
ORDER BY df_file_name
LIMIT 3;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Automated verification that the demo loaded correctly.

ASSERT ROW_COUNT = 4
ASSERT VALUE result = 'PASS' WHERE check_name = 'message_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'source_files_22'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
SELECT check_name, result FROM (

    SELECT 'message_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.edifact_messages) >= 22
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'source_files_22' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi_demos.edifact_messages) = 22
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'multi_message_type' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT unh_2) FROM {{zone_name}}.edi_demos.edifact_messages) >= 10
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.edifact_messages
                       WHERE df_transaction_json IS NOT NULL) >= 22
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
