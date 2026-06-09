-- ============================================================================
-- EDI Transportation & Logistics -- Demo Queries
-- ============================================================================
-- Queries showcasing how DeltaForge unifies X12 EDI transportation
-- documents across modes (motor, rail, warehouse) and trading partners
-- into queryable tables.
--
-- Two tables are available:
--   logistics_messages      -- Compact view: ISA/GS/ST headers + full JSON
--   logistics_materialized  -- Enriched view: headers + B2/B3/B10/N1/L3 columns
--
-- Column reference (always available -- X12 default fields):
--   ISA_6  = Interchange Sender ID      ISA_8  = Interchange Receiver ID
--   ISA_12 = Interchange Control Version GS_1   = Functional Identifier Code
--   GS_8   = Version/Release/Industry ID ST_1   = Transaction Set ID Code
--   ST_2   = Transaction Set Control Number
--   df_transaction_json = Full transaction as JSON
--   df_transaction_id   = Unique transaction hash
--
-- Materialized columns (logistics_materialized table only):
--   B2_2  = SCAC (carrier code)         B2_4  = Shipment ID (load tender)
--   B3_2  = Invoice number              B3_3  = Shipment ID (freight invoice)
--   B10_1 = Reference ID (shipment)     B10_2 = BOL number (shipment status)
--   N1_1  = Entity code (SH/CN/BT)     N1_2  = Party name
--   L3_1  = Weight                      L3_5  = Total charges
-- ============================================================================


-- ============================================================================
-- 1. All Transactions -- Header Overview
-- ============================================================================
-- This query reads from the compact table (logistics_messages) to show the
-- ISA/GS/ST header of every EDI transaction. Each row is one transaction
-- from one .edi file.
--
-- What you'll see:
--   - df_file_name:  The source .edi file this row came from
--   - sender:        ISA_6 Interchange Sender ID (e.g. MGCTLYST, SCAC, SENDER1)
--   - receiver:      ISA_8 Interchange Receiver ID (e.g. SCAC, MGCTLYST, RECEIVER1)
--   - transaction:   ST_1 Transaction Set ID Code (204, 210, 214, 404, 820, 832, 945, 990)
--   - func_group:    GS_1 Functional Identifier Code (SM, IM, IN, QM, RA, SC, GF)
--   - version:       GS_8 Version/Release code (004010, 004030, 005010)

ASSERT ROW_COUNT = 12
ASSERT VALUE transaction = '204' WHERE df_file_name = 'x12_204_motor_carrier_load_tender.edi'
ASSERT VALUE sender = 'MGCTLYST' WHERE df_file_name = 'x12_204_motor_carrier_load_tender.edi'
ASSERT VALUE func_group = 'SM' WHERE df_file_name = 'x12_204_motor_carrier_load_tender.edi'
ASSERT VALUE transaction = '990' WHERE df_file_name = 'x12_990_load_tender_response.edi'
ASSERT VALUE func_group = 'GF' WHERE df_file_name = 'x12_990_load_tender_response.edi'
ASSERT VALUE transaction = '820' WHERE df_file_name = 'x12_820_payment_order.edi'
ASSERT VALUE sender = 'TPTESTUS00' WHERE df_file_name = 'x12_820_payment_order.edi'
ASSERT VALUE version = '005010' WHERE df_file_name = 'x12_820_payment_order.edi'
SELECT
    df_file_name,
    isa_6 AS sender,
    isa_8 AS receiver,
    st_1  AS transaction,
    gs_1  AS func_group,
    gs_8  AS version
FROM {{zone_name}}.edi_demos.logistics_messages
ORDER BY df_file_name;


-- ============================================================================
-- 2. Transaction Type Distribution
-- ============================================================================
-- Groups transactions by their X12 Transaction Set ID (ST_1) to show how
-- many of each document type are in the dataset.
--
-- What you'll see:
--   - transaction_type: X12 code -- 204 (Load Tender), 210 (Freight Invoice),
--                       214 (Shipment Status), 404 (Rail Shipment),
--                       820 (Payment Order), 832 (Price Catalog),
--                       945 (Warehouse Advice), 990 (Tender Response)
--   - doc_count:        How many transactions of each type
--
-- 8 distinct types -- 204=1, 210=2, 214=3, 404=1, 820=1,
--           832=2, 945=1, 990=1 (total 12)

ASSERT ROW_COUNT = 8
ASSERT VALUE doc_count = 1 WHERE transaction_type = '204'
ASSERT VALUE doc_count = 2 WHERE transaction_type = '210'
ASSERT VALUE doc_count = 3 WHERE transaction_type = '214'
ASSERT VALUE doc_count = 1 WHERE transaction_type = '404'
ASSERT VALUE doc_count = 1 WHERE transaction_type = '820'
ASSERT VALUE doc_count = 2 WHERE transaction_type = '832'
ASSERT VALUE doc_count = 1 WHERE transaction_type = '945'
ASSERT VALUE doc_count = 1 WHERE transaction_type = '990'
SELECT
    st_1  AS transaction_type,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi_demos.logistics_messages
GROUP BY st_1
ORDER BY st_1;


-- ============================================================================
-- 3. Functional Group Codes
-- ============================================================================
-- Groups transactions by GS_1 (Functional Identifier Code). This code
-- indicates the business purpose of the functional group:
--   SM = Motor Carrier Load Tender     IM = Motor Carrier Invoice
--   IN = Invoice (general)             QM = Transportation Status
--   RA = Remittance Advice             SC = Price/Sales Catalog
--   GF = Response to a Load Tender
--
-- What you'll see:
--   - func_group:   GS_1 code
--   - doc_count:    Number of transactions using that code

ASSERT ROW_COUNT = 7
ASSERT VALUE doc_count = 5 WHERE func_group = 'IN'
ASSERT VALUE doc_count = 2 WHERE func_group = 'QM'
ASSERT VALUE doc_count = 1 WHERE func_group = 'SM'
ASSERT VALUE doc_count = 1 WHERE func_group = 'IM'
ASSERT VALUE doc_count = 1 WHERE func_group = 'GF'
ASSERT VALUE doc_count = 1 WHERE func_group = 'RA'
ASSERT VALUE doc_count = 1 WHERE func_group = 'SC'
SELECT
    gs_1  AS func_group,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi_demos.logistics_messages
GROUP BY gs_1
ORDER BY gs_1;


-- ============================================================================
-- 4. X12 Version Distribution
-- ============================================================================
-- Groups by ISA_12 (Interchange Control Version Number) to show which
-- X12 standard versions are represented. The dataset spans five versions
-- demonstrating DeltaForge parsing multiple X12 versions in a single table.
--
-- What you'll see:
--   - x12_version:   ISA_12 version code
--   - doc_count:     Number of transactions using that version
--
-- 5 distinct versions --
--   00204=5  (210 edifabric, 214 edifabric, 404, 832 edifabric, 945)
--   00400=1  (990)
--   00401=3  (204, 210 freight, 832 sales)
--   00403=2  (214 shipment, 214 transportation)
--   00501=1  (820)
--   Total = 12

ASSERT ROW_COUNT = 5
ASSERT VALUE doc_count = 5 WHERE x12_version = '00204'
ASSERT VALUE doc_count = 1 WHERE x12_version = '00400'
ASSERT VALUE doc_count = 3 WHERE x12_version = '00401'
ASSERT VALUE doc_count = 2 WHERE x12_version = '00403'
ASSERT VALUE doc_count = 1 WHERE x12_version = '00501'
SELECT
    isa_12 AS x12_version,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi_demos.logistics_messages
GROUP BY isa_12
ORDER BY isa_12;


-- ============================================================================
-- 5. Shipment Status Messages (214)
-- ============================================================================
-- Filters to only X12 214 (Transportation Carrier Shipment Status)
-- transactions. These are the tracking/status update messages that carriers
-- send to shippers about in-transit shipments.
--
-- What you'll see:
--   - df_file_name:  Source file (3 different 214 files)
--   - sender:        ISA_6 -- who sent the status update
--   - receiver:      ISA_8 -- who received it
--   - control_num:   ST_2 -- transaction set control number
--   - version:       GS_8 -- X12 version used

ASSERT ROW_COUNT = 3
ASSERT VALUE sender = 'SCAC' WHERE df_file_name = 'x12_214_shipment_status.edi'
ASSERT VALUE receiver = 'MGCTLYST' WHERE df_file_name = 'x12_214_shipment_status.edi'
ASSERT VALUE version = '004030' WHERE df_file_name = 'x12_214_shipment_status.edi'
ASSERT VALUE sender = 'SENDER1' WHERE df_file_name = 'x12_214_shipment_status_edifabric.edi'
ASSERT VALUE version = '004010' WHERE df_file_name = 'x12_214_shipment_status_edifabric.edi'
SELECT
    df_file_name,
    isa_6  AS sender,
    isa_8  AS receiver,
    st_2   AS control_num,
    gs_8   AS version
FROM {{zone_name}}.edi_demos.logistics_messages
WHERE st_1 = '214'
ORDER BY df_file_name;


-- ============================================================================
-- 6. Trading Partner Analysis
-- ============================================================================
-- Groups by sender (ISA_6) and receiver (ISA_8) to reveal the trading
-- partner relationships in this dataset. Each unique sender/receiver pair
-- represents an EDI partnership.
--
-- What you'll see:
--   - sender:    ISA_6 Interchange Sender ID (trimmed by X12 parser)
--   - receiver:  ISA_8 Interchange Receiver ID
--   - doc_count: How many transactions flow between this pair
--
-- Several distinct pairs including:
--   MGCTLYST -> SCAC (1: load tender 204)
--   SCAC -> MGCTLYST (4: 210 freight, 214 shipment, 214 transportation, 990)
--   SENDER1 -> RECEIVER1 (5: 210 edifabric, 214 edifabric, 404, 832 edifabric, 945)
--   TPTESTUS00 -> TESTCOMP (1: 820)
--   999999999 -> 6309246701 (1: 832 sales)

ASSERT ROW_COUNT = 5
ASSERT VALUE doc_count = 5 WHERE sender = 'SENDER1'
ASSERT VALUE doc_count = 4 WHERE sender = 'SCAC'
ASSERT VALUE doc_count = 1 WHERE sender = 'MGCTLYST'
ASSERT VALUE doc_count = 1 WHERE sender = 'TPTESTUS00'
ASSERT VALUE doc_count = 1 WHERE sender = '999999999'
SELECT
    isa_6  AS sender,
    isa_8  AS receiver,
    COUNT(*) AS doc_count
FROM {{zone_name}}.edi_demos.logistics_messages
GROUP BY isa_6, isa_8
ORDER BY doc_count DESC;


-- ============================================================================
-- 7. Materialized Logistics Fields -- Shipment Status Details
-- ============================================================================
-- Reads from the materialized table where B10/N1/L3 fields have been
-- extracted as first-class columns. Filters to rows where B10_1 (reference
-- identification / shipment ID) is populated -- these are the 214 Shipment
-- Status transactions that contain the B10 segment.
--
-- What you'll see:
--   - df_file_name:  Source .edi file
--   - shipment_id:   B10_1 -- reference identification for tracking
--   - bol_number:    B10_2 -- bill of lading number
--   - party_code:    N1_1 -- entity identifier (SH=shipper, CN=consignee, BT=bill-to)
--   - party_name:    N1_2 -- trading partner company name

ASSERT ROW_COUNT = 3
ASSERT VALUE shipment_id = '1751807' WHERE df_file_name = 'x12_214_shipment_status.edi'
ASSERT VALUE bol_number = '75027674' WHERE df_file_name = 'x12_214_shipment_status.edi'
ASSERT VALUE party_code = 'SH' WHERE df_file_name = 'x12_214_shipment_status.edi'
ASSERT VALUE party_name = 'CATALYST PAPER (USA) INC' WHERE df_file_name = 'x12_214_shipment_status.edi'
ASSERT VALUE shipment_id = '123456' WHERE df_file_name = 'x12_214_shipment_status_edifabric.edi'
SELECT
    df_file_name,
    b10_1 AS shipment_id,
    b10_2 AS bol_number,
    n1_1  AS party_code,
    n1_2  AS party_name
FROM {{zone_name}}.edi_demos.logistics_materialized
WHERE b10_1 IS NOT NULL
ORDER BY df_file_name;


-- ============================================================================
-- 8. Full Transaction JSON -- Deep Access
-- ============================================================================
-- Every row includes df_transaction_json containing the complete parsed
-- X12 transaction as a JSON object of segments. This enables access to ANY
-- segment/field -- including segments not materialized (AT7 status details,
-- BPR payment info, W12 warehouse items, LIN line items, etc.).
--
-- What you'll see:
--   - df_file_name:         Source file name
--   - transaction:          ST_1 transaction set type
--   - df_transaction_json:  Full transaction as JSON (click to expand in the UI)
--
-- Tip: The JSON contains every segment with its fields. Use JSON functions
-- for deep access without needing materialized_paths.

ASSERT ROW_COUNT = 3
ASSERT VALUE df_transaction_json IS NOT NULL WHERE df_file_name = 'x12_204_motor_carrier_load_tender.edi'
ASSERT VALUE df_transaction_json IS NOT NULL WHERE df_file_name = 'x12_210_freight_invoice.edi'
ASSERT VALUE df_transaction_json IS NOT NULL WHERE df_file_name = 'x12_210_freight_invoice_edifabric.edi'
SELECT
    df_file_name,
    st_1 AS transaction,
    df_transaction_json
FROM {{zone_name}}.edi_demos.logistics_messages
ORDER BY df_file_name
LIMIT 3;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: all 12 transactions loaded, all 8 types present,
-- materialized fields populated, and df_transaction_json non-null for all rows.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'transaction_count_12'
ASSERT VALUE result = 'PASS' WHERE check_name = 'source_files_12'
ASSERT VALUE result = 'PASS' WHERE check_name = 'json_populated'
SELECT check_name, result FROM (

    -- Check 1: Total transaction count = 12 (one per .edi file)
    SELECT 'transaction_count_12' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.logistics_messages) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 12 distinct source files in df_file_name
    SELECT 'source_files_12' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.edi_demos.logistics_messages) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: At least 6 distinct transaction types (actual: 8 -- 204,210,214,404,820,832,945,990)
    SELECT 'transaction_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT st_1) FROM {{zone_name}}.edi_demos.logistics_messages) >= 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Materialized table also has 12 rows (same files, different columns)
    SELECT 'materialized_count_12' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.logistics_materialized) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: B10_1 (shipment reference ID) is populated in at least some rows
    SELECT 'b10_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.logistics_materialized
                       WHERE b10_1 IS NOT NULL AND b10_1 <> '') > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: df_transaction_json is populated for all 12 transactions
    SELECT 'json_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.edi_demos.logistics_messages
                       WHERE df_transaction_json IS NOT NULL) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
