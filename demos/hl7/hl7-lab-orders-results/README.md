# HL7 Lab Orders & Results — ORM/ORU Workflow

## Overview

Models the complete laboratory workflow using HL7 v2 ORM (Order) and ORU
(Observation Result) messages. Orders are placed for lab tests and radiology
procedures; results come back with individual observations, reference ranges,
and abnormal flags. DeltaForge reads the raw `.hl7` files and produces
structured, queryable tables — no custom parser needed.

## Clinical Scenario

A hospital integration engine processes lab orders from clinicians (ORM^O01)
and receives results from the laboratory information system (ORU^R01). The
messages come from different systems — EPIC EHR, Ritten, InterfaceWare, GHH
LAB — across HL7 v2.3 through v2.5.1. Two tables provide different views:
a compact order table (ORM files only) and a materialized table with key
observation fields extracted (all files, filterable by message type).

## HL7 Features Demonstrated

| Feature | How It's Used |
| ------- | ------------- |
| ORM order parsing | MSH header + full order details in df_message_json |
| ORU result parsing | Materialized OBX fields: value, units, reference range, flags |
| Value type handling | NM (numeric), ST (string), TX (text), SN (structured numeric) |
| Abnormal flags | OBX_8: H=High, L=Low, N=Normal (glucose 182 flagged High) |
| Escape decoding | `\X0D\` line breaks in radiology narrative automatically decoded |
| Multi-file glob | `orm*.hl7` and `oru*.hl7` patterns for selective table loading |
| Full JSON access | df_message_json enables access to all OBX segments (only first is materialized) |

## What This Demo Sets Up

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `hl7` | HL7-backed external tables |
| Table | `external.hl7.lab_orders` | Compact: MSH header + JSON for ORM messages only (3 rows) |
| Table | `external.hl7.lab_results` | Materialized: MSH + PID/OBR/OBX fields for all messages (8 rows, 5 ORU) |

## Data Files

| File | Type | Source | Version | Key Content |
| ---- | ---- | ------ | ------- | ----------- |
| `orm_o01_order.hl7` | ORM | EHR | 2.5.1 | CBC, BMP, urine culture (3 tests, STAT) |
| `orm_o01_ritten.hl7` | ORM | Ritten | 2.5 | Single glucose order |
| `orm_o01_interfaceware.hl7` | ORM | InterfaceWare | 2.3 | X-ray ankle with fracture diagnosis |
| `oru_r01_lab_result.hl7` | ORU | LAB | 2.5.1 | CMP: 14 analytes with reference ranges |
| `oru_r01_ritten.hl7` | ORU | Ritten | 2.5 | Simple positive test result |
| `oru_r01_interfaceware.hl7` | ORU | InterfaceWare | 2.4 | Chest X-ray radiology report + DICOM UID |
| `hl7v2.3_oru_r01_immunization.hl7` | ORU | LinkLogic | 2.3 | 9 vaccine records (MMR, Hep B, HIB) |
| `hl7v2.4_oru_r01_glucose.hl7` | ORU | GHH LAB | 2.4 | Glucose 182 mg/dL (HIGH, ref 70-105) |

## Known Verification Values

| Check | Expected |
| ----- | -------- |
| ORM message count (lab_orders) | 3 |
| Total messages (lab_results) | 8 (3 ORM + 5 ORU) |
| ORU messages (lab_results) | 5 (filter via `MSH_9 LIKE 'ORU%'`) |
| High glucose flag | OBX_8 = 'H' (182 mg/dL) |
| HL7 versions | v2.3, v2.4, v2.5, v2.5.1 |
| PID_5 populated | At least 1 ORU result has patient name |

## Sample Queries

```sql
-- ORU results overview (materialized table, filtered to results only)
SELECT MSH_3 AS system, PID_5 AS patient, OBX_3 AS test_id, OBX_5 AS value
FROM external.hl7.lab_results WHERE MSH_9 LIKE 'ORU%' ORDER BY df_file_name;

-- Abnormal results flagged
SELECT PID_5 AS patient, OBX_3 AS test, OBX_5 AS value, OBX_8 AS flag
FROM external.hl7.lab_results WHERE OBX_8 IS NOT NULL AND OBX_8 <> '' AND OBX_8 <> 'N';

-- Full order details via JSON (compact table)
SELECT df_file_name, MSH_9 AS message_type, df_message_json
FROM external.hl7.lab_orders WHERE df_file_name LIKE '%orm_o01_order%';

-- Full CMP result with all OBX segments via JSON
SELECT df_file_name, PID_5 AS patient, df_message_json
FROM external.hl7.lab_results WHERE df_file_name LIKE '%lab_result%';
```

## What This Tests

1. ORM^O01 order message parsing with MSH header extraction
2. ORU^R01 observation result parsing with materialized OBX fields
3. Materialized path extraction for PID, OBR, and OBX fields
4. Abnormal flag detection (OBX_8 = H for high glucose)
5. Multi-value-type handling (NM, ST, TX, SN)
6. HL7 escape sequences in narrative text (`\X0D\` line breaks)
7. Full JSON output for deep access to all OBX segments
8. Multi-file glob patterns for selective message loading
9. Cross-system lab interface unification (4+ LIS/EHR systems)
