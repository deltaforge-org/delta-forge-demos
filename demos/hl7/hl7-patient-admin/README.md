# HL7 Patient Administration — ADT Lifecycle

## Overview

Demonstrates how DeltaForge ingests HL7 v2 ADT (Admit-Discharge-Transfer)
messages from multiple EHR systems and HL7 versions. Eight real-world messages
cover the full patient lifecycle — admission (A01), demographics update (A08),
and discharge (A03). Two external tables provide different views: a compact
header-only table and a materialized table with key patient fields extracted.

## Clinical Scenario

A hospital integration engine receives ADT messages from six different EHR
systems: EPIC, Folio3/MCM, Ritten, AWS, MegaReg, and Contoso/Azure. Each
system uses a different HL7 version (v2.3 through v2.6) and includes
different segments. DeltaForge reads all `.hl7` files and produces
structured tables — no custom parser or ETL pipeline required.

## HL7 Features Demonstrated

| Feature | How It's Used |
| ------- | ------------- |
| Multi-version parsing | v2.3, v2.3.1, v2.5, v2.5.1, v2.6 in one table |
| MSH header extraction | Sending app, facility, message type, version — always available |
| Materialized paths | PID_5 (name), PID_7 (DOB), PV1_2 (class) extracted as columns |
| Full JSON output | df_message_json contains complete message for deep access |
| Escape decoding | `\T\`, `\S\`, `\E\` automatically decoded |
| Z-segments | ZMP (custom Medicare) in Azure/Contoso message preserved in JSON |
| File metadata | df_file_name traces each row to its source file |

## What This Demo Sets Up

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `hl7` | HL7-backed external tables |
| Table | `external.hl7.adt_messages` | Compact view: MSH header + JSON (8 rows) |
| Table | `external.hl7.adt_materialized` | Materialized: MSH header + PID/PV1/EVN fields (8 rows) |

## Data Files

| File | Event | Source System | HL7 Version | Key Content |
| ---- | ----- | ------------- | ----------- | ----------- |
| `adt_a01_admission.hl7` | A01 | EPIC | 2.5.1 | Full admission: allergies, diagnosis, insurance |
| `adt_a08_update.hl7` | A08 | REGISTRATION | 2.5.1 | Address/contact update |
| `adt_a03_discharge.hl7` | A03 | IRIS SANTER | 2.5 | Discharge with procedure code |
| `adt_a01_folio3.hl7` | A01 | Folio3/MCM | 2.3.1 | Legacy format, surgical ward |
| `adt_a01_ritten.hl7` | A01 | Ritten | 2.5 | Minimal outpatient registration |
| `hl7v2.3_adt_a01_aws.hl7` | A01 | AWS | 2.3 | Compact admission with next of kin |
| `hl7v2.5_adt_a01_megareg.hl7` | A01 | MegaReg | 2.5 | Body measurements, dual address |
| `hl7v2.6_adt_a01_azure.hl7` | A01 | Contoso | 2.6 | Modern: Z-segments, multiple ID systems |

## Sample Queries

```sql
-- All patients with their demographics (materialized table)
SELECT PID_5 AS patient_name, PID_7 AS dob, PID_8 AS gender, PV1_2 AS patient_class
FROM external.hl7.adt_materialized ORDER BY PID_5;

-- Which EHR systems sent messages (compact table)
SELECT MSH_3 AS app, MSH_12 AS version, COUNT(*) AS messages
FROM external.hl7.adt_messages GROUP BY MSH_3, MSH_12;

-- Inpatient vs outpatient
SELECT PV1_2 AS patient_class, COUNT(*) AS count
FROM external.hl7.adt_materialized GROUP BY PV1_2;

-- Full message JSON for deep access
SELECT df_file_name, MSH_9 AS message_type, df_message_json
FROM external.hl7.adt_messages LIMIT 3;
```

## What This Tests

1. HL7 v2 ADT message parsing across versions 2.3 through 2.6
2. Multi-system message unification in a single table
3. MSH header field extraction (always available without materialized_paths)
4. Materialized path extraction for PID, PV1, and EVN fields
5. Full message JSON output for deep segment access
6. HL7 escape sequence decoding (`\T\`, `\S\`, `\E\`)
7. File metadata injection (df_file_name traceability)
8. Mixed patient class handling (inpatient, outpatient, emergency)
9. Cross-EHR field mapping differences (some fields NULL per system)
