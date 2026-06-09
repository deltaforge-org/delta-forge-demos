# HL7 Clinical Workflows — Documents, Scheduling & Edge Cases

## Overview

Demonstrates how DeltaForge handles HL7 v2 clinical workflow messages
beyond the core ADT and lab types. Includes MDM (Medical Document Management)
with a full History & Physical narrative, SIU (Scheduling) with appointment
booking from two different systems, and a comprehensive edge-case file testing
parser robustness with escape sequences, empty fields, and special characters.

## Clinical Scenario

A hospital's clinical systems generate diverse HL7 message types. The
documentation system (MDM) produces authenticated clinical notes. The
scheduling system (SIU) books and confirms appointments across clinics.
Meanwhile, the integration engine must handle messages with unusual formatting
— escape sequences, empty fields, and special characters. DeltaForge reads
all of these into structured tables without custom parsing logic.

## HL7 Features Demonstrated

| Feature | How It's Used |
| ------- | ------------- |
| MDM document parsing | TXA metadata materialized + OBX narrative in df_message_json |
| SIU scheduling | SCH fields materialized (appointment ID, reason, duration, status) |
| Escape sequences | `\F\` `\T\` `\S\` `\R\` `\E\` `\X0D\` automatically decoded |
| Empty field handling | OBX with no value parsed cleanly |
| Multi-line text | `\X0D\` carriage returns in clinical narratives |
| Full JSON access | df_message_json for deep access to TXA, SCH, AIS/AIG/AIL/AIP, OBX |
| Mixed message types | MDM, SIU, and ADT edge cases unified in one table |

## What This Demo Sets Up

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `hl7` | HL7-backed external tables |
| Table | `external.hl7.clinical_messages` | Compact: MSH header + JSON for all 4 messages |
| Table | `external.hl7.clinical_materialized` | Materialized: PID/TXA/SCH/OBX fields extracted (4 rows) |

## Data Files

| File | Type | Version | Key Content |
| ---- | ---- | ------- | ----------- |
| `mdm_t02_document.hl7` | MDM^T02 | 2.5.1 | History & Physical: chest pain, acute coronary syndrome |
| `siu_s12_schedule.hl7` | SIU^S12 | 2.5.1 | 30-min cardiology follow-up, Dr. Johnson, Clinic 001 |
| `hl7v2.3_siu_s12_scheduling.hl7` | SIU^S12 | 2.3 | 60-min office visit, dual providers, MESA system |
| `edge_cases.hl7` | ADT^A01 | 2.5.1 | All HL7 escape sequences, empty OBX, special chars |

## H&P Document Structure

The MDM message contains a complete History & Physical with these sections
(accessible via df_message_json):

| OBX # | Section Code | Content |
| ----- | ------------ | ------- |
| 1 | HP-CHIEF | Chief Complaint: Chest pain and shortness of breath |
| 2 | HP-HPI | HPI: 52yo male, acute chest pain radiating to left arm |
| 3 | HP-PMH | PMH: Hypertension, Hyperlipidemia, Type 2 Diabetes |
| 4 | HP-MEDS | Medications: Lisinopril, Atorvastatin, Metformin, Aspirin |
| 5 | HP-EXAM | Exam: VS, CV, Lungs, Extremities findings |
| 6 | HP-ASSESS | Assessment: Acute coronary syndrome, rule out STEMI |
| 7 | HP-PLAN | Plan: ECG, Troponin, medications, cardiology consult |

## Sample Queries

```sql
-- MDM document metadata (materialized table)
SELECT PID_5 AS patient, TXA_2 AS doc_type, TXA_14 AS status, OBX_3 AS first_section
FROM external.hl7.clinical_materialized WHERE MSH_9 LIKE 'MDM%';

-- Appointment schedule (materialized table)
SELECT PID_5 AS patient, SCH_1 AS appt_id, SCH_7 AS reason, SCH_25 AS status
FROM external.hl7.clinical_materialized WHERE MSH_9 LIKE 'SIU%';

-- Edge case: verify escape decoding
SELECT PID_5 AS patient_name, OBX_3 AS obs_id, OBX_5 AS obs_value
FROM external.hl7.clinical_materialized WHERE df_file_name LIKE '%edge%';

-- Full H&P narrative via JSON
SELECT df_file_name, MSH_9 AS message_type, df_message_json
FROM external.hl7.clinical_messages WHERE df_file_name LIKE '%mdm%';
```

## What This Tests

1. MDM^T02 medical document message parsing
2. TXA document metadata extraction via materialized paths
3. SIU^S12 scheduling message parsing with SCH fields materialized
4. Full message JSON for deep access to all OBX sections and resource segments
5. HL7 escape sequence decoding (`\F\`, `\T\`, `\S\`, `\R\`, `\E\`, `\X0D\`)
6. Empty field handling in OBX observations
7. Multi-line text with `\X0D\` carriage returns
8. Special characters in patient names and observation values
9. Cross-version scheduling (v2.3 vs v2.5.1 SIU differences)
10. Mixed message type unification (MDM + SIU + ADT in one table)
