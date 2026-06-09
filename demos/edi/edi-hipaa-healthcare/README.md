# EDI HIPAA Healthcare — Claims Lifecycle

## Overview

Demonstrates how DeltaForge ingests HIPAA X12 healthcare EDI transactions
covering the complete claims lifecycle. Eleven transactions span eligibility
verification, claim status inquiry, healthcare claims, payment/remittance,
benefit enrollment, and prior authorization. Two external tables provide
different views: a compact header-only table and a materialized table with
key clinical and financial fields extracted.

## Business Scenario

A health insurance clearinghouse receives HIPAA EDI transactions from multiple
trading partners — providers, payers, employers, and third-party administrators.
Each transaction type follows a different HIPAA 5010 implementation guide, but
all share the common X12 envelope structure (ISA/GS/ST). DeltaForge reads all
`.edi` files and produces structured tables — no custom parser, segment mapping,
or ETL pipeline required. Analysts can immediately query across the full claims
lifecycle using standard SQL.

## EDI Features Demonstrated

| Feature | How It's Used |
| ------- | ------------- |
| X12 envelope parsing | ISA (interchange), GS (functional group), ST (transaction set) headers |
| Multi-transaction types | 9 distinct ST types (270, 271, 276, 277, 278, 820, 834, 835, 837) |
| Implementation guides | 9 different HIPAA 5010 guide versions in one table |
| Materialized paths | BHT, NM1, CLM, BPR fields extracted as first-class columns |
| Full JSON output | df_transaction_json contains complete transaction for deep access |
| Functional groups | HC (Health Care) and BE (Benefit Enrollment) in the same feed |
| File metadata | df_file_name traces each row to its source .edi file |

## What This Demo Sets Up

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `edi` | EDI transaction-backed external tables |
| Table | `external.edi.hipaa_messages` | Compact view: ISA/GS/ST headers + JSON (11 rows) |
| Table | `external.edi.hipaa_materialized` | Materialized: headers + BHT/NM1/CLM/BPR fields (11 rows) |

## Data Files

| File | ST Type | GS Code | Implementation Guide | Key Segments |
| ---- | ------- | ------- | -------------------- | ------------ |
| `hipaa_270_eligibility_request.edi` | 270 | HC | 005010X279A1 | BHT, NM1, HL |
| `hipaa_271_eligibility_response.edi` | 271 | HC | 005010X279A1 | BHT, NM1, EB |
| `hipaa_276_claim_status_request.edi` | 276 | HC | 005010X212 | BHT, NM1, TRN, SVC |
| `hipaa_277_claim_status_response.edi` | 277 | HC | 005010X212 | BHT, NM1, STC, SVC |
| `hipaa_278_services_review.edi` | 278 | HC | 005010X217 | BHT, NM1, UM, HI |
| `hipaa_820_payment.edi` | 835 | HC | 005010X221A1 | BPR, TRN, CLP, SVC |
| `hipaa_820_payment_order.edi` | 820 | HC | 005010X218 | BPR, TRN, RMR |
| `hipaa_834_benefit_enrollment.edi` | 834 | BE | 005010X220A1 | BGN, INS, NM1, HD |
| `hipaa_835_claim_payment.edi` | 837 | HC | 005010X222A1 | BHT, NM1, CLM, SV1 |
| `hipaa_837D_dental_claim.edi` | 837 | HC | 005010X224A2 | BHT, NM1, CLM, SV3 |
| `hipaa_837I_institutional_claim.edi` | 837 | HC | 005010X223A2 | BHT, NM1, CLM, SV2 |

Note: ST type values come from file content (ST-01), not filenames. Two files have
ST types that differ from their filenames: `hipaa_820_payment.edi` contains ST=835,
and `hipaa_835_claim_payment.edi` contains ST=837.

## HIPAA Transaction Types

| Code | Name | Direction | Purpose |
| ---- | ---- | --------- | ------- |
| 270 | Eligibility Inquiry | Provider to Payer | Verify patient insurance coverage |
| 271 | Eligibility Response | Payer to Provider | Return coverage and benefit details |
| 276 | Claim Status Request | Provider to Payer | Ask about claim processing status |
| 277 | Claim Status Response | Payer to Provider | Return claim adjudication status |
| 278 | Health Services Review | Provider to Payer | Request prior authorization |
| 820 | Payment Order | Payer/Employer | Premium payment instructions |
| 834 | Benefit Enrollment | Employer to Payer | Employee enrollment and maintenance |
| 835 | Claim Payment/Remittance | Payer to Provider | Payment details and adjustments |
| 837 | Healthcare Claim | Provider to Payer | Claim submission (professional/dental/institutional) |

## Sample Queries

```sql
-- All transactions with type and implementation guide
SELECT df_file_name, ST_1 AS type, GS_8 AS guide, ISA_6 AS sender
FROM external.edi.hipaa_messages ORDER BY df_file_name;

-- Transaction type distribution
SELECT ST_1 AS type, COUNT(*) AS count
FROM external.edi.hipaa_messages GROUP BY ST_1 ORDER BY ST_1;

-- Claim details from materialized table
SELECT CLM_1 AS claim_id, CLM_2 AS amount, NM1_3 AS patient
FROM external.edi.hipaa_materialized WHERE CLM_1 IS NOT NULL;

-- Payment transactions
SELECT BPR_1 AS handling, BPR_2 AS amount, ST_1 AS type
FROM external.edi.hipaa_materialized WHERE BPR_1 IS NOT NULL;

-- Full transaction JSON for deep access
SELECT df_file_name, ST_1, df_transaction_json
FROM external.edi.hipaa_messages LIMIT 3;
```

## What This Tests

1. X12 EDI transaction parsing across 9 distinct HIPAA transaction types
2. HIPAA 5010 implementation guide version handling (9 different guides)
3. ISA/GS/ST envelope field extraction (always available without materialized_paths)
4. Materialized path extraction for BHT, NM1, CLM, and BPR segments
5. Full transaction JSON output for deep segment access
6. Mixed functional group codes (HC and BE) in a single table
7. File metadata injection (df_file_name traceability)
8. Claims lifecycle coverage (eligibility through payment)
9. Multi-claim-type handling (837 professional, dental, institutional)
