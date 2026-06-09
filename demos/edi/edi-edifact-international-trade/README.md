# EDI EDIFACT International Trade

Demonstrates UN/EDIFACT and EANCOM message parsing across 22 files covering
international trade, logistics, customs clearance, and passenger data from
multiple directory versions and syntax identifiers.

## Data Story

A global logistics company uses DeltaForge to unify EDIFACT messages arriving
from shipping lines, customs authorities, airlines, and retail supply chains.
Messages flow in from trading partners worldwide using different EDIFACT
directory versions (D:96A through D:13B), syntax character sets (UNOA through
UNOL), and both pure EDIFACT and EANCOM (GS1 retail) standards. The company
needs:

1. A **compact header table** showing every message's envelope metadata
   (sender, receiver, syntax version, message type) for routing and audit
2. A **materialized trade table** extracting key business fields (document
   codes, party identifiers, dates, line items) for operational dashboards

## EDIFACT Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **Multi-format** | Pure EDIFACT (16 files) and EANCOM (6 files) parsed together |
| **Multi-version** | D:96A, D:01B, D:03B, D:95B, D:10A, D:13B, D:00B directory versions |
| **Syntax variants** | UNOA, UNOB, UNOC, UNOL, IATB, IATA character sets |
| **Multi-message files** | edifact_multi_message.edi and edifact_CONTRL_acknowledgment.edi each contain 2 messages |
| **materialized_paths** | BGM, NAD, DTM, LIN fields extracted as first-class columns |
| **df_transaction_json** | Full parsed message available as JSON for deep access |
| **file_metadata** | df_file_name and df_row_number for provenance tracking |
| **17 message types** | ORDERS, ORDRSP, INVOIC, IFCSUM, CUSCAR, BAPLIE, PAXLST, PNRGOV, APERAK, CONTRL, INFENT, QUOTES, PAORES, DESADV, IFTSTA, PRICAT, IFTMIN |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `edifact_messages` | External Table | >= 22 | UNB/UNH headers + full JSON for all messages |
| `edifact_materialized` | External Table | >= 22 | UNB/UNH headers + BGM/NAD/DTM/LIN trade fields |

## Schema

**edifact_messages:** `UNB_1 VARCHAR, UNB_2 VARCHAR, UNB_3 VARCHAR, UNB_4 VARCHAR, UNB_5 VARCHAR, UNH_1 VARCHAR, UNH_2 VARCHAR, df_transaction_json VARCHAR, df_transaction_id VARCHAR, df_file_name VARCHAR, df_row_number BIGINT`

**edifact_materialized:** `UNB_1 VARCHAR, UNB_2 VARCHAR, UNB_3 VARCHAR, UNB_4 VARCHAR, UNB_5 VARCHAR, UNH_1 VARCHAR, UNH_2 VARCHAR, BGM_1 VARCHAR, BGM_2 VARCHAR, NAD_1 VARCHAR, NAD_2 VARCHAR, DTM_1 VARCHAR, DTM_2 VARCHAR, LIN_1 VARCHAR, LIN_3 VARCHAR, df_transaction_json VARCHAR, df_transaction_id VARCHAR, df_file_name VARCHAR, df_row_number BIGINT`

## Data Files

22 EDIFACT/EANCOM files organized by message type:

### EDIFACT Files (16)

| File | Message Type | Directory | Key Segments | Size |
|------|-------------|-----------|--------------|------|
| `edifact_ORDERS_purchase_order.edi` | ORDERS | D:96A:UN | BGM/NAD/LIN | 833 B |
| `edifact_ORDRSP_order_response.edi` | ORDRSP | D:10A:UN:1.1e | BGM/NAD/LIN | 703 B |
| `edifact_INVOIC_invoice_edifabric.edi` | INVOIC | D:96A:UN | BGM/NAD/LIN/TAX/MOA | 1,092 B |
| `edifact_D01B_INVOIC_invoice.edi` | INVOIC | D:01B:UN:EAN010 | BGM/TAX/MOA | 509 B |
| `edifact_D01B_IFCSUM_forwarding.edi` | IFCSUM | D:01B:UN:EAN003 | BGM/NAD/TDT/CNI/GID | 745 B |
| `edifact_D95B_CUSCAR_customs_cargo.edi` | CUSCAR | D:95B:UN | BGM/NAD/TDT/CNI/GID | 2,459 B |
| `edifact_CUSCAR_cargo_report.edi` | CUSCAR | D:03B:UN | BGM/NAD/TDT/CNI/GID | 1,199 B |
| `edifact_BAPLIE_bayplan_stowage.edi` | BAPLIE | D:13B:UN:SMDG31 | BGM/TDT/LOC/EQD | 593 B |
| `edifact_PAXLST_passenger_list.edi` | PAXLST | D:03B:UN | BGM/TDT/NAD/DOC | 522 B |
| `edifact_PNRGOV_passenger_data.edi` | PNRGOV | 11:1:IA | MSG/TVL/TIF/SSR | 1,820 B |
| `edifact_APERAK_acknowledgment.edi` | APERAK | D:96A:UN | BGM/RFF/NAD | 294 B |
| `edifact_CONTRL_acknowledgment.edi` | CONTRL | 4:1:UN | UCI (2 messages) | 233 B |
| `edifact_PAIEMENT_payment.edi` | INFENT | D:00B:UN:PD1501 | BGM/NAD/RFF | 624 B |
| `edifact_basic.edi` | QUOTES | D:96A:UN:EDIEL2 | BGM/LIN/PRI | 741 B |
| `edifact_multi_message.edi` | QUOTES | D:96A:UN:EDIEL2 | BGM/LIN/PRI (x2) | 1,366 B |
| `edifact_wikipedia_example.edi` | PAORES | 93:1:IA | TVL/PDI/APD | 339 B |

### EANCOM Files (6)

| File | Message Type | Directory | Key Segments | Size |
|------|-------------|-----------|--------------|------|
| `eancom_DESADV_despatch_advice.edi` | DESADV | D:96A:UN:EAN007 | BGM/NAD/CPS/LIN | 847 B |
| `eancom_IFTSTA_transport_status.edi` | IFTSTA | D:96A:UN:EAN004 | BGM/NAD/CNI/STS | 741 B |
| `eancom_INVOIC_invoice.edi` | INVOIC | D:01B:UN:EAN011 | BGM/NAD/LIN/TAX/MOA | 1,031 B |
| `eancom_ORDRSP_order_response.edi` | ORDRSP | D:96A:UN:EAN009 | BGM/NAD/LIN | 591 B |
| `eancom_PRICAT_price_catalogue.edi` | PRICAT | D:96A:UN:EAN009 | BGM/NAD/LIN/PRI | 1,069 B |
| `eancom_instruction.edi` | IFTMIN | D:96A:UN:EAN004 | BGM/NAD/GID/TMP | 565 B |

## Business Domains

The 22 files span five business domains:

| Domain | Message Types | Description |
|--------|--------------|-------------|
| **Commerce** | ORDERS, ORDRSP, INVOIC, PRICAT, QUOTES | Purchase orders, invoices, price catalogues |
| **Transport** | IFCSUM, IFTSTA, IFTMIN, BAPLIE, DESADV | Freight forwarding, shipment tracking, stowage |
| **Border** | CUSCAR, PAXLST, PNRGOV | Customs cargo reports, passenger manifests |
| **Acknowledgment** | APERAK, CONTRL | Application errors, syntax acknowledgments |
| **Other** | INFENT, PAORES | Payment information, travel reservations |

## Multi-Message Files

Two files contain multiple messages inside a single UNB interchange envelope:

- **edifact_multi_message.edi** — 2 QUOTES messages (identical structure)
- **edifact_CONTRL_acknowledgment.edi** — 2 CONTRL messages (UCI segments)

This means the total row count is >= 24 (22 files + 2 extra messages from
multi-message envelopes).

## Known Verification Values

| Check | Expected | Source |
|-------|----------|--------|
| Message count | >= 22 | 22 files, some with 2 messages |
| Source files | 22 distinct df_file_name | One per .edi file |
| Message types | >= 10 distinct UNH_2 | 17 distinct message types |
| Materialized count | >= 22 | Same files, different extraction |
| BGM populated | > 0 rows | Most messages have BGM segment |
| JSON populated | >= 22 | Every row has df_transaction_json |

## How to Verify

Run **Query #9 (Summary)** to see PASS/FAIL for all 6 checks:

```sql
SELECT check_name, result FROM (...) ORDER BY check_name;
```

All checks should return `PASS`.
