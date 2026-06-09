# EDI TRADACOMS UK Retail — Orders, Planning & Utilities

## Overview

Demonstrates how DeltaForge ingests TRADACOMS — the UK-specific EDI standard
used predominantly in retail and utilities. Four files cover purchase orders,
product planning forecasts, and utility bills, including escape character
handling for special characters in trading partner names. Two external tables
provide different views: a compact header-only table and a materialized table
with key trading partner and document type fields extracted.

## Business Scenario

A UK retail chain uses DeltaForge to consolidate TRADACOMS messages from
multiple trading partners. Purchase orders flow to manufacturers, product
planning forecasts coordinate stock levels with suppliers, and utility bills
arrive from energy providers. Each TRADACOMS transmission contains multiple
message types (header, detail, trailer), and DeltaForge exposes every message
as a queryable row — no custom parser or ETL pipeline required.

## TRADACOMS Features Demonstrated

| Feature | How It's Used |
| ------- | ------------- |
| Multi-message parsing | Each file contains 3-4 MHD segments; one row per message |
| STX envelope extraction | Sender, receiver, date, reference — always available |
| MHD type classification | ORDERS:9, PPRDET:2, UTLBIL:3, etc. as queryable columns |
| Materialized paths | TYP_1, SDT_2 (supplier), CDT_2 (customer) extracted as columns |
| Full JSON output | df_transaction_json contains complete message for deep access |
| Escape character decoding | ?' (apostrophe) and ?+ (plus) automatically decoded |
| File metadata | df_file_name traces each row to its source file |
| EAN code support | 13-digit EAN codes in STX_2/STX_3 for electronic identification |

## What This Demo Sets Up

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `edi` | EDI transaction-backed external tables |
| Table | `external.edi.tradacoms_messages` | Compact view: STX/MHD headers + JSON (15 rows) |
| Table | `external.edi.tradacoms_materialized` | Materialized: STX/MHD headers + TYP/SDT/CDT fields (15 rows) |

## Data Files

| File | Messages | MHD Types | Key Content |
| ---- | -------- | --------- | ----------- |
| `tradacoms_order.edi` | 4 | ORDHDR:9, ORDERS:9 (x2), ORDTLR:9 | Purchase order: ANY SHOP PLC to XYZ MANUFACTURING PLC |
| `tradacoms_product_planning.edi` | 3 | PPRHDR:2, PPRDET:2, PPRTLR:2 | Forecast with EAN-coded trading partners |
| `tradacoms_utility_bill.edi` | 4 | UTLHDR:3, UTLBIL:3, UVATLR:3, UTLTLR:3 | Electricity bill from SOME ELECTRIC COMPANY PLC |
| `tradacoms_utility_bill_escape.edi` | 4 | UTLHDR:3, UTLBIL:3, UVATLR:3, UTLTLR:3 | Same as above with escaped special characters |

## TRADACOMS Message Structure

Unlike X12 or EDIFACT, TRADACOMS wraps messages in an STX/END envelope:

```
STX=ANA:1+sender+receiver+date+reference'     <- Transmission start
MHD=1+ORDHDR:9'                                <- Message 1 header
TYP=0430+NEW ORDERS'                           <- Message body
...
MTR=6'                                         <- Message 1 trailer
MHD=2+ORDERS:9'                                <- Message 2 header
...
MTR=14'                                        <- Message 2 trailer
END=4'                                         <- Transmission end
```

The `?` character is the escape prefix: `?'` = literal apostrophe, `?+` = literal plus.

## Sample Queries

```sql
-- All messages with their types (compact table)
SELECT df_file_name, STX_2 AS sender, MHD_2 AS msg_type
FROM external.edi.tradacoms_messages ORDER BY df_file_name, MHD_1;

-- Message type distribution
SELECT MHD_2 AS msg_type, COUNT(*) AS count
FROM external.edi.tradacoms_messages GROUP BY MHD_2;

-- Supplier and customer names (materialized table)
SELECT SDT_2 AS supplier, CDT_2 AS customer, MHD_2 AS msg_type
FROM external.edi.tradacoms_materialized WHERE SDT_2 IS NOT NULL;

-- Escape character handling
SELECT CDT_2 AS customer_name
FROM external.edi.tradacoms_materialized WHERE df_file_name LIKE '%escape%';
```

## What This Tests

1. TRADACOMS EDI format parsing (UK retail standard)
2. Multi-message file handling (multiple MHD segments per file)
3. STX envelope field extraction (sender, receiver, date, reference)
4. MHD message type classification (ORDERS, PPRDET, UTLBIL, etc.)
5. Materialized path extraction for TYP, SDT, and CDT segments
6. Full transaction JSON output for deep segment access
7. TRADACOMS escape character decoding (?' and ?+)
8. File metadata injection (df_file_name traceability)
9. Mixed message type handling (orders, planning, utility bills)
