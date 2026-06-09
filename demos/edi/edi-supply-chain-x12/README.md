# EDI Supply Chain X12 — Order-to-Cash Lifecycle

## Overview

Demonstrates how DeltaForge ingests X12 EDI transactions spanning the full
supply chain order-to-cash lifecycle. Fourteen real-world transactions cover
purchase orders, invoices, shipping notices, acknowledgments, receiving advice,
and application advice. Two external tables provide different views: a compact
header-only table and a materialized table with key business fields extracted.

## Business Scenario

A company's EDI integration hub receives X12 transactions from multiple trading
partners: a parts supplier (000123456), a music supply company (ABCMUSICSUPPLY),
a general sender (SENDER1), and an acknowledgment partner (TO/FROM). Each
partner sends different transaction types using different X12 versions (00401
and 00204). DeltaForge reads all `.edi` files and produces structured tables
covering the entire order-to-cash cycle — no custom parser or ETL pipeline
required.

## EDI Features Demonstrated

| Feature | How It's Used |
| ------- | ------------- |
| Multi-version parsing | X12 versions 00401 and 00204 in one table |
| ISA envelope extraction | Sender, receiver, version, control number — always available |
| GS functional group | Group codes PO, IN, FA classify business purpose |
| ST transaction set | 8 distinct transaction types (850, 810, 855, 856, 857, 861, 997, 824) |
| Materialized paths | BEG_3 (PO number), BIG_2 (invoice number), N1_2 (party name) as columns |
| Full JSON output | df_transaction_json contains complete transaction for deep access |
| File metadata | df_file_name traces each row to its source file |
| Trading partner diversity | 4 sender/receiver pairs across 14 files |

## What This Demo Sets Up

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `edi` | EDI transaction-backed external tables |
| Table | `external.edi.supply_chain_messages` | Compact view: ISA/GS/ST headers + JSON (14 rows) |
| Table | `external.edi.supply_chain_materialized` | Materialized: headers + BEG/BIG/BSN/N1/CTT fields (14 rows) |

## Data Files

| File | Transaction Type | Description |
| ---- | ---------------- | ----------- |
| `x12_850_purchase_order.edi` | 850 Purchase Order | Simple PO: 1 line item, ship-to, UPS Ground |
| `x12_850_purchase_order_a.edi` | 850 Purchase Order | Complex PO: 3 textile line items, international |
| `x12_850_purchase_order_edifabric.edi` | 850 Purchase Order | Aerospace PO: defense parts, scheduled delivery |
| `x12_810_invoice_a.edi` | 810 Invoice | Music supply: 2 items, payment terms, remit-to bank |
| `x12_810_invoice_b.edi` | 810 Invoice | Music supply with tax: state and federal tax detail |
| `x12_810_invoice_c.edi` | 810 Invoice | Header-level: 1 consolidated line, allowances |
| `x12_810_invoice_d.edi` | 810 Invoice | Multi-line header: 2 items with delivery dates |
| `x12_810_invoice_edifabric.edi` | 810 Invoice | Aerospace: simple 1-line, ship-to ABC Aerospace |
| `x12_855_purchase_order_ack.edi` | 855 PO Acknowledgment | 3 line items, XYZ Manufacturing to Kohls |
| `x12_856_ship_notice.edi` | 856 Ship Notice | Hierarchical ASN: shipment/order/pack/item levels |
| `x12_856_ship_bill_notice.edi` | 857 Shipment & Billing | Combined ship/bill with pricing and discounts |
| `x12_861_receiving_advice.edi` | 861 Receiving Advice | Receipt confirmation with acceptance code |
| `x12_997_functional_acknowledgment.edi` | 997 Functional Ack | Accepts 1, rejects 1 of 2 transaction sets |
| `x12_824_application_advice.edi` | 824 Application Advice | Error: invalid part/plant/supplier combination |

## Sample Queries

```sql
-- All transactions with sender/receiver and type
SELECT ISA_6 AS sender, ISA_8 AS receiver, ST_1 AS txn_type, GS_8 AS version
FROM external.edi.supply_chain_messages ORDER BY df_file_name;

-- Transaction type distribution
SELECT ST_1 AS txn_type, COUNT(*) AS count
FROM external.edi.supply_chain_messages GROUP BY ST_1 ORDER BY count DESC;

-- Purchase order details (materialized)
SELECT BEG_3 AS po_number, BEG_5 AS po_date, N1_2 AS party_name
FROM external.edi.supply_chain_materialized WHERE BEG_3 IS NOT NULL;

-- Trading partner relationships
SELECT ISA_6 AS sender, ISA_8 AS receiver, COUNT(*) AS txns
FROM external.edi.supply_chain_messages GROUP BY ISA_6, ISA_8;

-- Full transaction JSON for deep access
SELECT df_file_name, ST_1 AS txn_type, df_transaction_json
FROM external.edi.supply_chain_messages LIMIT 3;
```

## What This Tests

1. X12 EDI transaction parsing across versions 00401 and 00204
2. Multi-transaction-type unification in a single table (8 distinct types)
3. ISA interchange envelope field extraction (always available)
4. GS functional group field extraction (PO, IN, FA codes)
5. ST transaction set identification (850, 810, 855, 856, 857, 861, 997, 824)
6. Materialized path extraction for BEG, BIG, BSN, N1, and CTT segments
7. Full transaction JSON output for deep segment access
8. File metadata injection (df_file_name traceability)
9. Trading partner diversity (4 sender/receiver pairs)
10. Order-to-cash lifecycle coverage (order through acknowledgment)
