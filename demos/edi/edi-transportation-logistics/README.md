# EDI Transportation & Logistics -- X12 Freight Lifecycle

## Overview

Demonstrates how DeltaForge ingests X12 EDI transactions spanning the full
freight lifecycle: load tendering (204/990), freight invoicing (210), shipment
tracking (214), rail transport (404), warehouse operations (945), payment
remittance (820), and price catalogs (832). Two external tables provide
different views: a compact header-only table and a materialized table with key
logistics fields extracted as first-class columns.

## Business Scenario

A freight company operates across multiple transportation modes -- motor
carrier, rail, and warehouse -- exchanging EDI documents with shippers,
carriers, consignees, and financial partners. Each trading partner uses a
different X12 version and functional group code. DeltaForge reads all `.edi`
files and produces structured tables -- no custom parser, version mapping, or
ETL pipeline required. Analysts can query shipment statuses alongside payment
remittances and load tenders in a single SQL interface.

## EDI Features Demonstrated

| Feature | How It's Used |
| ------- | ------------- |
| Multi-version parsing | X12 versions 00204, 00400, 00401, 00403, 00501 in one table |
| ISA/GS/ST extraction | Sender, receiver, transaction type, version -- always available |
| Materialized paths | B10_1 (shipment ID), B10_2 (BOL), N1_2 (party name) extracted as columns |
| Full JSON output | df_transaction_json contains complete transaction for deep access |
| Multi-transaction-type | 8 distinct X12 types (204, 210, 214, 404, 820, 832, 945, 990) |
| Trading partner analysis | ISA_6/ISA_8 reveal sender/receiver relationships across documents |
| File metadata | df_file_name traces each row to its source .edi file |

## What This Demo Sets Up

| Resource | Name | Description |
| -------- | ---- | ----------- |
| Zone | `external` | Shared namespace for all external/demo tables |
| Schema | `edi` | EDI transaction-backed external tables |
| Table | `external.edi.logistics_messages` | Compact view: ISA/GS/ST headers + JSON (12 rows) |
| Table | `external.edi.logistics_materialized` | Materialized: headers + B2/B3/B10/N1/L3 fields (12 rows) |

## Data Files

| File | X12 Type | Description | GS Code | X12 Version |
| ---- | -------- | ----------- | ------- | ----------- |
| `x12_204_motor_carrier_load_tender.edi` | 204 | Motor Carrier Load Tender -- 2 stops, weight/charges | SM | 00401 |
| `x12_210_freight_invoice.edi` | 210 | Freight Invoice -- 4 line items, fuel surcharge | IM | 00401 |
| `x12_210_freight_invoice_edifabric.edi` | 210 | Freight Invoice -- international MX-to-CA route | IN | 00204 |
| `x12_214_shipment_status.edi` | 214 | Shipment Status -- delivered, city/state location | QM | 00403 |
| `x12_214_shipment_status_edifabric.edi` | 214 | Shipment Status -- departed, GPS coordinates | IN | 00204 |
| `x12_214_transportation_status.edi` | 214 | Transportation Status -- delivered with parties | QM | 00403 |
| `x12_404_rail_carrier_shipment.edi` | 404 | Rail Carrier Shipment -- NS/BNSF, auto parts | IN | 00204 |
| `x12_820_payment_order.edi` | 820 | Payment Order -- $21K check, 7 remittance lines | RA | 00501 |
| `x12_832_price_catalog_edifabric.edi` | 832 | Price/Sales Catalog -- BISG book, ISBN/pricing | IN | 00204 |
| `x12_832_price_sales_catalog.edi` | 832 | Price/Sales Catalog -- 2 ISBN items, publisher | SC | 00401 |
| `x12_940_warehouse_shipping_advice.edi` | 945 | Warehouse Shipping Advice -- UPC/EAN, 2 items | IN | 00204 |
| `x12_990_load_tender_response.edi` | 990 | Load Tender Response -- acceptance of tender | GF | 00400 |

## Sample Queries

```sql
-- All transactions with header details (compact table)
SELECT df_file_name, ISA_6 AS sender, ST_1 AS transaction, GS_1 AS func_group
FROM external.edi.logistics_messages ORDER BY df_file_name;

-- Transaction type distribution
SELECT ST_1 AS transaction_type, COUNT(*) AS doc_count
FROM external.edi.logistics_messages GROUP BY ST_1 ORDER BY ST_1;

-- Shipment status details (materialized table)
SELECT B10_1 AS shipment_id, B10_2 AS bol, N1_2 AS party_name
FROM external.edi.logistics_materialized WHERE B10_1 IS NOT NULL;

-- Full transaction JSON for deep access
SELECT df_file_name, ST_1 AS transaction, df_transaction_json
FROM external.edi.logistics_messages LIMIT 3;
```

## What This Tests

1. X12 EDI transaction parsing across versions 00204 through 005010
2. Multi-transaction-type unification (8 types) in a single table
3. ISA/GS/ST header field extraction (always available without materialized_paths)
4. Materialized path extraction for B2, B3, B10, N1, and L3 segments
5. Full transaction JSON output for deep segment access
6. Trading partner relationship analysis via ISA sender/receiver IDs
7. File metadata injection (df_file_name traceability)
8. Mixed transportation modes (motor, rail, warehouse) in one dataset
9. Financial documents (820 payment, 210 invoice) alongside operational documents
