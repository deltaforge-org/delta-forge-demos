# Delta Forge Demos

180+ self-contained, ASSERT-validated demonstrations across every supported format and industry vertical.

## Overview

Delta Forge Demos is a structured library of end-to-end demonstrations that exercise the full breadth of the SQL engine. Each demo is a self-contained unit with setup SQL, analytical queries, cleanup SQL, and mathematically validated expected results. Demos serve a dual purpose: they are both interactive learning material for users and the source of truth for the automated correctness test suite.

## Key Features

### Self-Contained Demo Structure
- **setup.sql** - Creates tables, inserts seed data, registers external sources
- **queries.sql** - Analytical queries with inline `ASSERT` statements for expected values
- **cleanup.sql** - Drops all objects for clean teardown
- **demo.toml** - Metadata, configuration, and category tags
- **README.md** - Human-readable explanation of what the demo proves

### ASSERT-Based Validation
- Every query includes `ASSERT` annotations with pre-calculated expected values
- Assertions are mathematically verified — complex scenarios include Python proof scripts
- Results are deterministic and reproducible across platforms
- Demos double as regression tests when loaded by the test harness

### Delta Lake (100+ Demos)
- **CRUD operations** - INSERT, UPDATE, DELETE, MERGE with all clause combinations
- **MERGE patterns** - SCD Type 2, deduplication, idempotent upserts, composite keys, multi-source, soft delete, subquery predicates, computed columns
- **Time travel** - VERSION AS OF, TIMESTAMP AS OF, point-in-time joins, row-level diffs, vacuum boundary behavior
- **Partitioning** - Single/multi-level, cross-partition updates, selective OPTIMIZE, partition pruning, Unicode partition keys, DV interaction
- **Schema evolution** - Column addition, type widening, flexible ingestion, column mapping modes
- **Table maintenance** - VACUUM, OPTIMIZE, RESTORE, Z-ORDER, bloom filters, deletion vector accumulation, storage diagnostics
- **Advanced SQL** - Window functions, grouping sets, funnel analysis, overflow detection, subqueries, set operations, CASE expressions, duration arithmetic
- **Governance** - GDPR data erasure, audit trail versioning, views with data masking, constraint enforcement, append-only tables
- **Timestamps** - Timezone-naive types, cross-timezone scheduling, date/time analytics

### Healthcare (12 Demos)
- **FHIR** - Patient demographics, clinical observations, medication prescriptions, clinical records, XML clinical resources
- **HL7 v2** - Patient administration (ADT), lab orders and results, clinical workflows
- **Pseudonymisation** - Quickstart, apply, lifecycle, and healthcare-specific PII handling

### EDI / Supply Chain (18 Demos)
- **HIPAA** - Claims processing (837), remittance (835), eligibility (270/271), claim status (276/277)
- **X12** - Supply chain purchase orders, transportation logistics, order lifecycle tracking
- **EDIFACT** - International trade, customs/border, invoice reconciliation
- **TRADACOMS** - UK retail purchase orders, utility billing, deep JSON access
- **EANCOM** - Retail supply chain with ORDERS/DESADV/INVOIC

### Graph Analytics (13 Demos)
- **Cypher queries** - Pattern matching, variable-length paths, weighted shortest paths
- **Graph algorithms** - PageRank, community detection, centrality measures via SQL and Cypher
- **Storage modes** - Flattened, hybrid, and JSON-based graph representations
- **Real networks** - Karate Club, EU email, NetScience, political books, LDBC Social Network Benchmark
- **Mutations** - Node/edge creation and deletion within graph workspaces

### Geospatial (5 Demos)
- **H3 hexagonal indexing** - Delivery optimization, GPS fleet tracking, point-in-polygon
- **GIS operations** - Emergency response routing, maritime shipping lane analysis

### Multi-Format Coverage
- **Avro** - E-commerce orders, insurance claims, IoT sensors (logical types, schema evolution, compression)
- **CSV** - Northwind database, sales quickstart, veterinary clinic, CSV options testbench
- **Excel** - Sales analytics, multi-sheet reporting, options testbench
- **JSON** - CIA World Factbook, customer records, music catalog, subtree capture
- **ORC** - Banking transactions, clinical trials, energy meters, insurance claims, server logs, warehouse inventory
- **Parquet** - Flight delays, supply chain analytics
- **Protobuf** - Address book contacts, freight shipping manifests, sensor networks
- **XML** - Books with schema evolution, e-commerce order lines, NYT news RSS, subtree capture
- **Iceberg** - V1, V2, and V3 table format demonstrations

### Mathematical Proofs
- Python proof scripts validate complex ASSERT values for EDI and healthcare demos
- Proofs parse raw source data and independently compute expected aggregates
- Guarantees that every assertion is traceable back to the seed data

## Demo Structure

```
delta-forge-demos
├── manifest.json          # Machine-readable index of all demos
├── demos/
│   ├── avro/              # 3 demos
│   ├── csv/               # 4 demos
│   ├── delta/             # 100+ demos
│   ├── edi/               # 18 demos
│   ├── excel/             # 3 demos
│   ├── fhir/              # 5 demos
│   ├── graph/             # 13 demos
│   ├── hl7/               # 3 demos
│   ├── iceberg/           # 3 demos (v1, v2, v3)
│   ├── json/              # 4 demos
│   ├── orc/               # 6 demos
│   ├── parquet/           # 2 demos
│   ├── protobuf/          # 3 demos
│   ├── pseudonymisation/  # 4 demos
│   ├── spatial/           # 5 demos
│   └── xml/               # 4 demos
├── demo-proofs/           # Python proof scripts for complex assertions
├── icons/                 # SVG icons for each demo
└── QUERY_ANNOTATIONS.md   # Annotation format documentation
```

## Usage

### Running a Demo Manually

```sql
-- 1. Execute setup.sql to create tables and load data
-- 2. Execute queries.sql — each query includes ASSERT for validation
-- 3. Execute cleanup.sql to tear down
```

### Loading via Manifest

The `manifest.json` file provides a machine-readable index with metadata for every demo — categories, tags, difficulty level, estimated runtime, data size, and target objects. The test harness and GUI both consume this manifest to discover and execute demos automatically.

### Adding a New Demo

Each demo folder requires four files:
1. `setup.sql` - Table creation and data loading
2. `queries.sql` - Queries with ASSERT annotations
3. `cleanup.sql` - Object teardown
4. `demo.toml` - Demo metadata and configuration
