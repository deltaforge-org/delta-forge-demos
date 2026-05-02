# DeltaForge Demos

**258 ASSERT-validated demos. 10,500+ machine-checked assertions. 18 formats and industry verticals. Every result reproducible from seed data.**

This repository is the public proof that DeltaForge does what it says.

## Why this exists

Every evaluator asks the same question: *does it really work?* This repo is the answer. Each demo is a self-contained scenario that:

1. Builds tables, registers external sources, and loads realistic seed data.
2. Runs analytical queries that pin down behaviour with inline `ASSERT` annotations.
3. Validates the engine against pre-calculated expected values, refusing to pass on any drift.
4. Tears itself down cleanly so the next run starts from a fresh state.

If a demo passes on your install, the engine implements that capability correctly. If it fails, the failure is concrete: a specific query, an expected value, and the actual value the engine produced.

## Trust signals

**Mathematical, not anecdotal.** The 10,500+ assertions are not "looks about right" checks. They cover exact row counts, column-by-column expected values, statistical aggregates, time-travel snapshots, and cross-format equivalence comparisons.

**Independently verified.** Every assertion value was computed outside the engine before being written into the SQL, so the test never grades the engine using output from the same engine. Verification methods vary by demo: a handful of the most intricate EDI scenarios ship committed Python proof scripts under `demo-proofs/` that re-parse the raw X12 / EDIFACT payloads end to end; other demos were verified by hand calculation against the seed data, by cross-checking against reference implementations of the source format, or by running an alternative tool over the same input. In every case the expected value is traceable back to the raw source, not to a previous run of the system under test.

**Deterministic.** Demos are reproducible across platforms and runs. Seed data is fixed. Random-looking values are seeded. Time-travel demos pin to absolute version IDs and timestamps so snapshot queries return identical results every time.

**Production-shape data.** No toy "hello world" tables. The demos use realistic payloads: FHIR R4 patient bundles, HL7 v2 lab results, X12 837 claims, EDIFACT customs messages, TRADACOMS retail orders, EANCOM ORDERS/DESADV/INVOIC chains, NYT RSS, arXiv Atom feeds, GPS hexes, NetScience co-authorship graphs, multi-sheet Excel workbooks, multi-vendor XML clinical resources.

**Regression-locked.** The same `queries.sql` files that read as documentation are consumed by the automated test harness. If the engine ever drifts from documented behaviour, a build breaks before any release ships.

## Coverage

| Category | Demos | What it proves |
|---|---:|---|
| Delta Lake | 110 | CRUD, MERGE patterns (SCD2, dedup, idempotent upsert, composite keys, multi-source, soft delete), time travel, partitioning, schema evolution and column mapping, deletion vectors, OPTIMIZE / VACUUM / RESTORE, Z-order, bloom filters, change data feed, computed columns, constraints, GDPR erasure, audit trail versioning |
| Iceberg | 51 | Format V1, V2, and V3. UniForm interop with Delta, position deletes, equality deletes, Puffin deletion vectors, partition transforms, hidden partitions, copy-on-write and merge-on-read, time travel, large manifest handling |
| EDI | 19 | HIPAA 837 / 835 / 270 / 271 / 276 / 277, X12 850 and 856, EDIFACT (customs, invoice reconciliation, international trade), TRADACOMS (UK retail, utility billing, deep JSON access), EANCOM ORDERS / DESADV / INVOIC |
| Graph analytics | 17 | Cypher pattern matching, variable-length paths, weighted shortest paths, PageRank, community detection, GPU-accelerated graph workloads, real-world networks (Karate Club, EU email, NetScience, polbooks, LDBC SNB) |
| REST APIs | 10 | Live ingestion from arXiv (Atom XML), Frankfurter FX, GitHub topic search, NASA APOD, Open-Meteo, PokeAPI, public-holiday calendars, Rust release feed, JSONPlaceholder, httpbin auth flows |
| FHIR | 6 | R4 patient demographics, clinical observations, medication prescriptions, clinical records, multi-vendor XML resources, hospital bundle ingest |
| ORC | 6 | Banking transactions, clinical trials, energy meters, insurance claims, server logs, warehouse inventory |
| JSON | 5 | Country factbook, customer records, music catalog, subtree capture, typed billing events |
| Geospatial | 5 | H3 hexagonal indexing, GIS emergency response, maritime shipping lanes, point-in-polygon, GPS fleet tracking |
| XML | 5 | Schema evolution, e-commerce order lines, multi-vendor FHIR, NYT RSS, subtree capture |
| CSV | 4 | Northwind, sales quickstart, veterinary clinic, options testbench |
| HL7 v2 | 4 | ADT patient admin, lab orders / results, clinical workflows, typed chemistry panels |
| Pseudonymisation | 4 | Apply, lifecycle, exempt roles, healthcare PII handling |
| Avro | 3 | E-commerce orders, insurance claims, IoT sensors (logical types, schema evolution, compression) |
| Excel | 3 | Sales analytics, multi-sheet reporting, options testbench |
| Protobuf | 3 | Address book, freight shipping manifests, sensor networks |
| Parquet | 2 | Flight delays, supply-chain analytics |
| Charts | 1 | Server-rendered chart pipeline |

258 demos in total. 17 beginner, 137 intermediate, 104 advanced.

## Run any demo

```bash
delta-forge-cli demo-test <node> <zone> demos/edi/edi-hipaa-claims-financial
```

Each `ASSERT` is checked. The runner reports pass / fail per query and a final summary. No fixtures or scaffolding required beyond a running engine.

## Anatomy of a demo

```
demos/<category>/<demo-name>/
├── setup.sql        Tables, seed data, external source registration
├── queries.sql      Analytical queries with inline ASSERT annotations
├── cleanup.sql      Object teardown
├── demo.toml        Metadata, tags, difficulty, target objects
└── README.md        What the demo proves, in business terms
```

The annotation format is documented in `QUERY_ANNOTATIONS.md`. Supported annotations include exact row counts, range bounds, pipe-delimited value matrices, ordering constraints, set membership, and tolerance windows for floating-point comparisons.

## manifest.json

A single machine-readable index of all 258 demos with categories, tags, difficulty, estimated runtime, data size, and target objects. The CLI test runner, the evaluation tooling, and the GUI all consume this manifest to discover and execute demos.

## Adding a demo

1. Create a folder under the appropriate category.
2. Write `setup.sql`, `queries.sql` (with `ASSERT` annotations), and `cleanup.sql`.
3. Add `demo.toml` with metadata.
4. If your assertions involve non-trivial aggregation, add a Python proof under `demo-proofs/` that re-derives the expected values from the raw source data.
5. Regenerate `manifest.json`.

Bar for inclusion: the demo must run end-to-end clean on a fresh engine, every assertion must hold deterministically, and the demo's `README.md` must explain what business capability it proves.
