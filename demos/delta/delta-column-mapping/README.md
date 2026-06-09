# Delta Column Mapping — Physical & Logical Names

Demonstrates Delta Lake column mapping on an employee directory table.
Column mapping decouples logical column names from physical Parquet column
names, enabling column rename and drop operations without rewriting data files.

## Data Story

A company's employee directory uses column mapping (mode = 'name') to enable
future schema flexibility. Columns are referenced by logical names in queries
while stored with physical IDs in Parquet files. This allows the organization
to rename columns (e.g., "full_name" to "display_name") without rewriting
data. The directory tracks 40 employees across 6 departments, with promotions,
deactivations, and a new location column added via ALTER TABLE.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `employee_directory` | Delta Table | 40 | Employee directory with column mapping enabled |

## Column Mapping Properties

| Property | Value | Purpose |
|----------|-------|---------|
| `delta.columnMapping.mode` | `name` | Decouple logical/physical column names |
| `delta.minReaderVersion` | `2` | Required for column mapping support |
| `delta.minWriterVersion` | `5` | Required for column mapping support |

## Setup Phases

| Phase | Action | Result |
|-------|--------|--------|
| 1 | CREATE TABLE with TBLPROPERTIES | 8-column table with column mapping |
| 2 | INSERT 30 employees | 6 departments populated |
| 3 | GRANT ADMIN | Permissions granted |
| 4 | INSERT 10 more employees | 40 total rows |
| 5 | UPDATE 5 employees | Promoted to Senior/Lead titles |
| 6 | UPDATE 3 employees | Deactivated (is_active = 0) |
| 7 | ALTER TABLE ADD COLUMN location | 9 columns, location NULL for all rows |

## Verification

8 automated PASS/FAIL checks verify row counts, department distribution,
promotion counts, active/inactive status, NULL patterns from the new column,
and salary integrity.
