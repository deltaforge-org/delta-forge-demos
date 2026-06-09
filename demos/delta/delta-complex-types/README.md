# Delta Complex Types — Structs, Arrays & Maps

Demonstrates complex data modelling patterns in Delta tables using flat
columns to represent structured, array-like, and map-like data.

## Data Story

A company's HR system stores employee records with nested address information,
comma-delimited skill lists, and key=value metadata tags. Engineering
receives a 15% salary increase, and 10 new hires join across all departments.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `employees` | Delta Table | 40 (final) | Staff with struct/array/map patterns |

## Schema

**employees:** `id INT, name VARCHAR, department VARCHAR, salary DOUBLE, address_street VARCHAR, address_city VARCHAR, address_state VARCHAR, address_zip VARCHAR, skills VARCHAR, metadata VARCHAR, hire_date VARCHAR, is_active BOOLEAN, manager_id INT, level VARCHAR`

## Patterns Demonstrated

1. **Struct-like columns** — address broken into street, city, state, zip
2. **Array-like column** — skills stored as comma-delimited string
3. **Map-like column** — metadata stored as key=value pairs
4. **Pattern matching** — LIKE queries on structured strings
5. **Department aggregations** — GROUP BY with structured data

## Verification

8 automated PASS/FAIL checks verify row counts, department distribution,
state filtering, skill pattern matching, metadata filtering, salary
updates, and level distribution.
