# Delta Constraint Enforcement — NOT NULL & CHECK

Demonstrates data quality enforcement patterns in Delta tables where all
rows satisfy business rule constraints.

## Data Story

A company's HR system maintains employee records with strict validation
rules: age must be between 18 and 70, salary must be positive, and
performance ratings must be between 0.0 and 5.0. After inserting 50
employees (all valid), salary raises and rating adjustments are applied —
each operation preserving all constraints. Five employees are then removed.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `validated_employees` | Delta Table | 45 (final) | Employee records with constraint-valid data |

## Schema

**validated_employees:** `id INT, name VARCHAR, email VARCHAR, age INT, salary DOUBLE, rating DOUBLE, department VARCHAR, hire_date VARCHAR`

## Constraints Enforced

- **Age range:** 18 ≤ age ≤ 70
- **Positive salary:** salary > 0
- **Rating range:** 0.0 ≤ rating ≤ 5.0
- **Non-null IDs:** id IS NOT NULL

## Operations

- INSERT 50 employees (all within constraints)
- UPDATE all salaries +10% (preserves salary > 0)
- UPDATE ratings +0.2 where < 3.5, capped at 5.0 (preserves range)
- DELETE 5 employees

## Verification

8 automated PASS/FAIL checks verify: 45 total rows, all ages in range, all
salaries positive, all ratings in range, no NULL IDs, salary raise applied,
rating adjustment correct, and deleted employees absent.
