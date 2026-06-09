# Delta Computed Fields — Formulas, Defaults & Derived Values

Demonstrates computed columns using CTE formulas in INSERT statements
for consistent derived values.

## Data Story

A sales team tracks invoices with automatically computed subtotals,
discounts, totals, and commissions. Bulk orders (ids 41-50) get higher
discount tiers (15-25%). Top performer Sarah earns an 8% bonus commission
on orders over $500.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `sales_invoices` | Delta Table | 50 | Invoices with computed fields |

## Computed Formulas

| Column | Formula |
|--------|---------|
| subtotal | qty * unit_price |
| discount_amt | subtotal * discount_pct / 100 |
| total | subtotal - discount_amt |
| commission | total * 0.05 (or 0.08 for Sarah bonus) |

## Verification

7 automated PASS/FAIL checks verify formula correctness across all rows,
spot-check specific values, and validate commission tier rules.
