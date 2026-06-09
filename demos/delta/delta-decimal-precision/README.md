# Delta Decimal Precision & Arithmetic

Demonstrates DECIMAL column precision and arithmetic in Delta tables
using a multinational financial ledger with strict decimal fidelity.

## Data Story

A multinational finance team tracks transactions in multiple currencies
with strict decimal precision. Amounts use DECIMAL(15,4), balances use
DECIMAL(18,6), and exchange rates use DECIMAL(10,8). No floating-point
drift is allowed — values must survive roundtrip read/write exactly.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `financial_ledger` | Delta Table | 40 | Multinational transaction ledger with precise decimal columns |

## Schema

**financial_ledger:** `id INT, account VARCHAR, description VARCHAR, amount DECIMAL(15,4), balance DECIMAL(18,6), exchange_rate DECIMAL(10,8), currency VARCHAR`

## Currencies & Exchange Rates

- **USD:** 20 rows, exchange_rate = 1.00000000
- **EUR:** 5 rows, exchange_rate = 1.08547321
- **GBP:** 5 rows, exchange_rate = 1.27145200
- **JPY:** 5 rows, exchange_rate = 0.00667800
- **CHF:** 5 rows, exchange_rate = 1.10234500

## Operations

1. INSERT 30 rows (USD, EUR, GBP) with precise decimal values
2. INSERT 10 rows (JPY, CHF) with edge-case decimal values
3. UPDATE — compute balance = ROUND(amount * exchange_rate, 6) for EUR + GBP rows
4. UPDATE — negate amounts for 5 refund transactions

## Verification

8 automated PASS/FAIL checks verify total row count, exact USD sum,
exchange rate precision roundtrip, computed balance correctness, refund
negation, 6-digit decimal scale preservation, currency count, and exact
EUR balance sum.
