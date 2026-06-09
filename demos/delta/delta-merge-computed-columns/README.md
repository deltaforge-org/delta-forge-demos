# Delta MERGE — Computed Columns & CASE Logic

Embed business rules directly inside MERGE UPDATE SET and INSERT VALUES using CASE expressions, arithmetic formulas, and conditional logic to compute derived columns at merge time.

## Data Story

A SaaS company manages subscriptions where tier, discount percentage, and priority score are derived from the raw plan amount and tenure. When renewals or new signups arrive, the MERGE computes all derived columns inline — ensuring consistent business logic for both updates and inserts.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| subscriptions | Delta | 12 | Current subscriptions with derived columns (target) |
| subscription_changes | Delta | 10 | Renewals and new signups (source) |

## Operations Demonstrated

1. **CASE in UPDATE SET** — Tier classification based on monthly_amount thresholds
2. **CASE in INSERT VALUES** — Same tier logic applied to new subscriptions at insert time
3. **Multi-column CASE** — Discount bracket based on months_active loyalty tiers
4. **Arithmetic in SET** — Priority score formula: `amount * (1 + months_active / 10.0)`
5. Consistent business rules across UPDATE and INSERT branches

## Verification

- Tier assignments verified against amount thresholds
- Discount brackets verified against tenure
- Priority scores computed and verified
- New subscriptions get same derived columns as updates
