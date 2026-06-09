# Delta Advanced SQL — Windows, CTEs & Analytics

Demonstrates advanced SQL analytics on Delta tables using window functions,
CTEs, and complex aggregations.

## Data Story

A stock market analyst tracks 5 tech stocks (AAPL, MSFT, GOOGL, AMZN, TSLA)
over 20 trading days in January 2024. Window functions reveal trends,
running totals, rankings, and volatility patterns.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `stock_prices` | Delta Table | 100 | 5 stocks × 20 days |

## Schema

**stock_prices:** `symbol VARCHAR, trade_date VARCHAR, open_price DOUBLE, close_price DOUBLE, high_price DOUBLE, low_price DOUBLE, volume BIGINT`

## SQL Features Demonstrated

1. **LAG** — day-over-day price change
2. **Running SUM** — cumulative volume
3. **RANK** — best daily gains
4. **FIRST_VALUE / LAST_VALUE** — monthly return calculation
5. **Moving AVG** — 3-day smoothed prices
6. **ROW_NUMBER** — latest price per stock
7. **CTEs** — multi-step volatility ranking, up/down day analysis
8. **FILTER** — conditional aggregation

## Verification

7 automated PASS/FAIL checks verify row counts, stock counts,
AAPL monthly return, TSLA volatility ranking, and known close prices.
