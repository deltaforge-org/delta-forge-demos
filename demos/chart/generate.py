"""
Data + proof generator for the delta-chart-gallery demo.

Produces:
  - INSERT VALUES SQL for sales_daily and stock_prices tables
  - Pre-computed assertion values for every chart/query

Scenario: Mira's Mercantile, a 4-store regional retail chain, runs a
weekly executive dashboard that renders 10 chart types from unified
daily sales plus the parent company (ticker MIRA) stock OHLC.
"""

from collections import defaultdict
from datetime import date, timedelta

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

STORES = ["Downtown", "Mall", "Airport", "Beach"]
CATEGORIES = ["Apparel", "Electronics", "Home", "Beauty"]
DAYS = [date(2026, 3, 2) + timedelta(days=i) for i in range(5)]  # Mon-Fri

# revenue[store][category] baseline (USD)
BASE_REVENUE = {
    "Downtown":    {"Apparel": 1200.0, "Electronics": 2000.0, "Home": 1500.0, "Beauty":  800.0},
    "Mall":        {"Apparel": 1500.0, "Electronics": 2400.0, "Home": 1700.0, "Beauty": 1100.0},
    "Airport":     {"Apparel":  900.0, "Electronics": 1600.0, "Home": 1000.0, "Beauty":  600.0},
    "Beach":       {"Apparel": 1000.0, "Electronics": 1300.0, "Home": 1200.0, "Beauty":  700.0},
}

# Day factor by weekday index (Mon=0..Fri=4). Traffic builds through the week.
DAY_FACTOR = [1.00, 0.95, 1.05, 1.10, 1.20]

# Average unit price by category (USD per unit)
AVG_UNIT_PRICE = {"Apparel": 40.0, "Electronics": 200.0, "Home": 60.0, "Beauty": 25.0}

# Baseline discount by category. Beauty runs deeper promotions.
BASE_DISCOUNT = {"Apparel": 0.10, "Electronics": 0.05, "Home": 0.15, "Beauty": 0.20}

# Weekly stock prices for ticker MIRA (10 weeks, Jan-Mar 2026)
WEEK_STARTS = [date(2026, 1, 5) + timedelta(weeks=i) for i in range(10)]
# Deterministic OHLC sequence (open, high, low, close)
STOCK_OHLC = [
    (52.00, 54.50, 51.20, 53.80),
    (53.80, 55.40, 52.90, 54.60),
    (54.60, 55.10, 52.30, 52.70),
    (52.70, 53.20, 49.80, 50.10),
    (50.10, 51.00, 48.50, 50.70),
    (50.70, 53.60, 50.40, 53.20),
    (53.20, 56.80, 53.00, 56.40),
    (56.40, 58.20, 55.90, 57.90),
    (57.90, 59.70, 57.50, 59.10),
    (59.10, 61.40, 58.80, 60.80),
]


# ----------------------------------------------------------------------------
# Row generation
# ----------------------------------------------------------------------------

def round2(x: float) -> float:
    return round(x + 1e-9, 2)


def gen_sales_rows():
    """Yield sales_daily rows as dicts. Deterministic, 4x4x5 = 80 rows."""
    txn_id = 0
    for d_idx, d in enumerate(DAYS):
        day_factor = DAY_FACTOR[d_idx]
        for store in STORES:
            for cat in CATEGORIES:
                txn_id += 1
                base_rev = BASE_REVENUE[store][cat]
                raw_revenue = base_rev * day_factor
                # Discount goes up slightly later in the week
                discount = BASE_DISCOUNT[cat] + (0.02 if d_idx >= 3 else 0.0)
                # Net revenue after discount
                revenue = round2(raw_revenue * (1.0 - discount))
                # Units = raw_revenue / avg_price (ceiling, so it's an int)
                units_sold = int((raw_revenue + AVG_UNIT_PRICE[cat] - 1) // AVG_UNIT_PRICE[cat])
                # Customers: about half of units (rounded)
                customers = max(1, units_sold // 2)
                yield {
                    "txn_id": txn_id,
                    "txn_date": d,
                    "store_name": store,
                    "category": cat,
                    "units_sold": units_sold,
                    "revenue": revenue,
                    "discount_pct": round(discount, 2),
                    "customers": customers,
                }


def gen_stock_rows():
    for ws, (o, h, l, c) in zip(WEEK_STARTS, STOCK_OHLC):
        yield {
            "week_start": ws,
            "open_price": o,
            "high_price": h,
            "low_price": l,
            "close_price": c,
        }


# ----------------------------------------------------------------------------
# SQL emission
# ----------------------------------------------------------------------------

def sql_value(v):
    if isinstance(v, date):
        return f"'{v.isoformat()}'"
    if isinstance(v, str):
        return f"'{v}'"
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def emit_sales_insert(rows):
    lines = [
        "INSERT INTO {{zone_name}}.retail.sales_daily "
        "(txn_id, txn_date, store_name, category, units_sold, revenue, discount_pct, customers) VALUES"
    ]
    for i, r in enumerate(rows):
        tail = "," if i < len(rows) - 1 else ";"
        lines.append(
            f"    ({r['txn_id']}, "
            f"{sql_value(r['txn_date'])}, "
            f"{sql_value(r['store_name'])}, "
            f"{sql_value(r['category'])}, "
            f"{r['units_sold']}, "
            f"{r['revenue']:.2f}, "
            f"{r['discount_pct']:.2f}, "
            f"{r['customers']}){tail}"
        )
    return "\n".join(lines)


def emit_stock_insert(rows):
    lines = [
        "INSERT INTO {{zone_name}}.retail.stock_prices "
        "(week_start, open_price, high_price, low_price, close_price) VALUES"
    ]
    for i, r in enumerate(rows):
        tail = "," if i < len(rows) - 1 else ";"
        lines.append(
            f"    ({sql_value(r['week_start'])}, "
            f"{r['open_price']:.2f}, {r['high_price']:.2f}, "
            f"{r['low_price']:.2f}, {r['close_price']:.2f}){tail}"
        )
    return "\n".join(lines)


# ----------------------------------------------------------------------------
# Proof computation
# ----------------------------------------------------------------------------

def compute_proofs(sales, stock):
    p = {}
    p["row_count_sales"] = len(sales)
    p["row_count_stock"] = len(stock)

    # Category totals
    by_cat = defaultdict(lambda: {"rev": 0.0, "units": 0, "cust": 0})
    for r in sales:
        by_cat[r["category"]]["rev"] += r["revenue"]
        by_cat[r["category"]]["units"] += r["units_sold"]
        by_cat[r["category"]]["cust"] += r["customers"]
    p["by_cat"] = {k: {m: (round2(v) if isinstance(v, float) else v) for m, v in d.items()} for k, d in by_cat.items()}

    # Store totals
    by_store = defaultdict(lambda: {"rev": 0.0, "units": 0, "cust": 0, "disc_sum": 0.0, "rows": 0})
    for r in sales:
        by_store[r["store_name"]]["rev"] += r["revenue"]
        by_store[r["store_name"]]["units"] += r["units_sold"]
        by_store[r["store_name"]]["cust"] += r["customers"]
        by_store[r["store_name"]]["disc_sum"] += r["discount_pct"]
        by_store[r["store_name"]]["rows"] += 1
    for k, d in by_store.items():
        d["rev"] = round2(d["rev"])
        d["avg_disc"] = round(d["disc_sum"] / d["rows"], 4)
    p["by_store"] = dict(by_store)

    # Daily totals
    by_day = defaultdict(lambda: {"rev": 0.0, "units": 0, "cust": 0})
    for r in sales:
        by_day[r["txn_date"]]["rev"] += r["revenue"]
        by_day[r["txn_date"]]["units"] += r["units_sold"]
        by_day[r["txn_date"]]["cust"] += r["customers"]
    for k, d in by_day.items():
        d["rev"] = round2(d["rev"])
    p["by_day"] = {k.isoformat(): v for k, v in by_day.items()}

    # Store x category matrix (for HEATMAP)
    sxc = defaultdict(lambda: 0.0)
    for r in sales:
        sxc[(r["store_name"], r["category"])] += r["revenue"]
    p["store_cat_matrix"] = {f"{s}|{c}": round2(v) for (s, c), v in sxc.items()}

    # Grand totals
    p["total_revenue"] = round2(sum(r["revenue"] for r in sales))
    p["total_units"] = sum(r["units_sold"] for r in sales)
    p["total_customers"] = sum(r["customers"] for r in sales)

    # Extremes
    p["max_revenue_row"] = max(sales, key=lambda r: r["revenue"])
    p["min_revenue_row"] = min(sales, key=lambda r: r["revenue"])

    # Histogram sanity: per-row revenue range
    revenues = [r["revenue"] for r in sales]
    p["min_row_revenue"] = round2(min(revenues))
    p["max_row_revenue"] = round2(max(revenues))

    # Discount vs units correlation (Pearson) - used to sanity-check SCATTER
    n = len(sales)
    x = [r["discount_pct"] for r in sales]
    y = [r["units_sold"] for r in sales]
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    den_x = sum((xi - mean_x) ** 2 for xi in x) ** 0.5
    den_y = sum((yi - mean_y) ** 2 for yi in y) ** 0.5
    p["corr_discount_units"] = round(num / (den_x * den_y), 4) if den_x and den_y else 0.0

    # Day x category totals (for AREA chart)
    dxc = defaultdict(lambda: 0.0)
    for r in sales:
        dxc[(r["txn_date"], r["category"])] += r["revenue"]
    p["day_cat"] = {f"{d.isoformat()}|{c}": round2(v) for (d, c), v in dxc.items()}

    # Per-row revenue list (for histogram bucket checks). Sorted for inspection.
    revs = sorted(r["revenue"] for r in sales)
    p["rev_p25"] = revs[len(revs) // 4]
    p["rev_median"] = revs[len(revs) // 2]
    p["rev_p75"] = revs[(3 * len(revs)) // 4]

    # Count of rows in the lowest quartile bucket (<=600)
    p["rows_below_600"] = sum(1 for r in sales if r["revenue"] <= 600.0)
    p["rows_above_2000"] = sum(1 for r in sales if r["revenue"] >= 2000.0)

    # Stock proofs
    p["stock_weeks"] = len(stock)
    p["stock_first_open"] = stock[0]["open_price"]
    p["stock_last_close"] = stock[-1]["close_price"]
    p["stock_max_high"] = max(s["high_price"] for s in stock)
    p["stock_min_low"] = min(s["low_price"] for s in stock)
    # Weeks up (close > open)
    p["stock_up_weeks"] = sum(1 for s in stock if s["close_price"] > s["open_price"])
    # Max single-week gain
    p["stock_max_gain"] = round2(max(s["close_price"] - s["open_price"] for s in stock))

    return p


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys

    sales = list(gen_sales_rows())
    stock = list(gen_stock_rows())

    if len(sys.argv) > 1 and sys.argv[1] == "sql":
        print("-- sales_daily")
        print(emit_sales_insert(sales))
        print()
        print("-- stock_prices")
        print(emit_stock_insert(stock))
    else:
        proofs = compute_proofs(sales, stock)
        proofs["max_revenue_row"]["txn_date"] = proofs["max_revenue_row"]["txn_date"].isoformat()
        proofs["min_revenue_row"]["txn_date"] = proofs["min_revenue_row"]["txn_date"].isoformat()
        print(json.dumps(proofs, indent=2, default=str))
