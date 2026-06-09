#!/usr/bin/env python3
"""Analyze Iceberg table parquet files to compute values for ASSERT statements."""

import duckdb

DATA_DIR = "/home/chess/delta-forge/delta-forge-demos/demos/iceberg/v2/iceberg-native-time-travel/stock_prices/data"

INITIAL    = f"{DATA_DIR}/00000-0-1752b09e-560a-4a63-bf45-a389cd057a36-0-00001.parquet"
SNAP2_DATA = f"{DATA_DIR}/00000-4-b4735b53-d586-48c7-821e-b5574e499cb4-00001.parquet"
SNAP2_DEL  = f"{DATA_DIR}/00000-4-b4735b53-d586-48c7-821e-b5574e499cb4-00001-deletes.parquet"
SNAP3_DATA = f"{DATA_DIR}/00000-7-906a1593-adef-4edd-9729-56eadb53676e-0-00001.parquet"
SNAP4_DEL  = f"{DATA_DIR}/00000-11-505ceaa9-a7ac-4a5e-9dcb-f323c725b4ec-00001-deletes.parquet"

con = duckdb.connect()

# ---------------------------------------------------------------------------
# Step 0: Inspect schemas
# ---------------------------------------------------------------------------
print("=== SCHEMA: initial data file ===")
print(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{INITIAL}')").fetchdf().to_string())

print("\n=== SCHEMA: snap2 delete file ===")
print(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{SNAP2_DEL}')").fetchdf().to_string())

print("\n=== SCHEMA: snap4 delete file ===")
print(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{SNAP4_DEL}')").fetchdf().to_string())

# ---------------------------------------------------------------------------
# Step 1: Load all source data with row numbers
# ---------------------------------------------------------------------------

# Initial file with 0-based row positions
con.execute(f"""
CREATE TABLE initial_with_pos AS
SELECT
    row_number() OVER () - 1 AS _pos,
    ticker, company_name, price, volume, market_cap, sector, trade_date
FROM read_parquet('{INITIAL}')
""")

print(f"\n=== Initial file row count: {con.execute('SELECT COUNT(*) FROM initial_with_pos').fetchone()[0]} ===")

# Snap2 deletes (positions in initial file)
con.execute(f"""
CREATE TABLE snap2_deletes AS
SELECT file_path, pos
FROM read_parquet('{SNAP2_DEL}')
""")
print(f"Snap2 deletes count: {con.execute('SELECT COUNT(*) FROM snap2_deletes').fetchone()[0]}")
print("Snap2 delete file_path values (distinct):")
print(con.execute("SELECT DISTINCT file_path FROM snap2_deletes").fetchdf().to_string())

# Snap4 deletes
con.execute(f"""
CREATE TABLE snap4_deletes AS
SELECT file_path, pos
FROM read_parquet('{SNAP4_DEL}')
""")
print(f"Snap4 deletes count: {con.execute('SELECT COUNT(*) FROM snap4_deletes').fetchone()[0]}")
print("Snap4 delete file_path values (distinct):")
print(con.execute("SELECT DISTINCT file_path FROM snap4_deletes").fetchdf().to_string())

# Snap2 replacement data
con.execute(f"""
CREATE TABLE snap2_data AS
SELECT ticker, company_name, price, volume, market_cap, sector, trade_date
FROM read_parquet('{SNAP2_DATA}')
""")
print(f"\nSnap2 replacement data count: {con.execute('SELECT COUNT(*) FROM snap2_data').fetchone()[0]}")

# Snap3 IPO data
con.execute(f"""
CREATE TABLE snap3_data AS
SELECT ticker, company_name, price, volume, market_cap, sector, trade_date
FROM read_parquet('{SNAP3_DATA}')
""")
print(f"Snap3 IPO data count: {con.execute('SELECT COUNT(*) FROM snap3_data').fetchone()[0]}")

# ---------------------------------------------------------------------------
# Step 2: Apply snap2 deletes to initial, then snap4 deletes
# ---------------------------------------------------------------------------

# The delete file's file_path points to the initial parquet file.
# We use position-based deletion.
con.execute("""
CREATE TABLE after_snap2 AS
SELECT ticker, company_name, price, volume, market_cap, sector, trade_date
FROM initial_with_pos
WHERE _pos NOT IN (
    SELECT pos FROM snap2_deletes
)
""")
print(f"\nAfter snap2 deletes applied: {con.execute('SELECT COUNT(*) FROM after_snap2').fetchone()[0]} rows")

# Combine: after_snap2 + snap2_data + snap3_data = snapshot-3 state
con.execute("""
CREATE TABLE after_snap3 AS
SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM after_snap2
UNION ALL
SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM snap2_data
UNION ALL
SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM snap3_data
""")
print(f"After snap3 (before snap4 deletes): {con.execute('SELECT COUNT(*) FROM after_snap3').fetchone()[0]} rows")

# Snap4 deletes are position-based against snap3_data file
# Check what file_path the snap4 deletes reference
snap4_file_paths = con.execute("SELECT DISTINCT file_path FROM snap4_deletes").fetchdf()
print("\nSnap4 delete file_path values:")
print(snap4_file_paths.to_string())

# Snap4 deletes reference the snap3 data file (IPO rows) or the snap2 data file?
# Actually COP/SLB are in the initial data. Let's check which tickers are at those positions.
print("\nSnap2 delete positions:")
print(con.execute("SELECT pos FROM snap2_deletes ORDER BY pos").fetchdf().to_string())
print("\nSnap4 delete positions:")
print(con.execute("SELECT pos FROM snap4_deletes ORDER BY pos").fetchdf().to_string())

# Let's look at the tickers at snap2-deleted positions in initial
print("\nInitial rows at snap2-deleted positions (these are the tech stocks being replaced):")
print(con.execute("""
    SELECT i.* FROM initial_with_pos i
    JOIN snap2_deletes d ON i._pos = d.pos
    ORDER BY i._pos
""").fetchdf().to_string())

# For snap4, the file_path in deletes tells us which data file to look at
# Let's find COP and SLB in the initial data
print("\nCOP and SLB in initial data (to understand snap4 deletes):")
print(con.execute("SELECT _pos, ticker, sector FROM initial_with_pos WHERE ticker IN ('COP','SLB')").fetchdf().to_string())

# Count COP and SLB before deletion
print("\nCOP and SLB row counts BEFORE deletion:")
cop_slb = con.execute("SELECT ticker, COUNT(*) as cnt FROM after_snap3 WHERE ticker IN ('COP','SLB') GROUP BY ticker").fetchdf()
print(cop_slb.to_string())
print(f"Total COP+SLB rows before deletion: {cop_slb['cnt'].sum()}")

# ---------------------------------------------------------------------------
# Step 3: Build final state (snap4 deletes applied)
# ---------------------------------------------------------------------------
# snap4 deletes target rows by position in a specific file.
# We need to figure out which file and apply accordingly.

# Add row numbers to after_snap3 for position-based deletion
# First, check the snap4 delete file_path to determine which source file it targets
snap4_fp = snap4_file_paths['file_path'].iloc[0] if not snap4_file_paths.empty else ''
print(f"\nSnap4 targets file: {snap4_fp}")

# The snap4 deletes are position deletes against the INITIAL file
# because COP and SLB were never updated — they exist in after_snap2 (surviving rows from initial)
# But in merge-on-read, position deletes always reference the specific data file.
# Let's verify by checking if snap4_fp contains the initial file name.

initial_filename = "00000-0-1752b09e-560a-4a63-bf45-a389cd057a36-0-00001.parquet"
snap2_data_filename = "00000-4-b4735b53-d586-48c7-821e-b5574e499cb4-00001.parquet"
snap3_data_filename = "00000-7-906a1593-adef-4edd-9729-56eadb53676e-0-00001.parquet"

if initial_filename in snap4_fp:
    print("Snap4 deletes target the INITIAL data file")
    # Apply: remove from initial_with_pos at those positions
    con.execute("""
    CREATE TABLE final_state AS
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date
    FROM initial_with_pos
    WHERE _pos NOT IN (SELECT pos FROM snap2_deletes)
      AND _pos NOT IN (SELECT pos FROM snap4_deletes)
    UNION ALL
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM snap2_data
    UNION ALL
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM snap3_data
    """)
elif snap2_data_filename in snap4_fp:
    print("Snap4 deletes target the SNAP2 data file")
    con.execute(f"""
    CREATE TABLE snap2_data_with_pos AS
    SELECT row_number() OVER () - 1 AS _pos, ticker, company_name, price, volume, market_cap, sector, trade_date
    FROM read_parquet('{SNAP2_DATA}')
    """)
    con.execute("""
    CREATE TABLE final_state AS
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM after_snap2
    UNION ALL
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date
    FROM snap2_data_with_pos
    WHERE _pos NOT IN (SELECT pos FROM snap4_deletes)
    UNION ALL
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM snap3_data
    """)
elif snap3_data_filename in snap4_fp:
    print("Snap4 deletes target the SNAP3 data file")
    con.execute(f"""
    CREATE TABLE snap3_data_with_pos AS
    SELECT row_number() OVER () - 1 AS _pos, ticker, company_name, price, volume, market_cap, sector, trade_date
    FROM read_parquet('{SNAP3_DATA}')
    """)
    con.execute("""
    CREATE TABLE final_state AS
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM after_snap2
    UNION ALL
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM snap2_data
    UNION ALL
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date
    FROM snap3_data_with_pos
    WHERE _pos NOT IN (SELECT pos FROM snap4_deletes)
    """)
else:
    print(f"WARNING: snap4 file_path doesn't match known files: {snap4_fp}")
    # Fallback: try to find COP/SLB positions and remove from after_snap3
    con.execute("""
    CREATE TABLE final_state AS
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date FROM after_snap3
    WHERE ticker NOT IN ('COP','SLB')
    """)

print(f"\nFinal state row count: {con.execute('SELECT COUNT(*) FROM final_state').fetchone()[0]}")

# ---------------------------------------------------------------------------
# Step 4: Verify COP/SLB are absent
# ---------------------------------------------------------------------------
print("\n=== Query 5 verification: COP/SLB in final state ===")
cop_slb_final = con.execute("SELECT ticker, COUNT(*) FROM final_state WHERE ticker IN ('COP','SLB') GROUP BY ticker").fetchdf()
print(cop_slb_final.to_string() if not cop_slb_final.empty else "COP and SLB: 0 rows (correct)")

# ---------------------------------------------------------------------------
# Step 5: Query 1 — pick 3 representative rows
# ---------------------------------------------------------------------------
print("\n=== Query 1: 3 representative rows for ASSERT ===")

# One from original data that wasn't updated (not tech sector, not IPO)
# Snap2 updated tech stocks. IPO stocks are in snap3.
# Let's identify which sectors were tech-updated
tech_tickers_replaced = con.execute("""
    SELECT ticker FROM initial_with_pos WHERE _pos IN (SELECT pos FROM snap2_deletes)
""").fetchdf()['ticker'].tolist()

print(f"Tech tickers that were replaced (snap2): {sorted(tech_tickers_replaced)}")

# IPO tickers
ipo_tickers = con.execute("SELECT DISTINCT ticker FROM snap3_data ORDER BY ticker").fetchdf()['ticker'].tolist()
print(f"IPO tickers (snap3): {sorted(ipo_tickers)}")

# Original unchanged row (not in tech_replaced, not IPO, not COP/SLB)
excluded = set(tech_tickers_replaced) | set(ipo_tickers) | {'COP', 'SLB'}
print("\nRow 1 candidate (original, unchanged):")
original_row = con.execute(f"""
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date
    FROM final_state
    WHERE ticker NOT IN ({', '.join(repr(t) for t in excluded)})
    ORDER BY ticker
    LIMIT 1
""").fetchdf()
print(original_row.to_string())

# One tech stock that was updated (+5%)
print("\nRow 2 candidate (tech stock, +5% updated):")
tech_row = con.execute(f"""
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date
    FROM final_state
    WHERE ticker IN ({', '.join(repr(t) for t in tech_tickers_replaced)})
    ORDER BY ticker
    LIMIT 1
""").fetchdf()
print(tech_row.to_string())

# One IPO stock
print("\nRow 3 candidate (IPO stock):")
ipo_row = con.execute(f"""
    SELECT ticker, company_name, price, volume, market_cap, sector, trade_date
    FROM final_state
    WHERE ticker IN ({', '.join(repr(t) for t in ipo_tickers)})
    ORDER BY ticker
    LIMIT 1
""").fetchdf()
print(ipo_row.to_string())

# ---------------------------------------------------------------------------
# Step 6: Query 6 — IPO tickers detail
# ---------------------------------------------------------------------------
print("\n=== Query 6: IPO tickers (BIOT, FINX, GRNH, NWAI, QCMP) ===")
ipo_query_tickers = ['BIOT', 'FINX', 'GRNH', 'NWAI', 'QCMP']

ipo_detail = con.execute(f"""
    SELECT ticker, company_name, price, sector, trade_date
    FROM final_state
    WHERE ticker IN ({', '.join(repr(t) for t in ipo_query_tickers)})
    ORDER BY ticker, trade_date
""").fetchdf()
print(ipo_detail.to_string())

# First trade_date for each IPO ticker
print("\nFirst trade_date per IPO ticker:")
ipo_first = con.execute(f"""
    SELECT ticker, MIN(trade_date) AS first_trade_date, sector
    FROM final_state
    WHERE ticker IN ({', '.join(repr(t) for t in ipo_query_tickers)})
    GROUP BY ticker, sector
    ORDER BY ticker
""").fetchdf()
print(ipo_first.to_string())

# Price on that first date
print("\nPrice on first trade_date per IPO ticker:")
ipo_price_first = con.execute(f"""
    SELECT f.ticker, f.price, f.sector, f.trade_date
    FROM final_state f
    JOIN (
        SELECT ticker, MIN(trade_date) AS first_trade_date
        FROM final_state
        WHERE ticker IN ({', '.join(repr(t) for t in ipo_query_tickers)})
        GROUP BY ticker
    ) fm ON f.ticker = fm.ticker AND f.trade_date = fm.first_trade_date
    ORDER BY f.ticker
""").fetchdf()
print(ipo_price_first.to_string())

# ---------------------------------------------------------------------------
# Step 7: Summary printout for ASSERT statements
# ---------------------------------------------------------------------------
print("\n" + "="*70)
print("SUMMARY FOR ASSERT STATEMENTS")
print("="*70)

print(f"\nQuery 1 — SELECT * FROM stock_prices — ROW_COUNT=138:")
final_count = con.execute("SELECT COUNT(*) FROM final_state").fetchone()[0]
print(f"  Actual final row count: {final_count}")

if not original_row.empty:
    r = original_row.iloc[0]
    print(f"\n  Row A (original, unchanged):")
    print(f"    ticker='{r['ticker']}', price={r['price']}, trade_date='{r['trade_date']}'")
    print(f"    sector='{r['sector']}', company_name='{r['company_name']}'")

if not tech_row.empty:
    r = tech_row.iloc[0]
    print(f"\n  Row B (tech stock, +5% updated):")
    print(f"    ticker='{r['ticker']}', price={r['price']}, trade_date='{r['trade_date']}'")
    print(f"    sector='{r['sector']}', company_name='{r['company_name']}'")
    # Show original price for reference
    orig_price_rows = con.execute(f"SELECT price FROM initial_with_pos WHERE ticker='{r['ticker']}' LIMIT 1").fetchone()
    if orig_price_rows:
        print(f"    original price (before +5%): {orig_price_rows[0]}")

if not ipo_row.empty:
    r = ipo_row.iloc[0]
    print(f"\n  Row C (IPO stock):")
    print(f"    ticker='{r['ticker']}', price={r['price']}, trade_date='{r['trade_date']}'")
    print(f"    sector='{r['sector']}', company_name='{r['company_name']}'")

print(f"\nQuery 5 — WHERE ticker IN ('COP','SLB') — ROW_COUNT=0:")
cop_count = con.execute("SELECT COUNT(*) FROM initial_with_pos WHERE ticker='COP'").fetchone()[0]
slb_count = con.execute("SELECT COUNT(*) FROM initial_with_pos WHERE ticker='SLB'").fetchone()[0]
print(f"  COP rows before deletion: {cop_count}")
print(f"  SLB rows before deletion: {slb_count}")
print(f"  Total COP+SLB before deletion: {cop_count + slb_count}")

print(f"\nQuery 6 — WHERE ticker IN ('BIOT','FINX','GRNH','NWAI','QCMP') — ROW_COUNT=30:")
print(f"  IPO ticker prices and sectors on first trade_date:")
print(ipo_price_first.to_string())

print(f"\nQuery 8 — DESCRIBE HISTORY:")
print(f"  Expected snapshot count: 4")

# Extra: show all final_state sectors for reference
print("\n=== Final state sector distribution ===")
print(con.execute("SELECT sector, COUNT(*) as cnt FROM final_state GROUP BY sector ORDER BY cnt DESC").fetchdf().to_string())

# Show a sample of the final state
print("\n=== Final state sample (first 10 rows ordered by ticker) ===")
print(con.execute("SELECT * FROM final_state ORDER BY ticker, trade_date LIMIT 10").fetchdf().to_string())
