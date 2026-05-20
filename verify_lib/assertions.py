"""
Assertion helpers for data verification scripts.

Provides pass/fail tracking and a library of typed assertions. The helpers
accept any of: PyArrow Table, pandas DataFrame, or PySpark DataFrame.
Inputs are normalised to PyArrow Table internally via _to_arrow().
"""

import sys

# Force UTF-8 output on Windows to support Unicode symbols
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


def _to_arrow(table):
    """Normalise input to a PyArrow Table.

    Accepts PyArrow Table (returned unchanged), pandas DataFrame
    (converted via pa.Table.from_pandas), or PySpark DataFrame
    (materialised via .toPandas() then converted).
    """
    if table is None:
        return None
    if hasattr(table, "num_rows") and hasattr(table, "column_names"):
        return table
    import pyarrow as pa
    if hasattr(table, "toPandas"):
        pdf = table.toPandas()
        return pa.Table.from_pandas(pdf, preserve_index=False)
    if hasattr(table, "columns") and hasattr(table, "shape"):
        return pa.Table.from_pandas(table, preserve_index=False)
    raise TypeError(
        f"verify_lib helpers accept PyArrow Table, pandas DataFrame, or "
        f"PySpark DataFrame; got {type(table).__name__}"
    )

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

_passed = 0
_failed = 0


def get_passed():
    return _passed


def get_failed():
    return _failed


def reset_counters():
    global _passed, _failed
    _passed = 0
    _failed = 0


def ok(msg):
    global _passed
    _passed += 1
    print(f"  {GREEN}\u2713{RESET} {msg}")


def fail(msg):
    global _failed
    _failed += 1
    print(f"  {RED}\u2717{RESET} {msg}")


def warn(msg):
    print(f"  {YELLOW}\u26a0{RESET} {msg}")


def info(msg):
    print(f"  {CYAN}\u2022{RESET} {msg}")


# ---------------------------------------------------------------------------
# Assertion functions
# ---------------------------------------------------------------------------

def assert_row_count(table, expected, label=""):
    table = _to_arrow(table)
    ctx = f" ({label})" if label else ""
    if table.num_rows == expected:
        ok(f"Row count = {expected}{ctx}")
    else:
        fail(f"Row count = {table.num_rows}, expected {expected}{ctx}")


def assert_sum(table, column, expected, label=""):
    table = _to_arrow(table)
    import pyarrow.compute as pc
    actual = round(pc.sum(table.column(column)).as_py(), 2)
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"SUM({column}) = {expected}{ctx}")
    else:
        fail(f"SUM({column}) = {actual}, expected {expected}{ctx}")


def assert_avg(table, column, expected, label=""):
    table = _to_arrow(table)
    import pyarrow.compute as pc
    actual = round(pc.mean(table.column(column)).as_py(), 2)
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"AVG({column}) = {expected}{ctx}")
    else:
        fail(f"AVG({column}) = {actual}, expected {expected}{ctx}")


def assert_min(table, column, expected, label=""):
    table = _to_arrow(table)
    import pyarrow.compute as pc
    actual = pc.min(table.column(column)).as_py()
    if isinstance(expected, float):
        actual = round(float(actual), 2)
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"MIN({column}) = {expected}{ctx}")
    else:
        fail(f"MIN({column}) = {actual}, expected {expected}{ctx}")


def assert_max(table, column, expected, label=""):
    table = _to_arrow(table)
    import pyarrow.compute as pc
    actual = pc.max(table.column(column)).as_py()
    if isinstance(expected, float):
        actual = round(float(actual), 2)
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"MAX({column}) = {expected}{ctx}")
    else:
        fail(f"MAX({column}) = {actual}, expected {expected}{ctx}")


def assert_distinct_count(table, column, expected, label=""):
    table = _to_arrow(table)
    import pyarrow.compute as pc
    actual = pc.count_distinct(table.column(column)).as_py()
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"DISTINCT({column}) = {expected}{ctx}")
    else:
        fail(f"DISTINCT({column}) = {actual}, expected {expected}{ctx}")


def assert_count_where(table, filter_col, filter_val, expected, label=""):
    table = _to_arrow(table)
    import pyarrow.compute as pc
    ctx = f" ({label})" if label else ""
    if callable(filter_val):
        actual = _count_callable(table, filter_col, filter_val)
        label_val = "<predicate>"
    else:
        mask = pc.equal(table.column(filter_col), filter_val)
        actual = pc.sum(mask).as_py()
        label_val = repr(filter_val)
    if actual == expected:
        ok(f"COUNT WHERE {filter_col} = {label_val} = {expected}{ctx}")
    else:
        fail(f"COUNT WHERE {filter_col} = {label_val} = {actual}, expected {expected}{ctx}")


def _count_callable(arrow_table, filter_col, predicate):
    """Apply predicate to each scalar value in the column, returning count of truthy results.

    Tries vectorised application first (pandas Series in -> Series out). If that
    raises (predicate uses scalar-only ops like int(), startswith(), 'and', etc.),
    falls back to element-wise apply.
    """
    series = arrow_table.column(filter_col).to_pandas()
    try:
        result = predicate(series)
        if hasattr(result, "sum") and hasattr(result, "__len__") and len(result) == len(series):
            return int(result.fillna(False).astype(bool).sum())
    except Exception:
        pass
    count = 0
    for v in series:
        try:
            if predicate(v):
                count += 1
        except Exception:
            pass
    return count


def assert_value_where(table, value_col, expected, filter_col, filter_val, label=""):
    """Assert a specific value in value_col where filter_col = filter_val."""
    table = _to_arrow(table)
    import pyarrow.compute as pc
    if callable(filter_val):
        series = table.column(filter_col).to_pandas()
        mask_list = []
        for v in series:
            try:
                mask_list.append(bool(filter_val(v)))
            except Exception:
                mask_list.append(False)
        import pyarrow as pa
        mask = pa.array(mask_list, type=pa.bool_())
    else:
        mask = pc.equal(table.column(filter_col), filter_val)
    filtered = table.filter(mask)
    if filtered.num_rows == 0:
        fail(f"No rows where {filter_col} matches filter ({label})")
        return
    actual = filtered.column(value_col)[0].as_py()
    if isinstance(expected, float):
        actual = round(float(actual), 2)
    ctx = f" ({label})" if label else ""
    label_val = "<predicate>" if callable(filter_val) else repr(filter_val)
    if actual == expected:
        ok(f"{value_col} = {expected!r} WHERE {filter_col} = {label_val}{ctx}")
    else:
        fail(f"{value_col} = {actual!r}, expected {expected!r} WHERE {filter_col} = {label_val}{ctx}")


def assert_format_version(metadata, expected):
    """Works for both Iceberg and Delta metadata.

    Iceberg: checks metadata["format-version"].
    Delta:   checks metadata["format"] == "delta" (always passes if expected == "delta").
    """
    if metadata.get("format") == "delta":
        # Delta table — the "version" in metadata is the commit version, not a format version.
        # If caller expects "delta", just confirm the format field.
        if expected == "delta":
            ok(f"Table format = delta")
        else:
            # Caller passed an integer expecting an Iceberg format-version on a Delta table
            fail(f"Table is Delta, but expected Iceberg format-version {expected}")
    else:
        actual = metadata.get("format-version", "?")
        if actual == expected:
            ok(f"Iceberg format version = {expected}")
        else:
            fail(f"Iceberg format version = {actual}, expected {expected}")


def assert_column_names(table, expected_names, label=""):
    ctx = f" ({label})" if label else ""
    if hasattr(table, "column_names"):
        actual = list(table.column_names)
    elif hasattr(table, "columns"):
        actual = list(table.columns)
    else:
        fail(f"assert_column_names: unsupported input type {type(table).__name__}{ctx}")
        return
    if actual == list(expected_names):
        ok(f"Column names match: {expected_names}{ctx}")
    else:
        fail(f"Column names = {actual}, expected {list(expected_names)}{ctx}")


def assert_null_count(table, column, expected, label=""):
    table = _to_arrow(table)
    import pyarrow.compute as pc
    actual = pc.sum(pc.is_null(table.column(column))).as_py()
    ctx = f" ({label})" if label else ""
    if actual == expected:
        ok(f"NULL count in {column} = {expected}{ctx}")
    else:
        fail(f"NULL count in {column} = {actual}, expected {expected}{ctx}")


def assert_not_null(table, column, label=""):
    table = _to_arrow(table)
    import pyarrow.compute as pc
    null_count = pc.sum(pc.is_null(table.column(column))).as_py()
    ctx = f" ({label})" if label else ""
    if null_count == 0:
        ok(f"No NULLs in {column}{ctx}")
    else:
        fail(f"Found {null_count} NULLs in {column}, expected 0{ctx}")
