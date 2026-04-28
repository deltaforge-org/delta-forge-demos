-- ============================================================================
-- Demo: ODBC Driver Wire Benchmark Suite
-- ============================================================================
-- Every assertion below is derived from closed-form math against the
-- generation rule, never from an engine round trip. If the engine drifts on
-- counts, sums, or selected cell values, the corresponding ODBC code path is
-- the suspect.

-- ============================================================================
-- Query 1: bench.fixed_narrow row + sum + spot-check
-- ============================================================================
-- 50M-row pure-fixed-width baseline. SUM(rn) is the closed form
-- N*(N+1)/2 = 50_000_000 * 50_000_001 / 2.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 50000000
ASSERT VALUE sum_rn = 1250000025000000
ASSERT VALUE min_rn = 1
ASSERT VALUE max_rn = 50000000
SELECT
    COUNT(*) AS n_rows,
    SUM(rn) AS sum_rn,
    MIN(rn) AS min_rn,
    MAX(rn) AS max_rn
FROM {{zone_name}}.bench.fixed_narrow;

-- ============================================================================
-- Query 2: bench.fixed_narrow per-cell deterministic spot-check at rn=12345678
-- ============================================================================
-- Pins exact values for every column so a wire-decode or cast regression on
-- INT64/DOUBLE drops out immediately. 12345678 % 1024 = 334 because
-- 12056 * 1024 = 12345344 and 12345678 - 12345344 = 334.

ASSERT ROW_COUNT = 1
ASSERT VALUE i64_a = 12345678
ASSERT VALUE i64_b = 86419746
ASSERT VALUE i64_c = 334
ASSERT VALUE f64_a = 12345678.0
ASSERT VALUE f64_b = 6172839.0
ASSERT VALUE f64_c = 6.78
SELECT i64_a, i64_b, i64_c, f64_a, f64_b, f64_c
FROM {{zone_name}}.bench.fixed_narrow
WHERE rn = 12345678;

-- ============================================================================
-- Query 3: bench.fixed_wide row count + sum + boolean partition counts
-- ============================================================================
-- 5M rows, 60 fixed-width cols. Multiples of 2 in [1,5M] = 2_500_000;
-- multiples of 3 in [1,5M] = floor(5_000_000/3) = 1_666_666.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 5000000
ASSERT VALUE sum_rn = 12500002500000
ASSERT VALUE n_b01_true = 2500000
ASSERT VALUE n_b02_true = 1666666
SELECT
    COUNT(*) AS n_rows,
    SUM(rn) AS sum_rn,
    SUM(CASE WHEN b01 THEN 1 ELSE 0 END) AS n_b01_true,
    SUM(CASE WHEN b02 THEN 1 ELSE 0 END) AS n_b02_true
FROM {{zone_name}}.bench.fixed_wide;

-- ============================================================================
-- Query 4: bench.fixed_wide spot-check at rn=12345
-- ============================================================================
-- l01 = rn, l02 = rn+1. 12345 % 32767 = 12345 (since 12345 < 32767).
-- 12345 % 127: 12345 / 127 = 97 remainder 26. So s01 (rn%32767) = 12345 and
-- t01 (rn%127) = 26.

ASSERT ROW_COUNT = 1
ASSERT VALUE l01 = 12345
ASSERT VALUE l02 = 12346
ASSERT VALUE l12 = 12356
ASSERT VALUE i01 = 12345
ASSERT VALUE s01 = 12345
ASSERT VALUE t01 = 26
SELECT l01, l02, l12, i01, s01, t01
FROM {{zone_name}}.bench.fixed_wide
WHERE rn = 12345;

-- ============================================================================
-- Query 5: bench.string_narrow null density + length spot-check
-- ============================================================================
-- 10M rows, NULL when rn % 10 IN (0,1,2): exactly 30% NULL = 3_000_000 rows.
-- 7_000_000 non-null rows. md5*6 = 192 chars; lpad to 20 chars.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 10000000
ASSERT VALUE n_short_null = 3000000
ASSERT VALUE n_short_not_null = 7000000
ASSERT VALUE n_long_a_null = 3000000
SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN s_short  IS NULL THEN 1 ELSE 0 END) AS n_short_null,
    SUM(CASE WHEN s_short  IS NOT NULL THEN 1 ELSE 0 END) AS n_short_not_null,
    SUM(CASE WHEN s_long_a IS NULL THEN 1 ELSE 0 END) AS n_long_a_null
FROM {{zone_name}}.bench.string_narrow;

-- ============================================================================
-- Query 6: bench.string_narrow per-cell content + length at rn=12345
-- ============================================================================
-- 12345 % 10 = 5 so the row is non-null. lpad('12345', 20, '0') is fully
-- determined and must be exact.

ASSERT ROW_COUNT = 1
ASSERT VALUE s_short = '00000000000000012345'
ASSERT VALUE short_len = 20
ASSERT VALUE long_a_len = 192
ASSERT VALUE long_b_len = 192
SELECT
    s_short,
    LENGTH(s_short) AS short_len,
    LENGTH(s_long_a) AS long_a_len,
    LENGTH(s_long_b) AS long_b_len
FROM {{zone_name}}.bench.string_narrow
WHERE rn = 12345;

-- ============================================================================
-- Query 7: bench.string_narrow null witness at rn=10
-- ============================================================================
-- 10 % 10 = 0, so every string column is NULL by the generation rule.

ASSERT ROW_COUNT = 1
ASSERT VALUE s_short  IS NULL
ASSERT VALUE s_long_a IS NULL
ASSERT VALUE s_long_b IS NULL
ASSERT VALUE s_long_c IS NULL
ASSERT VALUE s_long_d IS NULL
SELECT s_short, s_long_a, s_long_b, s_long_c, s_long_d
FROM {{zone_name}}.bench.string_narrow
WHERE rn = 10;

-- ============================================================================
-- Query 8: bench.string_wide_kv null density + cell length
-- ============================================================================
-- 1M rows, NULL when rn % 20 = 0: exactly 5% NULL = 50_000 rows.
-- Each non-null cell is exactly 50 chars (lpad).

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 1000000
ASSERT VALUE n_k01_null = 50000
ASSERT VALUE n_k01_not_null = 950000
ASSERT VALUE n_k40_null = 50000
SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN k01 IS NULL THEN 1 ELSE 0 END) AS n_k01_null,
    SUM(CASE WHEN k01 IS NOT NULL THEN 1 ELSE 0 END) AS n_k01_not_null,
    SUM(CASE WHEN k40 IS NULL THEN 1 ELSE 0 END) AS n_k40_null
FROM {{zone_name}}.bench.string_wide_kv;

-- ============================================================================
-- Query 9: bench.string_wide_kv length and null spot at rn=1 and rn=20
-- ============================================================================
-- 1 % 20 = 1 (not null, len 50). 20 % 20 = 0 (null).

ASSERT ROW_COUNT = 2
ASSERT VALUE k01_len = 50 WHERE rn = 1
ASSERT VALUE k20_len = 50 WHERE rn = 1
ASSERT VALUE k40_len = 50 WHERE rn = 1
ASSERT VALUE k01_len IS NULL WHERE rn = 20
SELECT
    rn,
    LENGTH(k01) AS k01_len,
    LENGTH(k20) AS k20_len,
    LENGTH(k40) AS k40_len
FROM {{zone_name}}.bench.string_wide_kv
WHERE rn IN (1, 20)
ORDER BY rn;

-- ============================================================================
-- Query 10: bench.string_long row count + per-cell length
-- ============================================================================
-- 100K rows. Every cell is exactly md5 (32 chars) repeated 200 times = 6400
-- chars. Tests SQLGetData chunked-read path with cells well over typical
-- buf_len boundaries.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 100000
ASSERT VALUE sum_rn = 5000050000
ASSERT VALUE min_s1_len = 6400
ASSERT VALUE max_s1_len = 6400
ASSERT VALUE min_s4_len = 6400
SELECT
    COUNT(*) AS n_rows,
    SUM(rn) AS sum_rn,
    MIN(LENGTH(s1)) AS min_s1_len,
    MAX(LENGTH(s1)) AS max_s1_len,
    MIN(LENGTH(s4)) AS min_s4_len
FROM {{zone_name}}.bench.string_long;

-- ============================================================================
-- Query 11: bench.binary_blobs row count + per-cell byte length envelopes
-- ============================================================================
-- b1 size = 32 * (1 + rn%32)  -> [32, 1024] bytes
-- b2 size = 32 * (1 + rn%64)  -> [32, 2048] bytes
-- b3 size = 32 * (1 + rn%2048) -> [32, 65536] bytes
-- N=50_000. SUM(rn) closed form = 1_250_025_000.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 50000
ASSERT VALUE sum_rn = 1250025000
ASSERT VALUE min_b1_len = 32
ASSERT VALUE max_b1_len = 1024
ASSERT VALUE min_b2_len = 32
ASSERT VALUE max_b2_len = 2048
ASSERT VALUE min_b3_len = 32
SELECT
    COUNT(*) AS n_rows,
    SUM(rn) AS sum_rn,
    MIN(OCTET_LENGTH(b1)) AS min_b1_len,
    MAX(OCTET_LENGTH(b1)) AS max_b1_len,
    MIN(OCTET_LENGTH(b2)) AS min_b2_len,
    MAX(OCTET_LENGTH(b2)) AS max_b2_len,
    MIN(OCTET_LENGTH(b3)) AS min_b3_len
FROM {{zone_name}}.bench.binary_blobs;

-- ============================================================================
-- Query 12: bench.decimal_temporal row + sum + spot-check
-- ============================================================================
-- 5M rows. SUM(rn) closed form = 12_500_002_500_000. d_d at rn=12345:
-- d_d = (12345 % 1_000_000) + 0.000000001 = 12345.000000001.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 5000000
ASSERT VALUE sum_rn = 12500002500000
SELECT
    COUNT(*) AS n_rows,
    SUM(rn) AS sum_rn
FROM {{zone_name}}.bench.decimal_temporal;

ASSERT ROW_COUNT = 1
ASSERT VALUE d_a = 12345.123456789
ASSERT VALUE d_d = 12345.000000001
SELECT d_a, d_d
FROM {{zone_name}}.bench.decimal_temporal
WHERE rn = 12345;

-- ============================================================================
-- Query 13: bench.nested_json row + sum
-- ============================================================================
-- 500K rows. STRUCT/ARRAY/MAP/VARIANT exercise the format-bound wire path.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 500000
ASSERT VALUE sum_rn = 125000250000
SELECT
    COUNT(*) AS n_rows,
    SUM(rn) AS sum_rn
FROM {{zone_name}}.bench.nested_json;

-- ============================================================================
-- Query 14: bench.null_heavy null density across all 30 nullable columns
-- ============================================================================
-- 5M rows, 30 nullable cols. Each col populated only when rn%20=0, so
-- non-null = 250_000 and null = 4_750_000 per column. We sample three cols
-- of different physical layouts (BIGINT, STRING, DECIMAL).

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 5000000
ASSERT VALUE n_l01_not_null = 250000
ASSERT VALUE n_l01_null = 4750000
ASSERT VALUE n_s01_not_null = 250000
ASSERT VALUE n_m01_not_null = 250000
ASSERT VALUE n_bo01_not_null = 250000
SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN l01  IS NOT NULL THEN 1 ELSE 0 END) AS n_l01_not_null,
    SUM(CASE WHEN l01  IS NULL     THEN 1 ELSE 0 END) AS n_l01_null,
    SUM(CASE WHEN s01  IS NOT NULL THEN 1 ELSE 0 END) AS n_s01_not_null,
    SUM(CASE WHEN m01  IS NOT NULL THEN 1 ELSE 0 END) AS n_m01_not_null,
    SUM(CASE WHEN bo01 IS NOT NULL THEN 1 ELSE 0 END) AS n_bo01_not_null
FROM {{zone_name}}.bench.null_heavy;

-- ============================================================================
-- Query 15: bench.null_heavy populated witness at rn=20
-- ============================================================================
-- 20 % 20 = 0 so every col is populated by the generation rule.

ASSERT ROW_COUNT = 1
ASSERT VALUE l01 = 20
ASSERT VALUE l02 = 21
ASSERT VALUE i01 = 20
ASSERT VALUE s01 = '20'
SELECT l01, l02, i01, s01
FROM {{zone_name}}.bench.null_heavy
WHERE rn = 20;

-- ============================================================================
-- Query 16: bench.skewed_strings skew distribution
-- ============================================================================
-- 5M rows. skew is 100_000 chars when rn%100=0, otherwise CAST(rn AS STRING)
-- which is 1..7 chars for rn in [1, 5_000_000]. Long-cell count = 50_000.

ASSERT ROW_COUNT = 1
ASSERT VALUE n_rows = 5000000
ASSERT VALUE n_long_skew = 50000
ASSERT VALUE n_short_skew = 4950000
ASSERT VALUE max_skew_len = 100000
SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN LENGTH(skew) = 100000 THEN 1 ELSE 0 END) AS n_long_skew,
    SUM(CASE WHEN LENGTH(skew) <= 7     THEN 1 ELSE 0 END) AS n_short_skew,
    MAX(LENGTH(skew)) AS max_skew_len
FROM {{zone_name}}.bench.skewed_strings;

-- ============================================================================
-- Query 17: bench.skewed_strings spot-check at rn=100 and rn=101
-- ============================================================================
-- 100 % 100 = 0 -> 100_000-char skew cell (the 1% long-cell case).
-- 101 % 100 = 1 -> CAST(101 AS STRING) = '101' (3 chars).

ASSERT ROW_COUNT = 2
ASSERT VALUE skew_len = 100000 WHERE rn = 100
ASSERT VALUE skew_len = 3      WHERE rn = 101
ASSERT VALUE c1_len = 3        WHERE rn = 100
ASSERT VALUE c5_len = 32       WHERE rn = 100
SELECT
    rn,
    LENGTH(skew) AS skew_len,
    LENGTH(c1) AS c1_len,
    LENGTH(c5) AS c5_len
FROM {{zone_name}}.bench.skewed_strings
WHERE rn IN (100, 101)
ORDER BY rn;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One row per benchmark table, each pinned to its closed-form row count and
-- (where applicable) closed-form SUM(rn). If any number drifts, the table
-- name in that row points to the regressing wire path.

ASSERT ROW_COUNT = 10
ASSERT RESULT SET INCLUDES
    ('fixed_narrow',     50000000, 1250000025000000),
    ('fixed_wide',        5000000,   12500002500000),
    ('string_narrow',    10000000,   50000005000000),
    ('string_wide_kv',    1000000,     500000500000),
    ('string_long',        100000,       5000050000),
    ('binary_blobs',        50000,       1250025000),
    ('decimal_temporal',  5000000,   12500002500000),
    ('nested_json',        500000,     125000250000),
    ('null_heavy',        5000000,   12500002500000),
    ('skewed_strings',    5000000,   12500002500000)
SELECT 'fixed_narrow'     AS tbl, COUNT(*) AS n, SUM(rn) AS s FROM {{zone_name}}.bench.fixed_narrow
UNION ALL SELECT 'fixed_wide',       COUNT(*), SUM(rn) FROM {{zone_name}}.bench.fixed_wide
UNION ALL SELECT 'string_narrow',    COUNT(*), SUM(rn) FROM {{zone_name}}.bench.string_narrow
UNION ALL SELECT 'string_wide_kv',   COUNT(*), SUM(rn) FROM {{zone_name}}.bench.string_wide_kv
UNION ALL SELECT 'string_long',      COUNT(*), SUM(rn) FROM {{zone_name}}.bench.string_long
UNION ALL SELECT 'binary_blobs',     COUNT(*), SUM(rn) FROM {{zone_name}}.bench.binary_blobs
UNION ALL SELECT 'decimal_temporal', COUNT(*), SUM(rn) FROM {{zone_name}}.bench.decimal_temporal
UNION ALL SELECT 'nested_json',      COUNT(*), SUM(rn) FROM {{zone_name}}.bench.nested_json
UNION ALL SELECT 'null_heavy',       COUNT(*), SUM(rn) FROM {{zone_name}}.bench.null_heavy
UNION ALL SELECT 'skewed_strings',   COUNT(*), SUM(rn) FROM {{zone_name}}.bench.skewed_strings;
