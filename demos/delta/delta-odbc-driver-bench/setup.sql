-- ==========================================================================
-- Demo: ODBC Driver Wire Benchmark Suite
-- Feature: Ten Delta tables, ~3.27M rows total, each isolating one ODBC wire
--          dimension so a regression on byte counts or throughput points at
--          one code path. Every cell is row_number-derived: two runs are
--          bit-identical and any drift is a real regression.
--
-- Sizes are sized to stay safely under Arrow's 2GB i32-offset limit per
-- StringArray batch. Scale up table-by-table once the engine path is proven.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'ODBC driver wire-benchmark zone';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.bench
    COMMENT 'Benchmark tables that isolate one ODBC wire dimension at a time';

-- --------------------------------------------------------------------------
-- Table 1: bench.fixed_narrow
-- 1M rows, 8 cols, all INT64/DOUBLE, no nulls.
-- Stresses driver upper bound: pure decode + memcpy. Speed-of-light baseline.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.fixed_narrow (
    rn    BIGINT NOT NULL,
    i64_a BIGINT NOT NULL,
    i64_b BIGINT NOT NULL,
    i64_c BIGINT NOT NULL,
    f64_a DOUBLE NOT NULL,
    f64_b DOUBLE NOT NULL,
    f64_c DOUBLE NOT NULL,
    f64_d DOUBLE NOT NULL
)
LOCATION '{{data_path}}/bench/fixed_narrow';

-- --------------------------------------------------------------------------
-- Table 2: bench.fixed_wide
-- 100K rows, 60 cols, all fixed-width primitives.
-- Stresses per-cell overhead at scale and the column slab cache hit path.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.fixed_wide (
    rn   BIGINT NOT NULL,
    l01 BIGINT NOT NULL, l02 BIGINT NOT NULL, l03 BIGINT NOT NULL, l04 BIGINT NOT NULL,
    l05 BIGINT NOT NULL, l06 BIGINT NOT NULL, l07 BIGINT NOT NULL, l08 BIGINT NOT NULL,
    l09 BIGINT NOT NULL, l10 BIGINT NOT NULL, l11 BIGINT NOT NULL, l12 BIGINT NOT NULL,
    i01 INT NOT NULL, i02 INT NOT NULL, i03 INT NOT NULL, i04 INT NOT NULL,
    i05 INT NOT NULL, i06 INT NOT NULL, i07 INT NOT NULL, i08 INT NOT NULL,
    i09 INT NOT NULL, i10 INT NOT NULL, i11 INT NOT NULL, i12 INT NOT NULL,
    s01 SMALLINT NOT NULL, s02 SMALLINT NOT NULL, s03 SMALLINT NOT NULL, s04 SMALLINT NOT NULL,
    s05 SMALLINT NOT NULL, s06 SMALLINT NOT NULL, s07 SMALLINT NOT NULL, s08 SMALLINT NOT NULL,
    t01 TINYINT NOT NULL, t02 TINYINT NOT NULL, t03 TINYINT NOT NULL, t04 TINYINT NOT NULL,
    t05 TINYINT NOT NULL, t06 TINYINT NOT NULL, t07 TINYINT NOT NULL, t08 TINYINT NOT NULL,
    d01 DOUBLE NOT NULL, d02 DOUBLE NOT NULL, d03 DOUBLE NOT NULL, d04 DOUBLE NOT NULL,
    d05 DOUBLE NOT NULL, d06 DOUBLE NOT NULL, d07 DOUBLE NOT NULL, d08 DOUBLE NOT NULL,
    f01 FLOAT NOT NULL, f02 FLOAT NOT NULL, f03 FLOAT NOT NULL, f04 FLOAT NOT NULL,
    f05 FLOAT NOT NULL, f06 FLOAT NOT NULL, f07 FLOAT NOT NULL, f08 FLOAT NOT NULL,
    b01 BOOLEAN NOT NULL, b02 BOOLEAN NOT NULL,
    da01 DATE NOT NULL, da02 DATE NOT NULL
)
LOCATION '{{data_path}}/bench/fixed_wide';

-- --------------------------------------------------------------------------
-- Table 3: bench.string_narrow
-- 500K rows, 5 cols (one short + four long), ~30% NULL density.
-- Stresses the UTF-8 decode hot path and the indicator-array path with
-- realistic null sparsity.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.string_narrow (
    rn       BIGINT NOT NULL,
    s_short  STRING,
    s_long_a STRING,
    s_long_b STRING,
    s_long_c STRING,
    s_long_d STRING
)
LOCATION '{{data_path}}/bench/string_narrow';

-- --------------------------------------------------------------------------
-- Table 4: bench.string_wide_kv
-- 100K rows, 40 cols, all ~50-char UTF-8, ~5% NULL.
-- Stresses indicator-array path plus UTF-8 cost when most cells are present.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.string_wide_kv (
    rn  BIGINT NOT NULL,
    k01 STRING, k02 STRING, k03 STRING, k04 STRING, k05 STRING,
    k06 STRING, k07 STRING, k08 STRING, k09 STRING, k10 STRING,
    k11 STRING, k12 STRING, k13 STRING, k14 STRING, k15 STRING,
    k16 STRING, k17 STRING, k18 STRING, k19 STRING, k20 STRING,
    k21 STRING, k22 STRING, k23 STRING, k24 STRING, k25 STRING,
    k26 STRING, k27 STRING, k28 STRING, k29 STRING, k30 STRING,
    k31 STRING, k32 STRING, k33 STRING, k34 STRING, k35 STRING,
    k36 STRING, k37 STRING, k38 STRING, k39 STRING, k40 STRING
)
LOCATION '{{data_path}}/bench/string_wide_kv';

-- --------------------------------------------------------------------------
-- Table 5: bench.string_long
-- 10K rows, 4 cols of ~6.4KB strings each.
-- Tests SQLGetData chunked reads (buf_len smaller than cell).
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.string_long (
    rn BIGINT NOT NULL,
    s1 STRING NOT NULL,
    s2 STRING NOT NULL,
    s3 STRING NOT NULL,
    s4 STRING NOT NULL
)
LOCATION '{{data_path}}/bench/string_long';

-- --------------------------------------------------------------------------
-- Table 6: bench.binary_blobs
-- 5K rows, 3 cols of BINARY 32B-32KB per cell.
-- Stresses SQL_C_BINARY path and chunked truncation.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.binary_blobs (
    rn BIGINT NOT NULL,
    b1 BINARY NOT NULL,
    b2 BINARY NOT NULL,
    b3 BINARY NOT NULL
)
LOCATION '{{data_path}}/bench/binary_blobs';

-- --------------------------------------------------------------------------
-- Table 7: bench.decimal_temporal
-- 500K rows, 10 cols: DECIMAL(38,9), DATE, TIMESTAMP, TIME.
-- Tests decimal cast and temporal formatting, often a regression hot spot.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.decimal_temporal (
    rn      BIGINT NOT NULL,
    d_a     DECIMAL(38,9) NOT NULL,
    d_b     DECIMAL(38,9) NOT NULL,
    d_c     DECIMAL(38,9) NOT NULL,
    d_d     DECIMAL(38,9) NOT NULL,
    dt_a    DATE NOT NULL,
    dt_b    DATE NOT NULL,
    ts_a    TIMESTAMP,
    ts_b    TIMESTAMP,
    tm_a    TIME,
    tm_b    TIME
)
LOCATION '{{data_path}}/bench/decimal_temporal';

-- --------------------------------------------------------------------------
-- Table 8: bench.nested_json
-- 50K rows, 7 cols: 3 STRUCT, 2 ARRAY<INT>, 2 MAP<STRING,STRING>.
-- Exercises the format-bound path that shipments_full_types first exposed.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.nested_json (
    rn   BIGINT NOT NULL,
    st_a STRUCT<id: BIGINT, name: STRING, score: DOUBLE>,
    st_b STRUCT<lat: DOUBLE, lng: DOUBLE>,
    st_c STRUCT<inner: STRUCT<k: BIGINT, v: STRING>>,
    ar_a ARRAY<INT>,
    ar_b ARRAY<INT>,
    mp_a MAP<STRING, STRING>,
    mp_b MAP<STRING, STRING>
)
LOCATION '{{data_path}}/bench/nested_json';

-- --------------------------------------------------------------------------
-- Table 9: bench.null_heavy
-- 500K rows, 30 mixed-type cols, 95% NULL. Common shape in real fact tables.
-- Exercises the indicator-only fast path.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.null_heavy (
    rn   BIGINT NOT NULL,
    l01 BIGINT, l02 BIGINT, l03 BIGINT, l04 BIGINT, l05 BIGINT,
    i01 INT, i02 INT, i03 INT, i04 INT, i05 INT,
    s01 STRING, s02 STRING, s03 STRING, s04 STRING, s05 STRING,
    d01 DOUBLE, d02 DOUBLE, d03 DOUBLE, d04 DOUBLE, d05 DOUBLE,
    m01 DECIMAL(18,4), m02 DECIMAL(18,4), m03 DECIMAL(18,4), m04 DECIMAL(18,4),
    da01 DATE, da02 DATE, da03 DATE,
    ts01 TIMESTAMP, ts02 TIMESTAMP,
    bo01 BOOLEAN
)
LOCATION '{{data_path}}/bench/null_heavy';

-- --------------------------------------------------------------------------
-- Table 10: bench.skewed_strings
-- 500K rows, 6 cols. The skew column is 99% short strings + 1% 100KB strings.
-- Stresses chunked-read offset machinery under realistic skew.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.skewed_strings (
    rn   BIGINT NOT NULL,
    c1   STRING NOT NULL,
    c2   STRING NOT NULL,
    c3   STRING NOT NULL,
    c4   STRING NOT NULL,
    c5   STRING NOT NULL,
    skew STRING NOT NULL
)
LOCATION '{{data_path}}/bench/skewed_strings';

-- ==========================================================================
-- Population: each INSERT below is fully deterministic. Single
-- generate_series per insert keeps the input stream simple, with N <= 1M
-- in every case to stay well under the documented "very large ranges
-- materialize in memory" pitfall and Arrow's i32 offset limit.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Populate bench.fixed_narrow (1M rows). rn ranges 1..1_000_000.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.fixed_narrow
SELECT
    b.v AS rn,
    b.v AS i64_a,
    b.v * 7 AS i64_b,
    b.v % 1024 AS i64_c,
    CAST(b.v AS DOUBLE) AS f64_a,
    CAST(b.v AS DOUBLE) * 0.5 AS f64_b,
    CAST(b.v % 1000 AS DOUBLE) / 100.0 AS f64_c,
    SQRT(CAST(b.v AS DOUBLE)) AS f64_d
FROM generate_series(1, 1000000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate bench.fixed_wide (100K rows). rn ranges 1..100_000.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.fixed_wide
SELECT
    rn,
    rn, rn + 1, rn + 2, rn + 3, rn + 4, rn + 5, rn + 6, rn + 7, rn + 8, rn + 9, rn + 10, rn + 11,
    CAST(rn % 2147483647 AS INT),
    CAST((rn + 1) % 2147483647 AS INT),
    CAST((rn * 3) % 2147483647 AS INT),
    CAST((rn * 5) % 2147483647 AS INT),
    CAST((rn * 7) % 2147483647 AS INT),
    CAST((rn * 11) % 2147483647 AS INT),
    CAST((rn * 13) % 2147483647 AS INT),
    CAST((rn * 17) % 2147483647 AS INT),
    CAST((rn * 19) % 2147483647 AS INT),
    CAST((rn * 23) % 2147483647 AS INT),
    CAST((rn * 29) % 2147483647 AS INT),
    CAST((rn * 31) % 2147483647 AS INT),
    CAST(rn % 32767 AS SMALLINT),
    CAST((rn + 1) % 32767 AS SMALLINT),
    CAST((rn + 2) % 32767 AS SMALLINT),
    CAST((rn + 3) % 32767 AS SMALLINT),
    CAST((rn + 4) % 32767 AS SMALLINT),
    CAST((rn + 5) % 32767 AS SMALLINT),
    CAST((rn + 6) % 32767 AS SMALLINT),
    CAST((rn + 7) % 32767 AS SMALLINT),
    CAST(rn % 127 AS TINYINT),
    CAST((rn + 1) % 127 AS TINYINT),
    CAST((rn + 2) % 127 AS TINYINT),
    CAST((rn + 3) % 127 AS TINYINT),
    CAST((rn + 4) % 127 AS TINYINT),
    CAST((rn + 5) % 127 AS TINYINT),
    CAST((rn + 6) % 127 AS TINYINT),
    CAST((rn + 7) % 127 AS TINYINT),
    CAST(rn AS DOUBLE),
    CAST(rn AS DOUBLE) * 0.5,
    CAST(rn AS DOUBLE) * 0.25,
    CAST(rn AS DOUBLE) * 0.125,
    SQRT(CAST(rn AS DOUBLE)),
    LN(CAST(rn AS DOUBLE) + 1.0),
    CAST(rn % 1000 AS DOUBLE) / 1000.0,
    CAST((rn * 7) % 1000 AS DOUBLE) / 1000.0,
    CAST(CAST(rn AS DOUBLE) AS FLOAT),
    CAST(CAST(rn AS DOUBLE) * 0.5 AS FLOAT),
    CAST(CAST(rn AS DOUBLE) * 0.25 AS FLOAT),
    CAST(CAST(rn AS DOUBLE) * 0.125 AS FLOAT),
    CAST(SQRT(CAST(rn AS DOUBLE)) AS FLOAT),
    CAST(LN(CAST(rn AS DOUBLE) + 1.0) AS FLOAT),
    CAST(CAST(rn % 1000 AS DOUBLE) / 1000.0 AS FLOAT),
    CAST(CAST((rn * 7) % 1000 AS DOUBLE) / 1000.0 AS FLOAT),
    rn % 2 = 0,
    rn % 3 = 0,
    DATE '2000-01-01' + CAST(rn % 18250 AS INT),
    DATE '1970-01-01' + CAST((rn * 7) % 36500 AS INT)
FROM (
    SELECT b.v AS rn
    FROM generate_series(1, 100000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate bench.string_narrow (500K rows). NULL when rn % 10 IN (0, 1, 2)
-- which gives exactly 30% NULL density. s_short = lpad to 20 chars,
-- s_long_* = repeat(md5, 6) which is 192 chars per cell.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.string_narrow
SELECT
    b.v AS rn,
    CASE WHEN b.v % 10 IN (0, 1, 2) THEN NULL
         ELSE lpad(CAST(b.v AS STRING), 20, '0')
    END AS s_short,
    CASE WHEN b.v % 10 IN (0, 1, 2) THEN NULL
         ELSE repeat(md5(CAST(b.v AS STRING)), 6)
    END AS s_long_a,
    CASE WHEN b.v % 10 IN (0, 1, 2) THEN NULL
         ELSE repeat(md5(CAST(b.v * 3 AS STRING)), 6)
    END AS s_long_b,
    CASE WHEN b.v % 10 IN (0, 1, 2) THEN NULL
         ELSE repeat(md5(CAST(b.v * 7 AS STRING)), 6)
    END AS s_long_c,
    CASE WHEN b.v % 10 IN (0, 1, 2) THEN NULL
         ELSE repeat(md5(CAST(b.v * 11 AS STRING)), 6)
    END AS s_long_d
FROM generate_series(1, 500000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate bench.string_wide_kv (100K rows). NULL when rn % 20 = 0 which
-- gives exactly 5% NULL density. Each non-null cell is exactly 50 chars.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.string_wide_kv
SELECT
    b.v AS rn,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k01-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k02-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k03-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k04-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k05-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k06-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k07-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k08-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k09-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k10-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k11-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k12-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k13-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k14-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k15-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k16-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k17-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k18-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k19-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k20-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k21-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k22-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k23-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k24-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k25-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k26-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k27-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k28-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k29-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k30-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k31-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k32-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k33-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k34-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k35-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k36-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k37-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k38-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k39-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('k40-', CAST(b.v AS STRING)), 50, 'x') END
FROM generate_series(1, 100000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate bench.string_long (10K rows). md5 is 32 hex chars; repeat 200x
-- gives a 6400-char cell.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.string_long
SELECT
    b.v AS rn,
    repeat(md5(CAST(b.v AS STRING)), 200)        AS s1,
    repeat(md5(CAST(b.v * 3 AS STRING)), 200)    AS s2,
    repeat(md5(CAST(b.v * 7 AS STRING)), 200)    AS s3,
    repeat(md5(CAST(b.v * 11 AS STRING)), 200)   AS s4
FROM generate_series(1, 10000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate bench.binary_blobs (5K rows). Sizes vary by (rn mod K) so the
-- driver sees the full chunked-truncation matrix:
--   b1: 32 .. 1024 bytes  (1 + rn%32 repeats of 32-byte md5)
--   b2: 32 .. 2048 bytes  (1 + rn%64 repeats)
--   b3: 32 .. 32768 bytes (1 + rn%1024 repeats; ~32KB max)
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.binary_blobs
SELECT
    b.v AS rn,
    CAST(repeat(md5(CAST(b.v AS STRING)), 1 + CAST(b.v % 32 AS INT)) AS BINARY)        AS b1,
    CAST(repeat(md5(CAST(b.v * 3 AS STRING)), 1 + CAST(b.v % 64 AS INT)) AS BINARY)    AS b2,
    CAST(repeat(md5(CAST(b.v * 7 AS STRING)), 1 + CAST(b.v % 1024 AS INT)) AS BINARY)  AS b3
FROM generate_series(1, 5000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate bench.decimal_temporal (500K rows). ts_a / ts_b are bare TIMESTAMP.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.decimal_temporal
SELECT
    rn,
    CAST(rn AS DECIMAL(38,9)) + CAST(0.123456789 AS DECIMAL(38,9))                       AS d_a,
    CAST(rn * 7 AS DECIMAL(38,9)) + CAST(0.987654321 AS DECIMAL(38,9))                   AS d_b,
    CAST(rn * 13 AS DECIMAL(38,9)) / CAST(1000 AS DECIMAL(38,9))                         AS d_c,
    CAST(rn % 1000000 AS DECIMAL(38,9)) + CAST(0.000000001 AS DECIMAL(38,9))             AS d_d,
    DATE '2000-01-01' + CAST(rn % 18250 AS INT)                                          AS dt_a,
    DATE '1970-01-01' + CAST(rn % 36500 AS INT)                                          AS dt_b,
    make_timestamp(
        2025, 1, 1,
        CAST((rn % 86400) / 3600 AS INT),
        CAST(((rn % 86400) % 3600) / 60 AS INT),
        CAST((rn % 86400) % 60 AS DOUBLE)
    )                                                                                                AS ts_a,
    make_timestamp(
        2030, 6, 15,
        CAST((rn % 86400) / 3600 AS INT),
        CAST(((rn % 86400) % 3600) / 60 AS INT),
        CAST((rn % 86400) % 60 AS DOUBLE)
    )                                                                                                AS ts_b,
    make_time(
        CAST((rn % 86400) / 3600 AS INT),
        CAST(((rn % 86400) % 3600) / 60 AS INT),
        CAST((rn % 86400) % 60 AS DOUBLE)
    )                                                                                                AS tm_a,
    make_time(
        CAST((43200 + rn % 43200) / 3600 AS INT),
        CAST(((43200 + rn % 43200) % 3600) / 60 AS INT),
        CAST((43200 + rn % 43200) % 60 AS DOUBLE)
    )                                                                                                AS tm_b
FROM (
    SELECT b.v AS rn
    FROM generate_series(1, 500000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate bench.nested_json (50K rows). Mix of STRUCT, ARRAY, MAP.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.nested_json
SELECT
    b.v AS rn,
    named_struct('id', b.v, 'name', CAST(b.v AS STRING), 'score', CAST(b.v AS DOUBLE) * 0.5),
    named_struct('lat', CAST(b.v % 180 AS DOUBLE) - 90.0, 'lng', CAST(b.v % 360 AS DOUBLE) - 180.0),
    named_struct('inner', named_struct('k', b.v, 'v', CAST(b.v AS STRING))),
    array(CAST(b.v AS INT), CAST(b.v % 100 AS INT), CAST(b.v % 1000 AS INT)),
    array(CAST(b.v % 7 AS INT), CAST(b.v % 13 AS INT)),
    map('id', CAST(b.v AS STRING), 'mod10', CAST(b.v % 10 AS STRING)),
    map('hash', md5(CAST(b.v AS STRING)))
FROM generate_series(1, 50000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate bench.null_heavy (500K rows). All 30 nullable cols populated only
-- when rn % 20 = 0, so 5% non-null density per column.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.null_heavy
SELECT
    rn,
    CASE WHEN rn % 20 = 0 THEN rn ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN rn + 1 ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN rn * 3 ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN rn * 5 ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN rn * 7 ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST(rn % 2147483647 AS INT) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST((rn + 1) % 2147483647 AS INT) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST((rn * 3) % 2147483647 AS INT) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST((rn * 5) % 2147483647 AS INT) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST((rn * 7) % 2147483647 AS INT) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST(rn AS STRING) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN md5(CAST(rn AS STRING)) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN md5(CAST(rn * 3 AS STRING)) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN md5(CAST(rn * 7 AS STRING)) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN md5(CAST(rn * 11 AS STRING)) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST(rn AS DOUBLE) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST(rn AS DOUBLE) * 0.5 ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST(rn AS DOUBLE) * 0.25 ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN SQRT(CAST(rn AS DOUBLE)) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN LN(CAST(rn AS DOUBLE) + 1.0) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST(rn AS DECIMAL(18,4)) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST(rn AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4)) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST(rn * 7 AS DECIMAL(18,4)) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN CAST(rn % 1000000 AS DECIMAL(18,4)) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN DATE '2000-01-01' + CAST(rn % 18250 AS INT) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN DATE '1970-01-01' + CAST((rn * 3) % 36500 AS INT) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN DATE '2025-01-01' + CAST(rn % 1000 AS INT) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN make_timestamp(
        2025, 1, 1,
        CAST((rn % 86400) / 3600 AS INT),
        CAST(((rn % 86400) % 3600) / 60 AS INT),
        CAST((rn % 86400) % 60 AS DOUBLE)
    ) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN make_timestamp(
        2030, 6, 15,
        CAST((rn % 86400) / 3600 AS INT),
        CAST(((rn % 86400) % 3600) / 60 AS INT),
        CAST((rn % 86400) % 60 AS DOUBLE)
    ) ELSE NULL END,
    CASE WHEN rn % 20 = 0 THEN rn % 2 = 0 ELSE NULL END
FROM (
    SELECT b.v AS rn
    FROM generate_series(1, 500000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate bench.skewed_strings (500K rows). The skew column is a 1%/99% mix:
-- when rn % 100 = 0 the cell is repeat(md5,3125) = 100,000 chars (5K cells
-- total = 500MB); otherwise CAST(rn AS STRING) which is 1-6 chars.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.bench.skewed_strings
SELECT
    b.v AS rn,
    CAST(b.v AS STRING)                              AS c1,
    CONCAT('row-', CAST(b.v AS STRING))              AS c2,
    CONCAT('mod10-', CAST(b.v % 10 AS STRING))       AS c3,
    CONCAT('mod100-', CAST(b.v % 100 AS STRING))     AS c4,
    md5(CAST(b.v AS STRING))                         AS c5,
    CASE WHEN b.v % 100 = 0
         THEN repeat(md5(CAST(b.v AS STRING)), 3125)
         ELSE CAST(b.v AS STRING)
    END AS skew
FROM generate_series(1, 500000) AS b(v);

-- --------------------------------------------------------------------------
-- Schema Detection & Permissions
-- --------------------------------------------------------------------------

DETECT SCHEMA FOR TABLE {{zone_name}}.bench.fixed_narrow;
DETECT SCHEMA FOR TABLE {{zone_name}}.bench.fixed_wide;
DETECT SCHEMA FOR TABLE {{zone_name}}.bench.string_narrow;
DETECT SCHEMA FOR TABLE {{zone_name}}.bench.string_wide_kv;
DETECT SCHEMA FOR TABLE {{zone_name}}.bench.string_long;
DETECT SCHEMA FOR TABLE {{zone_name}}.bench.binary_blobs;
DETECT SCHEMA FOR TABLE {{zone_name}}.bench.decimal_temporal;
DETECT SCHEMA FOR TABLE {{zone_name}}.bench.nested_json;
DETECT SCHEMA FOR TABLE {{zone_name}}.bench.null_heavy;
DETECT SCHEMA FOR TABLE {{zone_name}}.bench.skewed_strings;

GRANT ADMIN ON TABLE {{zone_name}}.bench.fixed_narrow      TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.bench.fixed_wide        TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.bench.string_narrow     TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.bench.string_wide_kv    TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.bench.string_long       TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.bench.binary_blobs      TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.bench.decimal_temporal  TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.bench.nested_json       TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.bench.null_heavy        TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.bench.skewed_strings    TO USER {{current_user}};
