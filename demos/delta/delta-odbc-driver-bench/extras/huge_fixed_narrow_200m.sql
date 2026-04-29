-- ==========================================================================
-- Standalone: bench.fixed_narrow_200m
-- ==========================================================================
-- A 200,000,000-row scale-up of fixed_narrow for stress-testing Delta Forge
-- writer behaviour and ODBC throughput at memcpy speed-of-light.
--
-- This script is INTENTIONALLY NOT part of the demo. It writes to a
-- separate table (bench.fixed_narrow_200m), so running it does not affect
-- the demo's idempotency or its 1M-row bench.fixed_narrow.
--
-- Schema: 8 cols, all INT64 / DOUBLE, no nulls. No string or binary
-- columns => no Arrow i32-offset overflow risk; the sole concern is
-- generator memory and disk space.
--
-- Generator factoring: cross-join generate_series(0, 199) x generate_series(1, 1000000)
-- so no single series exceeds 1,000,000 values, staying inside the
-- documented "very large ranges materialize in memory" pitfall.
--
-- Expected run time: 15-25 minutes (Local zone, default disk).
-- Expected disk:     ~6-10 GB Parquet under the zone's storage path.
--
-- Usage:
--   1. Substitute  {{zone_name}}   with your actual zone (e.g. dttest).
--   2. Substitute  {{data_path}}   with the zone's storage_path.
--   3. Substitute  {{current_user}} with your DF username.
--   4. Run the substituted script via the GUI SQL editor or:
--        delta-forge-cli run extras/huge_fixed_narrow_200m.sql
--
-- Closed-form invariants (analytical, no script needed to verify):
--   COUNT(*)     = 200,000,000
--   MIN(rn)      = 1
--   MAX(rn)      = 200,000,000
--   SUM(rn)      = N*(N+1)/2 = 20,000,000,100,000,000
--   SUM(i64_a)   = SUM(rn)   = 20,000,000,100,000,000
--   SUM(i64_b)   = 7 * SUM(rn) = 140,000,000,700,000,000
--   COUNT WHERE i64_c = 0     = floor(200_000_000 / 1024) = 195,312
-- ==========================================================================

-- Drop any previous version so re-runs are idempotent.
DROP DELTA TABLE IF EXISTS {{zone_name}}.bench.fixed_narrow_200m WITH FILES;

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.bench.fixed_narrow_200m (
    rn    BIGINT NOT NULL,
    i64_a BIGINT NOT NULL,
    i64_b BIGINT NOT NULL,
    i64_c BIGINT NOT NULL,
    f64_a DOUBLE NOT NULL,
    f64_b DOUBLE NOT NULL,
    f64_c DOUBLE NOT NULL,
    f64_d DOUBLE NOT NULL
)
LOCATION '{{data_path}}/bench/fixed_narrow_200m';

INSERT INTO {{zone_name}}.bench.fixed_narrow_200m
SELECT
    a.v * 1000000 + b.v AS rn,
    a.v * 1000000 + b.v AS i64_a,
    (a.v * 1000000 + b.v) * 7 AS i64_b,
    (a.v * 1000000 + b.v) % 1024 AS i64_c,
    CAST(a.v * 1000000 + b.v AS DOUBLE) AS f64_a,
    CAST(a.v * 1000000 + b.v AS DOUBLE) * 0.5 AS f64_b,
    CAST((a.v * 1000000 + b.v) % 1000 AS DOUBLE) / 100.0 AS f64_c,
    SQRT(CAST(a.v * 1000000 + b.v AS DOUBLE)) AS f64_d
FROM generate_series(0, 199) AS a(v)
CROSS JOIN generate_series(1, 1000000) AS b(v);

DETECT SCHEMA FOR TABLE {{zone_name}}.bench.fixed_narrow_200m;
GRANT ADMIN ON TABLE {{zone_name}}.bench.fixed_narrow_200m TO USER {{current_user}};

-- ==========================================================================
-- Optional sanity checks (run after the INSERT finishes):
-- ==========================================================================

-- Closed-form total + bounds
SELECT
    COUNT(*) AS n_rows,
    SUM(rn) AS sum_rn,
    MIN(rn) AS min_rn,
    MAX(rn) AS max_rn
FROM {{zone_name}}.bench.fixed_narrow_200m;
-- Expect: n_rows=200000000, sum_rn=20000000100000000, min_rn=1, max_rn=200000000

-- Mid-table spot check: deterministic per-cell values at rn=12345678
SELECT i64_a, i64_b, i64_c, f64_a, f64_b, f64_c
FROM {{zone_name}}.bench.fixed_narrow_200m
WHERE rn = 12345678;
-- Expect: 12345678, 86419746, 334, 12345678.0, 6172839.0, 6.78
