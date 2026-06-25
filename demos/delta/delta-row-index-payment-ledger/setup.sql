-- ============================================================================
-- Card Payment Ledger - Row-Index Cost / Benefit - Setup
-- ============================================================================
-- A fintech card-payment platform with a LARGE authorization ledger:
-- 20,000,000 card authorizations across ~140 Delta files.
--
-- The lookup key, txn_id, is a random external reference (not the ingestion
-- order), so its values are SCATTERED across every file: each file's min/max
-- on txn_id spans almost the whole id domain. Delta keeps per-file min/max
-- statistics for the leading columns, but those stats can only prune when a
-- column's values are clustered on disk. A scattered key defeats min/max, so a
-- point lookup on txn_id plans to scan every file.
--
-- Even where runtime row-group statistics narrow the read to a single file and
-- row group, the engine still DECODES that entire ~16k-row group to extract one
-- row. A row-level B+ tree index stores each key's exact location (file + row
-- offset), so a point read or a keyed UPDATE touches just that one row instead
-- of decoding the whole row group. That is the benefit this demo MEASURES.
--
-- Generated inline with generate_series in four 5M chunks (no external files).
-- Deletion vectors are on so a keyed UPDATE marks one row instead of rewriting
-- its file.
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables - demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: payments - 20,000,000 authorizations, scattered txn_id
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.payments WITH FILES;

CREATE DELTA TABLE {{zone_name}}.delta_demos.payments (
    txn_id        BIGINT,
    card_id       BIGINT,
    amount        DOUBLE,
    status        VARCHAR,
    authorized_at VARCHAR
) LOCATION 'delta-row-index-payment-ledger/payments'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- 20M rows in four 5M chunks. txn_id = (id * 2654435761) mod 1e9 is a
-- multiplicative hash: consecutive ids land on scattered txn_ids, so every
-- file ends up spanning almost the whole txn_id domain and min/max cannot
-- prune a txn_id lookup. card_id recurs (200k distinct cards).
INSERT INTO {{zone_name}}.delta_demos.payments
SELECT
    ((id +        0) * 2654435761) % 1000000000 AS txn_id,
    (id % 200000)                                AS card_id,
    ROUND((id % 1000) * 1.5 + 1.0, 2)            AS amount,
    CASE WHEN (id % 50) = 0 THEN 'declined' ELSE 'authorized' END AS status,
    '2026-06-01'                                 AS authorized_at
FROM generate_series(1, 5000000) AS t(id);

INSERT INTO {{zone_name}}.delta_demos.payments
SELECT
    ((id +  5000000) * 2654435761) % 1000000000 AS txn_id,
    ((id + 5000000) % 200000)                    AS card_id,
    ROUND((id % 1000) * 1.5 + 1.0, 2)            AS amount,
    CASE WHEN (id % 50) = 0 THEN 'declined' ELSE 'authorized' END AS status,
    '2026-06-02'                                 AS authorized_at
FROM generate_series(1, 5000000) AS t(id);

INSERT INTO {{zone_name}}.delta_demos.payments
SELECT
    ((id + 10000000) * 2654435761) % 1000000000 AS txn_id,
    ((id + 10000000) % 200000)                   AS card_id,
    ROUND((id % 1000) * 1.5 + 1.0, 2)            AS amount,
    CASE WHEN (id % 50) = 0 THEN 'declined' ELSE 'authorized' END AS status,
    '2026-06-03'                                 AS authorized_at
FROM generate_series(1, 5000000) AS t(id);

INSERT INTO {{zone_name}}.delta_demos.payments
SELECT
    ((id + 15000000) * 2654435761) % 1000000000 AS txn_id,
    ((id + 15000000) % 200000)                   AS card_id,
    ROUND((id % 1000) * 1.5 + 1.0, 2)            AS amount,
    CASE WHEN (id % 50) = 0 THEN 'declined' ELSE 'authorized' END AS status,
    '2026-06-04'                                 AS authorized_at
FROM generate_series(1, 5000000) AS t(id);
