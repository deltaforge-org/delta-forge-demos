-- ==========================================================================
-- Demo: ACME Corporation Production Warehouse (ODBC Driver Wire Benchmark)
-- ==========================================================================
-- Ten realistic operational tables that double as a deterministic ODBC
-- wire-benchmark suite. Each table represents a recognisable real-world
-- workload while still isolating one ODBC wire dimension at a time so a
-- regression on byte counts or throughput points at one code path. Every
-- cell remains row_number-derived: two runs are bit-identical and any
-- drift is real.
--
-- Large tier: ~70.65M rows total. forum_posts uses a 0.5% long-cell rate
-- (rn % 200 = 0) so the 100KB skew character is preserved while staying
-- safely under Arrow's 2GB i32 string-offset limit.
--
-- All tables target 128 MB Parquet files (Databricks-recommended sweet
-- spot) via delta.targetFileSize, aligned with Power Query / Power BI
-- partition refresh behaviour.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'ACME Corporation production data warehouse';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.acme
    COMMENT 'Mixed-workload demo schema; each table maps to one ODBC wire dimension';

-- --------------------------------------------------------------------------
-- Table 1: acme.market_ticks
-- 100M-row equity tick stream. 8 cols, all INT64/DOUBLE, no nulls.
-- Stresses driver upper bound: pure decode + memcpy. Speed-of-light baseline.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.market_ticks (
    tick_id               BIGINT NOT NULL,
    instrument_id         BIGINT NOT NULL,
    bid_size_units        BIGINT NOT NULL,
    exchange_lookup_code  BIGINT NOT NULL,
    last_price            DOUBLE NOT NULL,
    bid_price             DOUBLE NOT NULL,
    ask_spread_bps        DOUBLE NOT NULL,
    vwap_volatility_score DOUBLE NOT NULL
)
LOCATION '{{data_path}}/acme/market_ticks'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- --------------------------------------------------------------------------
-- Table 2: acme.manufacturing_runs
-- 2M plant run snapshots. 60 fixed-width primitive sensor cols.
-- Stresses per-cell overhead at scale and the column slab cache hit path.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.manufacturing_runs (
    run_id BIGINT NOT NULL,
    machine_serial_l01 BIGINT NOT NULL, machine_serial_l02 BIGINT NOT NULL, machine_serial_l03 BIGINT NOT NULL,
    machine_serial_l04 BIGINT NOT NULL, machine_serial_l05 BIGINT NOT NULL, machine_serial_l06 BIGINT NOT NULL,
    machine_serial_l07 BIGINT NOT NULL, machine_serial_l08 BIGINT NOT NULL, machine_serial_l09 BIGINT NOT NULL,
    machine_serial_l10 BIGINT NOT NULL, machine_serial_l11 BIGINT NOT NULL, machine_serial_l12 BIGINT NOT NULL,
    cycle_count_i01 INT NOT NULL, cycle_count_i02 INT NOT NULL, cycle_count_i03 INT NOT NULL,
    cycle_count_i04 INT NOT NULL, cycle_count_i05 INT NOT NULL, cycle_count_i06 INT NOT NULL,
    cycle_count_i07 INT NOT NULL, cycle_count_i08 INT NOT NULL, cycle_count_i09 INT NOT NULL,
    cycle_count_i10 INT NOT NULL, cycle_count_i11 INT NOT NULL, cycle_count_i12 INT NOT NULL,
    batch_size_s01 SMALLINT NOT NULL, batch_size_s02 SMALLINT NOT NULL, batch_size_s03 SMALLINT NOT NULL,
    batch_size_s04 SMALLINT NOT NULL, batch_size_s05 SMALLINT NOT NULL, batch_size_s06 SMALLINT NOT NULL,
    batch_size_s07 SMALLINT NOT NULL, batch_size_s08 SMALLINT NOT NULL,
    shift_id_t01 TINYINT NOT NULL, shift_id_t02 TINYINT NOT NULL, shift_id_t03 TINYINT NOT NULL,
    shift_id_t04 TINYINT NOT NULL, shift_id_t05 TINYINT NOT NULL, shift_id_t06 TINYINT NOT NULL,
    shift_id_t07 TINYINT NOT NULL, shift_id_t08 TINYINT NOT NULL,
    temperature_c_d01 DOUBLE NOT NULL, temperature_c_d02 DOUBLE NOT NULL, temperature_c_d03 DOUBLE NOT NULL,
    temperature_c_d04 DOUBLE NOT NULL, temperature_c_d05 DOUBLE NOT NULL, temperature_c_d06 DOUBLE NOT NULL,
    temperature_c_d07 DOUBLE NOT NULL, temperature_c_d08 DOUBLE NOT NULL,
    pressure_psi_f01 FLOAT NOT NULL, pressure_psi_f02 FLOAT NOT NULL, pressure_psi_f03 FLOAT NOT NULL,
    pressure_psi_f04 FLOAT NOT NULL, pressure_psi_f05 FLOAT NOT NULL, pressure_psi_f06 FLOAT NOT NULL,
    pressure_psi_f07 FLOAT NOT NULL, pressure_psi_f08 FLOAT NOT NULL,
    is_overcurrent BOOLEAN NOT NULL, has_alarm BOOLEAN NOT NULL,
    run_start_date DATE NOT NULL, shift_change_date DATE NOT NULL
)
LOCATION '{{data_path}}/acme/manufacturing_runs'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- --------------------------------------------------------------------------
-- Table 3: acme.support_tickets
-- 5M customer-support tickets. 5 cols, ~30% NULL on every text column.
-- Stresses the UTF-8 decode hot path and the indicator-array path.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.support_tickets (
    ticket_id         BIGINT NOT NULL,
    ticket_code       STRING,
    summary           STRING,
    description       STRING,
    resolution_notes  STRING,
    internal_comment  STRING
)
LOCATION '{{data_path}}/acme/support_tickets'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- --------------------------------------------------------------------------
-- Table 4: acme.product_catalog
-- 1M products, 40 i18n display-name cols (one per supported locale),
-- ~5% NULL because not every product is translated into every locale.
-- Each non-null cell is exactly 50 chars.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.product_catalog (
    product_id BIGINT NOT NULL,
    display_name_en STRING, display_name_fr STRING, display_name_de STRING, display_name_es STRING,
    display_name_it STRING, display_name_pt STRING, display_name_ja STRING, display_name_zh_cn STRING,
    display_name_zh_tw STRING, display_name_ko STRING, display_name_ar STRING, display_name_he STRING,
    display_name_nl STRING, display_name_sv STRING, display_name_no STRING, display_name_fi STRING,
    display_name_da STRING, display_name_pl STRING, display_name_cs STRING, display_name_ru STRING,
    display_name_uk STRING, display_name_tr STRING, display_name_el STRING, display_name_th STRING,
    display_name_vi STRING, display_name_id STRING, display_name_ms STRING, display_name_hi STRING,
    display_name_bn STRING, display_name_ta STRING, display_name_te STRING, display_name_ml STRING,
    display_name_kn STRING, display_name_mr STRING, display_name_pa STRING, display_name_ur STRING,
    display_name_fa STRING, display_name_sw STRING, display_name_zu STRING, display_name_ha STRING
)
LOCATION '{{data_path}}/acme/product_catalog'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- --------------------------------------------------------------------------
-- Table 5: acme.knowledge_articles
-- 100K wiki/KB articles. 4 cols of ~6.4KB strings each.
-- Tests SQLGetData chunked reads (buf_len smaller than cell).
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.knowledge_articles (
    article_id        BIGINT NOT NULL,
    abstract_text     STRING NOT NULL,
    body_markdown     STRING NOT NULL,
    metadata_blob     STRING NOT NULL,
    legal_disclaimer  STRING NOT NULL
)
LOCATION '{{data_path}}/acme/knowledge_articles'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- --------------------------------------------------------------------------
-- Table 6: acme.document_archive
-- 50K document records with embedded image / PDF / attachment payloads.
-- 3 BINARY cols of 32B-32KB. Stresses SQL_C_BINARY chunked truncation.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.document_archive (
    document_id            BIGINT NOT NULL,
    thumbnail_png          BINARY NOT NULL,
    preview_pdf_first_page BINARY NOT NULL,
    archived_attachment    BINARY NOT NULL
)
LOCATION '{{data_path}}/acme/document_archive'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- --------------------------------------------------------------------------
-- Table 7: acme.banking_transactions
-- 5M bank transactions. DECIMAL(38,9) for money + fx, multi-stage temporal.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.banking_transactions (
    transaction_id           BIGINT NOT NULL,
    amount_usd               DECIMAL(38,9) NOT NULL,
    fx_amount_eur            DECIMAL(38,9) NOT NULL,
    wire_fee_usd             DECIMAL(38,9) NOT NULL,
    withholding_tax          DECIMAL(38,9) NOT NULL,
    value_date               DATE NOT NULL,
    settle_date              DATE NOT NULL,
    captured_ts              TIMESTAMP,
    posted_ts                TIMESTAMP,
    processing_window_start  TIME,
    processing_window_end    TIME
)
LOCATION '{{data_path}}/acme/banking_transactions'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- --------------------------------------------------------------------------
-- Table 8: acme.shipment_orders
-- 500K e-commerce orders with nested STRUCT, ARRAY, MAP.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.shipment_orders (
    order_id                 BIGINT NOT NULL,
    customer                 STRUCT<id: BIGINT, name: STRING, score: DOUBLE>,
    shipping_address         STRUCT<lat: DOUBLE, lng: DOUBLE>,
    billing_info             STRUCT<inner: STRUCT<k: BIGINT, v: STRING>>,
    line_item_quantities     ARRAY<INT>,
    applied_discount_codes   ARRAY<INT>,
    tracking_metadata        MAP<STRING, STRING>,
    audit_tags               MAP<STRING, STRING>
)
LOCATION '{{data_path}}/acme/shipment_orders'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- --------------------------------------------------------------------------
-- Table 9: acme.patient_records
-- 5M patient records with 30 sparse extension fields. 95% NULL.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.patient_records (
    record_id BIGINT NOT NULL,
    lab_value_l01 BIGINT, lab_value_l02 BIGINT, lab_value_l03 BIGINT, lab_value_l04 BIGINT, lab_value_l05 BIGINT,
    diagnosis_code_i01 INT, diagnosis_code_i02 INT, diagnosis_code_i03 INT, diagnosis_code_i04 INT, diagnosis_code_i05 INT,
    clinical_note_s01 STRING, clinical_note_s02 STRING, clinical_note_s03 STRING, clinical_note_s04 STRING, clinical_note_s05 STRING,
    vital_reading_d01 DOUBLE, vital_reading_d02 DOUBLE, vital_reading_d03 DOUBLE, vital_reading_d04 DOUBLE, vital_reading_d05 DOUBLE,
    billing_amt_m01 DECIMAL(18,4), billing_amt_m02 DECIMAL(18,4), billing_amt_m03 DECIMAL(18,4), billing_amt_m04 DECIMAL(18,4),
    admission_date DATE, discharge_date DATE, follow_up_date DATE,
    charted_ts TIMESTAMP, signed_ts TIMESTAMP,
    is_amended BOOLEAN
)
LOCATION '{{data_path}}/acme/patient_records'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- --------------------------------------------------------------------------
-- Table 10: acme.forum_posts
-- 2M discussion-forum posts. Most posts are short comments (1-7 chars,
-- modelling chat-style channels); 0.5% (rn % 200 = 0) are 100KB essays.
-- 10K outliers x 100KB = 1 GB per col total, well under Arrow's 2GB cap.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.acme.forum_posts (
    post_id          BIGINT NOT NULL,
    author_handle    STRING NOT NULL,
    post_title       STRING NOT NULL,
    thread_category  STRING NOT NULL,
    tag_list_csv     STRING NOT NULL,
    content_hash     STRING NOT NULL,
    body             STRING NOT NULL
)
LOCATION '{{data_path}}/acme/forum_posts'
TBLPROPERTIES (
    'delta.targetFileSize' = '134217728'
);

-- ==========================================================================
-- Population: each INSERT below is fully deterministic. Tables larger than
-- 1M rows use a cross-join of two generate_series factors so no single
-- series exceeds 1M values.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Populate acme.market_ticks (100M rows). tick_id ranges 1..100_000_000.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.market_ticks
SELECT
    rn AS tick_id,
    rn AS instrument_id,
    rn * 7 AS bid_size_units,
    rn % 1024 AS exchange_lookup_code,
    CAST(rn AS DOUBLE) AS last_price,
    CAST(rn AS DOUBLE) * 0.5 AS bid_price,
    CAST(rn % 1000 AS DOUBLE) / 100.0 AS ask_spread_bps,
    SQRT(CAST(rn AS DOUBLE)) AS vwap_volatility_score
FROM (
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 99) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate acme.manufacturing_runs (2M rows). run_id ranges 1..2_000_000.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.manufacturing_runs
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
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 1) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate acme.support_tickets (5M rows). NULL when ticket_id % 10 IN
-- (0,1,2): exactly 30% NULL = 1.5M rows. Non-null ticket_code = lpad to
-- 20 chars, non-null summary/description/etc = repeat(md5,6) = 192 chars.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.support_tickets
SELECT
    rn AS ticket_id,
    CASE WHEN rn % 10 IN (0, 1, 2) THEN NULL
         ELSE lpad(CAST(rn AS STRING), 20, '0')
    END,
    CASE WHEN rn % 10 IN (0, 1, 2) THEN NULL
         ELSE repeat(md5(CAST(rn AS STRING)), 6)
    END,
    CASE WHEN rn % 10 IN (0, 1, 2) THEN NULL
         ELSE repeat(md5(CAST(rn * 3 AS STRING)), 6)
    END,
    CASE WHEN rn % 10 IN (0, 1, 2) THEN NULL
         ELSE repeat(md5(CAST(rn * 7 AS STRING)), 6)
    END,
    CASE WHEN rn % 10 IN (0, 1, 2) THEN NULL
         ELSE repeat(md5(CAST(rn * 11 AS STRING)), 6)
    END
FROM (
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 4) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate acme.product_catalog (1M rows). NULL when product_id % 20 = 0
-- which gives exactly 5% NULL. Each non-null cell is exactly 50 chars.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.product_catalog
SELECT
    b.v AS product_id,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('en-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('fr-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('de-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('es-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('it-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('pt-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('ja-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('zh_cn-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('zh_tw-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('ko-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('ar-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('he-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('nl-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('sv-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('no-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('fi-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('da-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('pl-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('cs-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('ru-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('uk-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('tr-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('el-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('th-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('vi-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('id-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('ms-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('hi-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('bn-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('ta-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('te-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('ml-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('kn-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('mr-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('pa-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('ur-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('fa-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('sw-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('zu-', CAST(b.v AS STRING)), 50, 'x') END,
    CASE WHEN b.v % 20 = 0 THEN NULL ELSE lpad(CONCAT('ha-', CAST(b.v AS STRING)), 50, 'x') END
FROM generate_series(1, 1000000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate acme.knowledge_articles (100K rows). md5 is 32 hex chars; repeat
-- 200x gives a 6400-char cell.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.knowledge_articles
SELECT
    b.v AS article_id,
    repeat(md5(CAST(b.v AS STRING)), 200)        AS abstract_text,
    repeat(md5(CAST(b.v * 3 AS STRING)), 200)    AS body_markdown,
    repeat(md5(CAST(b.v * 7 AS STRING)), 200)    AS metadata_blob,
    repeat(md5(CAST(b.v * 11 AS STRING)), 200)   AS legal_disclaimer
FROM generate_series(1, 100000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate acme.document_archive (50K rows). Sizes vary by (rn mod K) so
-- the driver sees the full chunked-truncation matrix:
--   thumbnail_png:          32 .. 1024  bytes  (1 + rn%32 repeats of md5)
--   preview_pdf_first_page: 32 .. 2048  bytes  (1 + rn%64 repeats)
--   archived_attachment:    32 .. 32768 bytes  (1 + rn%1024 repeats)
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.document_archive
SELECT
    b.v AS document_id,
    CAST(repeat(md5(CAST(b.v AS STRING)), 1 + CAST(b.v % 32 AS INT)) AS BINARY)        AS thumbnail_png,
    CAST(repeat(md5(CAST(b.v * 3 AS STRING)), 1 + CAST(b.v % 64 AS INT)) AS BINARY)    AS preview_pdf_first_page,
    CAST(repeat(md5(CAST(b.v * 7 AS STRING)), 1 + CAST(b.v % 1024 AS INT)) AS BINARY)  AS archived_attachment
FROM generate_series(1, 50000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate acme.banking_transactions (5M rows). captured_ts / posted_ts
-- are bare TIMESTAMP, processing_window_* are TIME.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.banking_transactions
SELECT
    rn,
    CAST(rn AS DECIMAL(38,9)) + CAST(0.123456789 AS DECIMAL(38,9))                       AS amount_usd,
    CAST(rn * 7 AS DECIMAL(38,9)) + CAST(0.987654321 AS DECIMAL(38,9))                   AS fx_amount_eur,
    CAST(rn * 13 AS DECIMAL(38,9)) / CAST(1000 AS DECIMAL(38,9))                         AS wire_fee_usd,
    CAST(rn % 1000000 AS DECIMAL(38,9)) + CAST(0.000000001 AS DECIMAL(38,9))             AS withholding_tax,
    DATE '2000-01-01' + CAST(rn % 18250 AS INT)                                          AS value_date,
    DATE '1970-01-01' + CAST(rn % 36500 AS INT)                                          AS settle_date,
    make_timestamp(
        2025, 1, 1,
        CAST((rn % 86400) / 3600 AS INT),
        CAST(((rn % 86400) % 3600) / 60 AS INT),
        CAST((rn % 86400) % 60 AS DOUBLE)
    )                                                                                                AS captured_ts,
    make_timestamp(
        2030, 6, 15,
        CAST((rn % 86400) / 3600 AS INT),
        CAST(((rn % 86400) % 3600) / 60 AS INT),
        CAST((rn % 86400) % 60 AS DOUBLE)
    )                                                                                                AS posted_ts,
    make_time(
        CAST((rn % 86400) / 3600 AS INT),
        CAST(((rn % 86400) % 3600) / 60 AS INT),
        CAST((rn % 86400) % 60 AS DOUBLE)
    )                                                                                                AS processing_window_start,
    make_time(
        CAST((43200 + rn % 43200) / 3600 AS INT),
        CAST(((43200 + rn % 43200) % 3600) / 60 AS INT),
        CAST((43200 + rn % 43200) % 60 AS DOUBLE)
    )                                                                                                AS processing_window_end
FROM (
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 4) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate acme.shipment_orders (500K rows). Mix of STRUCT, ARRAY, MAP.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.shipment_orders
SELECT
    b.v AS order_id,
    named_struct('id', b.v, 'name', CAST(b.v AS STRING), 'score', CAST(b.v AS DOUBLE) * 0.5),
    named_struct('lat', CAST(b.v % 180 AS DOUBLE) - 90.0, 'lng', CAST(b.v % 360 AS DOUBLE) - 180.0),
    named_struct('inner', named_struct('k', b.v, 'v', CAST(b.v AS STRING))),
    array(CAST(b.v AS INT), CAST(b.v % 100 AS INT), CAST(b.v % 1000 AS INT)),
    array(CAST(b.v % 7 AS INT), CAST(b.v % 13 AS INT)),
    map('id', CAST(b.v AS STRING), 'mod10', CAST(b.v % 10 AS STRING)),
    map('hash', md5(CAST(b.v AS STRING)))
FROM generate_series(1, 500000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate acme.patient_records (5M rows). All 30 nullable cols populated
-- only when record_id % 20 = 0, so 5% non-null density per column.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.patient_records
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
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 4) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate acme.forum_posts (2M rows). The body column is a 0.5%/99.5% mix:
-- when post_id % 200 = 0 the cell is repeat(md5,3125) = 100,000 chars
-- (10K cells total = 1 GB per col); otherwise CAST(post_id AS STRING).
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.acme.forum_posts
SELECT
    rn AS post_id,
    CAST(rn AS STRING)                              AS author_handle,
    CONCAT('row-', CAST(rn AS STRING))              AS post_title,
    CONCAT('mod10-', CAST(rn % 10 AS STRING))       AS thread_category,
    CONCAT('mod100-', CAST(rn % 100 AS STRING))     AS tag_list_csv,
    md5(CAST(rn AS STRING))                         AS content_hash,
    CASE WHEN rn % 200 = 0
         THEN repeat(md5(CAST(rn AS STRING)), 3125)
         ELSE CAST(rn AS STRING)
    END AS body
FROM (
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 1) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Schema Detection
-- --------------------------------------------------------------------------

DETECT SCHEMA FOR TABLE {{zone_name}}.acme.market_ticks;
DETECT SCHEMA FOR TABLE {{zone_name}}.acme.manufacturing_runs;
DETECT SCHEMA FOR TABLE {{zone_name}}.acme.support_tickets;
DETECT SCHEMA FOR TABLE {{zone_name}}.acme.product_catalog;
DETECT SCHEMA FOR TABLE {{zone_name}}.acme.knowledge_articles;
DETECT SCHEMA FOR TABLE {{zone_name}}.acme.document_archive;
DETECT SCHEMA FOR TABLE {{zone_name}}.acme.banking_transactions;
DETECT SCHEMA FOR TABLE {{zone_name}}.acme.shipment_orders;
DETECT SCHEMA FOR TABLE {{zone_name}}.acme.patient_records;
DETECT SCHEMA FOR TABLE {{zone_name}}.acme.forum_posts;
