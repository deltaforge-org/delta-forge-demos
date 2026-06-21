-- ============================================================================
-- Demo: Order-Event Auto-Onboarding: Queries
-- ============================================================================
-- The landing folder holds 45 order events sunk from Kafka as two Avro files:
--   order_events_part_001.avro : 25 events (Avro container, embedded schema)
--   order_events_part_002.avro : 20 events (Avro container, embedded schema)
-- DISCOVER registers ONE external table over both. Every assertion value
-- below was precomputed from the data files, not the engine.
-- ============================================================================

-- ============================================================================
-- Query 1: DISCOVER in PRINT mode: review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the landed
-- files, recognises the Avro container magic header (Obj\x01), parses the
-- embedded record schema, and derives the columns from it (no inference, the
-- schema is authoritative). FILE_METADATA adds the df_file_name /
-- df_row_number provenance columns to the generated table.

DISCOVER {{zone_name}}.discover_demos.order_events
    PATH 'discover-avro-order-events'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode: auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). The
-- evidence column records the Avro magic-header match across both files. This
-- single statement replaces a hand-written CREATE EXTERNAL TABLE that would
-- otherwise have to restate every column type from the embedded schema.

DISCOVER {{zone_name}}.discover_demos.order_events
    PATH 'discover-avro-order-events'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: The unified feed: all 45 events from both Avro files
-- ============================================================================
-- One table spans both Avro objects. The discovered schema is the embedded
-- Avro record schema, identical across both files.

ASSERT ROW_COUNT = 45
SELECT
    order_id,
    customer_id,
    product_sku,
    category,
    order_status,
    channel,
    df_file_name
FROM {{zone_name}}.discover_demos.order_events;

-- ============================================================================
-- Query 4: Category mix: orders per product category
-- ============================================================================
-- Proves the body of every record decoded correctly across both Avro files.
-- Counts are exact and precomputed from the data.

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 12 WHERE category = 'electronics'
ASSERT VALUE order_count = 11 WHERE category = 'apparel'
ASSERT VALUE order_count = 11 WHERE category = 'home'
ASSERT VALUE order_count = 11 WHERE category = 'grocery'
SELECT
    category,
    COUNT(*) AS order_count
FROM {{zone_name}}.discover_demos.order_events
GROUP BY category
ORDER BY order_count DESC, category;

-- ============================================================================
-- Query 5: Fulfilment funnel: orders per status
-- ============================================================================
-- The order_status column comes straight from the embedded Avro schema.

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 12 WHERE order_status = 'placed'
ASSERT VALUE order_count = 11 WHERE order_status = 'shipped'
ASSERT VALUE order_count = 11 WHERE order_status = 'delivered'
ASSERT VALUE order_count = 11 WHERE order_status = 'cancelled'
SELECT
    order_status,
    COUNT(*) AS order_count
FROM {{zone_name}}.discover_demos.order_events
GROUP BY order_status
ORDER BY order_count DESC, order_status;

-- ============================================================================
-- Query 6: Units per category: integer aggregate over a typed Avro column
-- ============================================================================
-- quantity is an Avro int. SUM stays integer, so the totals are exact (no
-- float-equality fragility). Each value was precomputed from the rows.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_units = 36 WHERE category = 'electronics'
ASSERT VALUE total_units = 34 WHERE category = 'grocery'
ASSERT VALUE total_units = 33 WHERE category = 'home'
ASSERT VALUE total_units = 32 WHERE category = 'apparel'
SELECT
    category,
    SUM(quantity) AS total_units
FROM {{zone_name}}.discover_demos.order_events
GROUP BY category
ORDER BY total_units DESC, category;

-- ============================================================================
-- Query 7: Provenance: rows per source Avro file
-- ============================================================================
-- df_file_name comes from the FILE_METADATA = true discovery option. The two
-- Avro objects contribute 25 and 20 rows respectively.

ASSERT ROW_COUNT = 2
ASSERT VALUE rows_from_file = 25 WHERE source_file = 'order_events_part_001.avro'
ASSERT VALUE rows_from_file = 20 WHERE source_file = 'order_events_part_002.avro'
SELECT
    CASE
        WHEN df_file_name LIKE '%part_001.avro' THEN 'order_events_part_001.avro'
        ELSE 'order_events_part_002.avro'
    END AS source_file,
    COUNT(*) AS rows_from_file
FROM {{zone_name}}.discover_demos.order_events
GROUP BY 1
ORDER BY rows_from_file DESC;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total order volume, the
-- cancellation tail, total units sold, and the two-file union.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_orders = 45
ASSERT VALUE cancelled_orders = 11
ASSERT VALUE total_units = 135
ASSERT VALUE distinct_categories = 4
ASSERT VALUE distinct_source_files = 2
SELECT
    COUNT(*)                                              AS total_orders,
    COUNT(*) FILTER (WHERE order_status = 'cancelled')    AS cancelled_orders,
    SUM(quantity)                                         AS total_units,
    COUNT(DISTINCT category)                              AS distinct_categories,
    COUNT(DISTINCT df_file_name)                          AS distinct_source_files
FROM {{zone_name}}.discover_demos.order_events;
