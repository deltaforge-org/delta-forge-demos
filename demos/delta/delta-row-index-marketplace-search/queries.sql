-- ============================================================================
-- Multi-Vendor Marketplace — Multiple Indexes on the Same Table
-- ============================================================================
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │            ONE TABLE CAN CARRY SEVERAL INDEXES                       │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │                                                                      │
--  │ Real workloads rarely have a single query shape. A marketplace      │
--  │ catalog gets pinged by:                                              │
--  │                                                                      │
--  │   • the WAREHOUSE service     → "where is SKU X?"                    │
--  │   • the STOREFRONT service    → "show me everything from brand Y"   │
--  │   • the BROWSE pages          → "category Z under $100"             │
--  │                                                                      │
--  │ One composite index can't cover all three — they filter on           │
--  │ different leading columns. The clean answer: build SEPARATE indexes  │
--  │ for each shape. The planner picks the cheapest applicable one for   │
--  │ each query at runtime; the caller writes ordinary SQL.              │
--  │                                                                      │
--  │ You don't tell the planner which index to use. It looks at every    │
--  │ index that COULD apply (matches the predicate columns), estimates   │
--  │ the cost of each, and picks the cheapest. If none beats ordinary    │
--  │ file pruning it falls back. No hints, no manual selection.          │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                 WHEN TO HAVE MULTIPLE INDEXES                        │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ ✓ Several distinct query shapes hit the table regularly             │
--  │ ✓ Each shape filters on a different leading column                   │
--  │ ✓ Each shape is selective enough that an index helps it             │
--  │ ✓ The combined storage + write overhead of N indexes is justified   │
--  │   by the read savings                                                │
--  │                                                                      │
--  │ When NOT to multiply indexes:                                        │
--  │ ✗ Only one query shape matters — extra indexes are dead weight     │
--  │ ✗ Writes are very heavy — every index multiplies write cost (each  │
--  │   index has to update on every commit if auto_update is on)        │
--  │ ✗ Several queries share a common LEFTMOST PREFIX — one composite   │
--  │   index can serve all of them. (See the IoT telemetry demo.)        │
--  │ ✗ Two indexes cover the same column with similar cost — pick the   │
--  │   tighter one and drop the other                                    │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │                   AUDITING YOUR INDEXES                              │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ • SHOW INDEXES ON TABLE T          — list every index on the table  │
--  │ • DESCRIBE INDEX idx ON TABLE T    — status, columns, version       │
--  │ • DROP INDEX idx ON TABLE T        — remove one when it stops       │
--  │                                       earning its keep              │
--  └──────────────────────────────────────────────────────────────────────┘
--
-- This demo builds three indexes: idx_sku for warehouse fulfillment,
-- idx_brand for storefront, and a composite (category, price) for
-- faceted browse. Each query targets a different one. A final query
-- (predicate on `stock`) shows graceful fallback when no index applies.
-- The demo finishes by DROPping a redundant index and re-running the
-- query that used it — same answer, different (still-correct) path.
-- ============================================================================


-- ============================================================================
-- BUILD: Create Three Indexes for Three Search Shapes
-- ============================================================================
-- Each search shape on the marketplace gets its own index. The
-- planner picks the cheapest applicable one per query at runtime —
-- the caller writes ordinary SQL.
--
-- 1. idx_sku            : warehouse fulfillment, exact SKU lookup
-- 2. idx_brand          : storefront landing pages, equality on brand
-- 3. idx_category_price : faceted browse, leading-column equality
--                         plus a trailing range on price

CREATE INDEX idx_sku
    ON TABLE {{zone_name}}.delta_demos.marketplace_listings (sku)
    WITH (auto_update = true);

CREATE INDEX idx_brand
    ON TABLE {{zone_name}}.delta_demos.marketplace_listings (brand)
    WITH (auto_update = true);

CREATE INDEX idx_category_price
    ON TABLE {{zone_name}}.delta_demos.marketplace_listings (category, price)
    WITH (auto_update = true);


-- ============================================================================
-- EXPLORE: Inventory Mix
-- ============================================================================
-- 70 listings across 6 brands and 5 categories.

ASSERT ROW_COUNT = 6
ASSERT VALUE listing_count = 12 WHERE brand = 'AcmeAudio'
ASSERT VALUE listing_count = 12 WHERE brand = 'Bellweather'
ASSERT VALUE listing_count = 12 WHERE brand = 'Crestwood'
ASSERT VALUE listing_count = 12 WHERE brand = 'Driftvale'
ASSERT VALUE listing_count = 11 WHERE brand = 'Emberforge'
ASSERT VALUE listing_count = 11 WHERE brand = 'Foxkin'
SELECT brand,
       COUNT(*)         AS listing_count,
       SUM(stock)       AS total_stock,
       ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.delta_demos.marketplace_listings
GROUP BY brand
ORDER BY brand;


-- ============================================================================
-- LEARN: Warehouse SKU Lookup — `idx_sku` Wins
-- ============================================================================
-- Pulling SKU-EM-5002 for fulfillment. The selector picks idx_sku
-- because it offers the most narrowing for an equality predicate on
-- sku. The other two indexes are not applicable to this predicate.

ASSERT ROW_COUNT = 1
ASSERT VALUE sku = 'SKU-EM-5002'
ASSERT VALUE title = 'Enameled Dutch Oven'
ASSERT VALUE brand = 'Emberforge'
ASSERT VALUE price = 149.0
ASSERT VALUE stock = 41
SELECT sku, title, brand, category, price, stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE sku = 'SKU-EM-5002';


-- ============================================================================
-- LEARN: Storefront Brand Filter — `idx_brand` Wins
-- ============================================================================
-- Storefront landing page for Crestwood. The brand index narrows to
-- exactly the slices carrying Crestwood rows.

ASSERT ROW_COUNT = 1
ASSERT VALUE listing_count = 12
ASSERT VALUE total_stock = 1142
-- Non-deterministic: AVG of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE avg_price BETWEEN 85.91 AND 85.93
SELECT COUNT(*)              AS listing_count,
       SUM(stock)            AS total_stock,
       ROUND(AVG(price), 2)  AS avg_price
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE brand = 'Crestwood';


-- ============================================================================
-- LEARN: Faceted Browse — `idx_category_price` Wins
-- ============================================================================
-- Browse page: outdoor gear between $50 and $150. The composite
-- index uses both columns: leading category narrows the slices, then
-- the trailing price range narrows further within each.

ASSERT ROW_COUNT = 1
ASSERT VALUE listing_count = 5
ASSERT VALUE max_price = 139.0
ASSERT VALUE total_stock = 311
SELECT COUNT(*)              AS listing_count,
       MAX(price)            AS max_price,
       SUM(stock)            AS total_stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE category = 'outdoor'
  AND price BETWEEN 50 AND 150;


-- ============================================================================
-- LEARN: Leftmost-Prefix on the Composite — Still Helps
-- ============================================================================
-- A predicate on category alone uses idx_category_price's leading
-- column. The selector picks it; the trailing price column simply
-- isn't constrained.

ASSERT ROW_COUNT = 1
ASSERT VALUE listing_count = 23
ASSERT VALUE total_stock = 2022
SELECT COUNT(*)        AS listing_count,
       SUM(stock)      AS total_stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE category = 'apparel';


-- ============================================================================
-- LEARN: No Index Applies — Graceful Fallback
-- ============================================================================
-- Looking for low-stock alerts: predicate is on stock alone. None of
-- the three indexes is keyed on stock, so the selector falls back to
-- ordinary file pruning. The query still runs correctly — the index
-- subsystem simply does nothing.

ASSERT ROW_COUNT = 12
ASSERT VALUE stock < 20
SELECT listing_id, brand, category, title, stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE stock < 20
ORDER BY stock, listing_id;


-- ============================================================================
-- LEARN: SHOW INDEXES — Inventory of Available Indexes
-- ============================================================================
-- Operators inspect what's available. This is what the selector
-- consults internally for every query.

SHOW INDEXES ON TABLE {{zone_name}}.delta_demos.marketplace_listings;


-- ============================================================================
-- LEARN: DROP a Redundant Index
-- ============================================================================
-- Auditing reveals brand searches are rare; the index isn't worth
-- its storage and write cost. DROP removes it; future queries that
-- would have used it fall back to the next-best applicable index
-- (or to file pruning if none applies).

DROP INDEX IF EXISTS idx_brand
    ON TABLE {{zone_name}}.delta_demos.marketplace_listings;

SHOW INDEXES ON TABLE {{zone_name}}.delta_demos.marketplace_listings;


-- ============================================================================
-- LEARN: Brand Query After DROP — Same Answer, Different Path
-- ============================================================================
-- The same brand filter still works. Without idx_brand the selector
-- falls back to file pruning. Result is identical.

ASSERT ROW_COUNT = 1
ASSERT VALUE listing_count = 12
ASSERT VALUE total_stock = 1142
SELECT COUNT(*)              AS listing_count,
       SUM(stock)            AS total_stock
FROM {{zone_name}}.delta_demos.marketplace_listings
WHERE brand = 'Crestwood';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_listings = 70
ASSERT VALUE total_stock = 5284
ASSERT VALUE distinct_brands = 6
ASSERT VALUE distinct_categories = 5
ASSERT VALUE distinct_sellers = 6
-- Non-deterministic: SUM of DOUBLE column — minor float variance possible across platforms
ASSERT WARNING VALUE total_price BETWEEN 8454.4 AND 8454.6
SELECT COUNT(*)                              AS total_listings,
       SUM(stock)                            AS total_stock,
       COUNT(DISTINCT brand)                 AS distinct_brands,
       COUNT(DISTINCT category)              AS distinct_categories,
       COUNT(DISTINCT seller_id)             AS distinct_sellers,
       ROUND(SUM(price), 2)                  AS total_price
FROM {{zone_name}}.delta_demos.marketplace_listings;
