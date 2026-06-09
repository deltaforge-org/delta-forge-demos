-- ==========================================================================
-- Demo: Pacific Retail Group: Power BI Star Warehouse
-- ==========================================================================
-- A realistic Kimball star schema sized for serious Power BI workloads.
-- Four dimensions (date, store, product, customer) and three fact tables
-- (sales, inventory snapshots, web events) totalling ~506 million rows.
--
-- DATA REALISM (this is the point of the demo).
--
-- Earlier revisions derived every value from a low-period modular function
-- of the row index (cyclic_lookup = values[(i*m) % len], arithmetic =
-- (i*m) % mod + offset). That produced two artefacts that made the data
-- look obviously synthetic in a Power BI report:
--   1. Columns whose periods shared a factor became perfectly correlated.
--      city cycled with period 50 and number_of_children with period 5, so
--      every customer in a city had the identical child count.
--   2. A measure that is a pure sawtooth of the row index sums to the
--      identical total for every grouping, because each group receives a
--      statistically identical multiset of values. Every city showed the
--      same SUM(total_amount_usd).
--
-- This revision drives the columns that matter through the seeded
-- distribution kernels in df_generate_table (zipf_int, exponential, and a
-- single seeded geo rank), so:
--   * number_of_children is seeded independently of geography and follows a
--     realistic right-skewed household distribution (most households 0-1).
--   * geography is population-skewed and self-consistent: one seeded rank
--     selects aligned (city, state, region) values, so the top metros carry
--     proportionally more customers and stores, the way real markets do.
--   * order quantities, prices, discounts, lifetime revenue, inventory, and
--     session duration are sampled from skewed distributions, so per-group
--     aggregates fluctuate the way real-world data does.
--
-- DETERMINISM. Every kernel is a pure function of (seed, row_index). Two
-- runs produce bit-identical output. The assertions in queries.sql stay
-- closed-form: they pin row counts, primary-key sums (the keys are the row
-- index), distinct cardinalities, value ranges, and the two deterministic
-- flags (return_flag, is_bounce). None of them depend on a specific seed's
-- realisation, so any drift is a real regression.
--
-- SYNTHESIS PATH. df_generate_table is a streaming TableProvider that builds
-- Arrow batches in parallel Rust loops and yields them on demand to the
-- Delta writer; memory in flight is bounded to one chunk per partition, so a
-- 200M row fact does not materialise. Compound strings and the few derived
-- measures are computed in SQL on top of the streamed batches.
--
-- The three fact tables target 150 MB Parquet files (delta.targetFileSize)
-- to align with Power BI Import refresh and DirectQuery scan behaviour.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Pacific Retail Group analytics warehouse (Power BI benchmark)';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.retail
    COMMENT 'Star schema retail warehouse: 4 dimensions and 3 fact tables for Power BI';

-- ==========================================================================
-- DIMENSION TABLES
-- ==========================================================================

-- --------------------------------------------------------------------------
-- dim_date (7,305 rows, 16 cols)
-- 20 years of dates: 2010-01-01 through 2029-12-31. Standard PBI date table.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.dim_date (
    date_key            INT  NOT NULL,
    full_date           DATE NOT NULL,
    year                INT  NOT NULL,
    quarter             INT  NOT NULL,
    month               INT  NOT NULL,
    month_name          STRING NOT NULL,
    day_of_month        INT  NOT NULL,
    day_of_week         INT  NOT NULL,
    day_name            STRING NOT NULL,
    week_of_year        INT  NOT NULL,
    fiscal_year         INT  NOT NULL,
    fiscal_quarter      INT  NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_month_end        BOOLEAN NOT NULL,
    is_holiday          BOOLEAN NOT NULL,
    season              STRING NOT NULL
)
LOCATION '{{data_path}}/retail/dim_date';

-- --------------------------------------------------------------------------
-- dim_store (25,000 rows, 14 cols)
-- Physical and online store master. Geography is population-skewed and
-- self-consistent via a single seeded metro rank.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.dim_store (
    store_key            BIGINT NOT NULL,
    store_code          STRING NOT NULL,
    store_name          STRING NOT NULL,
    store_type          STRING NOT NULL,
    banner              STRING NOT NULL,
    city                STRING NOT NULL,
    state_code          STRING NOT NULL,
    region              STRING NOT NULL,
    square_feet         INT NOT NULL,
    employee_count      INT NOT NULL,
    annual_lease_usd    DECIMAL(18,2) NOT NULL,
    opening_date        DATE NOT NULL,
    is_active           BOOLEAN NOT NULL,
    has_pharmacy        BOOLEAN NOT NULL
)
LOCATION '{{data_path}}/retail/dim_store';

-- --------------------------------------------------------------------------
-- dim_product (1,000,000 rows, 14 cols)
-- Product master with a 3-level category hierarchy. Cost is right-skewed;
-- list price is a fixed markup over cost.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.dim_product (
    product_key              BIGINT NOT NULL,
    sku                     STRING NOT NULL,
    product_name            STRING NOT NULL,
    brand                   STRING NOT NULL,
    category_l1             STRING NOT NULL,
    category_l2             STRING NOT NULL,
    category_l3             STRING NOT NULL,
    color                   STRING NOT NULL,
    unit_cost_usd           DECIMAL(18,4) NOT NULL,
    list_price_usd          DECIMAL(18,4) NOT NULL,
    abc_class               STRING NOT NULL,
    is_active               BOOLEAN NOT NULL,
    is_seasonal             BOOLEAN NOT NULL,
    launch_date             DATE NOT NULL
)
LOCATION '{{data_path}}/retail/dim_product';

-- --------------------------------------------------------------------------
-- dim_customer (5,000,000 rows, 17 cols)
-- Customer master. number_of_children is seeded independently of geography
-- (the headline data-quality fix). lifetime_revenue is right-skewed.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.dim_customer (
    customer_key                 BIGINT NOT NULL,
    customer_code               STRING NOT NULL,
    full_name                   STRING NOT NULL,
    email                       STRING NOT NULL,
    gender                      STRING NOT NULL,
    birth_date                  DATE NOT NULL,
    age_band                    STRING NOT NULL,
    city                        STRING NOT NULL,
    state_code                  STRING NOT NULL,
    region                      STRING NOT NULL,
    number_of_children          INT NOT NULL,
    household_size              INT NOT NULL,
    loyalty_tier                STRING NOT NULL,
    segment                     STRING NOT NULL,
    signup_date                 DATE NOT NULL,
    lifetime_revenue_usd        DECIMAL(18,2) NOT NULL,
    marketing_opt_in            BOOLEAN NOT NULL
)
LOCATION '{{data_path}}/retail/dim_customer';

-- ==========================================================================
-- FACT TABLES
-- ==========================================================================

-- --------------------------------------------------------------------------
-- fact_sales (200,000,000 rows, 21 cols)
-- Order-line grain. Keys reference all four dimensions; measures are sampled
-- from skewed distributions so per-dimension aggregates fluctuate realistically.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.fact_sales (
    sale_id                     BIGINT NOT NULL,
    order_id                    BIGINT NOT NULL,
    line_number                 INT NOT NULL,
    date_key                    INT NOT NULL,
    order_date                  DATE NOT NULL,
    hour_of_day                 INT NOT NULL,
    customer_key                BIGINT NOT NULL,
    product_key                 BIGINT NOT NULL,
    store_key                   BIGINT NOT NULL,
    quantity                    INT NOT NULL,
    unit_price_usd              DECIMAL(18,4) NOT NULL,
    discount_pct                DOUBLE NOT NULL,
    gross_revenue_usd           DECIMAL(18,4) NOT NULL,
    discount_amt_usd            DECIMAL(18,4) NOT NULL,
    tax_amt_usd                 DECIMAL(18,4) NOT NULL,
    total_amount_usd            DECIMAL(18,4) NOT NULL,
    sales_channel               STRING NOT NULL,
    payment_method              STRING NOT NULL,
    order_status                STRING NOT NULL,
    return_flag                 BOOLEAN NOT NULL,
    currency_code               STRING NOT NULL
)
LOCATION '{{data_path}}/retail/fact_sales'
TBLPROPERTIES (
    'delta.targetFileSize' = '157286400'
);

-- --------------------------------------------------------------------------
-- fact_inventory_snapshot (100,000,000 rows, 12 cols)
-- Daily store-and-product inventory snapshots over a 365 day window.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.fact_inventory_snapshot (
    inventory_snapshot_id           BIGINT NOT NULL,
    snapshot_date                   DATE NOT NULL,
    date_key               INT NOT NULL,
    store_key                       BIGINT NOT NULL,
    product_key                     BIGINT NOT NULL,
    on_hand_units                   INT NOT NULL,
    reorder_point                   INT NOT NULL,
    available_units                 INT NOT NULL,
    retail_value_usd                DECIMAL(18,4) NOT NULL,
    stock_status                    STRING NOT NULL,
    abc_classification              STRING NOT NULL,
    store_region                    STRING NOT NULL
)
LOCATION '{{data_path}}/retail/fact_inventory_snapshot'
TBLPROPERTIES (
    'delta.targetFileSize' = '157286400'
);

-- --------------------------------------------------------------------------
-- fact_web_events (200,000,000 rows, 13 cols)
-- Clickstream event log. event_type follows a realistic funnel skew; device
-- and browser follow market-share skew.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.fact_web_events (
    event_id                BIGINT NOT NULL,
    session_id              STRING NOT NULL,
    customer_key            BIGINT NOT NULL,
    event_date              DATE NOT NULL,
    date_key          INT NOT NULL,
    event_type              STRING NOT NULL,
    device_type             STRING NOT NULL,
    browser                 STRING NOT NULL,
    time_on_page_sec        INT NOT NULL,
    is_bounce               BOOLEAN NOT NULL,
    conversion_value_usd    DECIMAL(18,4) NOT NULL,
    products_viewed_count   INT NOT NULL,
    search_query            STRING
)
LOCATION '{{data_path}}/retail/fact_web_events'
TBLPROPERTIES (
    'delta.targetFileSize' = '157286400'
);

-- ==========================================================================
-- POPULATION
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Populate dim_date (7,305 rows). Dates 2010-01-01 .. 2029-12-31. The
-- calendar attributes are computed from the real date, so this dimension is
-- realistic by construction.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_date
SELECT
    CAST(year(d) * 10000 + month(d) * 100 + dayofmonth(d) AS INT)       AS date_key,
    d                                                                   AS full_date,
    CAST(year(d) AS INT)                                                AS year,
    CAST(quarter(d) AS INT)                                             AS quarter,
    CAST(month(d) AS INT)                                               AS month,
    date_format(d, 'MMMM')                                              AS month_name,
    CAST(dayofmonth(d) AS INT)                                          AS day_of_month,
    CAST(dayofweek(d) AS INT)                                           AS day_of_week,
    date_format(d, 'EEEE')                                              AS day_name,
    CAST(weekofyear(d) AS INT)                                          AS week_of_year,
    CAST(CASE WHEN month(d) >= 4 THEN year(d) + 1 ELSE year(d) END AS INT) AS fiscal_year,
    CAST(CASE
        WHEN month(d) IN (4, 5, 6)    THEN 1
        WHEN month(d) IN (7, 8, 9)    THEN 2
        WHEN month(d) IN (10, 11, 12) THEN 3
        ELSE 4
    END AS INT)                                                         AS fiscal_quarter,
    dayofweek(d) IN (1, 7)                                              AS is_weekend,
    d = last_day(d)                                                     AS is_month_end,
    (month(d) = 1  AND dayofmonth(d) = 1)
        OR (month(d) = 7  AND dayofmonth(d) = 4)
        OR (month(d) = 12 AND dayofmonth(d) = 25)
        OR (month(d) = 11 AND dayofmonth(d) BETWEEN 22 AND 28 AND dayofweek(d) = 5)
                                                                        AS is_holiday,
    CASE
        WHEN month(d) IN (12, 1, 2) THEN 'Winter'
        WHEN month(d) IN (3, 4, 5)  THEN 'Spring'
        WHEN month(d) IN (6, 7, 8)  THEN 'Summer'
        ELSE 'Autumn'
    END                                                                 AS season
FROM (
    SELECT DATE '2010-01-01' + CAST(rn AS INT) AS d
    FROM generate_series(0, 7304) AS t(rn)
) s;

-- --------------------------------------------------------------------------
-- Populate dim_store (25,000 rows). geo_rank is a seeded Zipf rank over 20
-- US metros (rank 1 = New York is most common), so stores cluster in the
-- largest markets. city / state / region are read from aligned arrays by
-- that single rank, so geography is always self-consistent.
-- square_feet is right-skewed; employee_count and annual_lease are derived
-- from it so a bigger store has proportionally more staff and a higher lease.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_store
SELECT
    store_key,
    store_code,
    store_name,
    store_type,
    banner,
    city,
    state_code,
    region,
    square_feet,
    CAST(LEAST(GREATEST(square_feet / 450, 4), 600) AS INT)             AS employee_count,
    CAST(ROUND(square_feet * 26.0, 2) AS DECIMAL(18,2))                 AS annual_lease_usd,
    opening_date,
    is_active,
    has_pharmacy
FROM (
    SELECT
        g.store_key,
        g.store_code,
        concat(g.banner, ' #', CAST(g.store_key AS STRING))             AS store_name,
        g.store_type,
        g.banner,
        element_at(array('New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','Columbus','Charlotte','Indianapolis','Seattle','Denver','Boston','Nashville','Detroit'), g.geo_rank) AS city,
        element_at(array('NY','CA','IL','TX','AZ','PA','TX','CA','TX','CA','TX','FL','OH','NC','IN','WA','CO','MA','TN','MI'), g.geo_rank) AS state_code,
        element_at(array('Northeast','West','Midwest','South','West','Northeast','South','West','South','West','South','South','Midwest','South','Midwest','West','West','Northeast','South','Midwest'), g.geo_rank) AS region,
        CAST(LEAST(GREATEST(3000.0 + g.sqft_noise, 3000.0), 200000.0) AS INT) AS square_feet,
        g.opening_date,
        g.is_active,
        g.has_pharmacy
    FROM df_generate_table(25000, '[
        {"type": "row_index",     "name": "store_key", "start": 1},
        {"type": "id_sequence",   "name": "store_code", "prefix": "STORE-", "pad": 6, "start": 1},
        {"type": "cyclic_lookup", "name": "store_type", "values": ["Hypermarket","Supermarket","Express","Online","Marketplace"]},
        {"type": "cyclic_lookup", "name": "banner",     "values": ["PacificMart","PacificFresh","PacificDirect","PacificClub","PacificExpress"]},
        {"type": "zipf_int",      "name": "geo_rank",   "n": 20, "alpha": 0.65, "seed": 1101},
        {"type": "exponential_distribution", "name": "sqft_noise", "lambda": 0.00004, "seed": 1102},
        {"type": "date",          "name": "opening_date","base": "2005-01-01", "multiplier": 7, "modulo": 7305},
        {"type": "boolean_cond",  "name": "is_active",  "modulo": 50, "ne": 0},
        {"type": "boolean_cond",  "name": "has_pharmacy","modulo": 3, "eq": 0}
    ]') g
) s;

-- --------------------------------------------------------------------------
-- Populate dim_product (1,000,000 rows). brand and the three category levels
-- cycle so the slicer cardinalities are clean (50 brands, 10 / 20 / 50
-- categories). unit_cost is right-skewed (most products cheap, a long tail of
-- expensive ones); list_price is a 1.6x markup.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_product
SELECT
    g.product_key,
    g.sku,
    concat(g.brand, ' ', g.category_l3)                                 AS product_name,
    g.brand,
    g.category_l1,
    g.category_l2,
    g.category_l3,
    g.color,
    CAST(ROUND(LEAST(GREATEST(g.cost_noise, 0.5), 3000.0), 2) AS DECIMAL(18,4))        AS unit_cost_usd,
    CAST(ROUND(LEAST(GREATEST(g.cost_noise, 0.5), 3000.0) * 1.6, 2) AS DECIMAL(18,4))  AS list_price_usd,
    g.abc_class,
    g.is_active,
    g.is_seasonal,
    g.launch_date
FROM df_generate_table(1000000, '[
    {"type": "row_index",     "name": "product_key", "start": 1},
    {"type": "id_sequence",   "name": "sku", "prefix": "SKU-", "pad": 8, "start": 1},
    {"type": "cyclic_lookup", "name": "brand",        "values": ["Acme","Globex","Initech","Umbrella","Wayne","Stark","Soylent","Cyberdyne","Tyrell","Wonka","Hooli","Vandelay","Gringotts","Massive","Oscorp","LexCorp","PymTech","Roxxon","Frobozz","Aperture","PaperStreet","DunderMifflin","Sterling","Pawnee","Cogswell","Spacely","Yoyodyne","Strickland","VaultTec","PizzaPlanet","Bluth","Costanza","Kramerica","SterlingCooper","Pendant","InGen","Weyland","OmniCorp","MassiveDynamic","Gekko","Initrode","BuyMore","Rekall","Macguffin","Spadina","Vance","Zorin","Zapf","Encom","Nakatomi"]},
    {"type": "cyclic_lookup", "name": "category_l1",  "values": ["Electronics","Apparel","Home","Grocery","Toys","Sports","Beauty","Books","Office","Pet"]},
    {"type": "cyclic_lookup", "name": "category_l2",  "values": ["Phones","Computers","Audio","TVs","Cameras","Tops","Bottoms","Shoes","Furniture","Decor","Beverages","Snacks","Frozen","OutdoorToys","BoardGames","Fitness","OutdoorGear","Skincare","Haircare","Fiction"]},
    {"type": "cyclic_lookup", "name": "category_l3",  "values": ["Smartphones","Tablets","Laptops","Desktops","Headphones","Speakers","LEDTVs","OLEDTVs","DSLR","Mirrorless","TShirts","Polos","Jeans","Shorts","Sneakers","Boots","Sofas","Tables","WallArt","Vases","Sodas","Juices","Chips","Cookies","IceCream","FrozenMeals","ActionFigures","BuildingBlocks","CardGames","StrategyGames","Yoga","Cardio","Tents","Backpacks","Cleansers","Moisturizers","Shampoos","Conditioners","Novels","Biographies","Pens","Notebooks","Toys","Treats","Beds","Carriers","Vitamins","Wraps","Cookware","Mugs"]},
    {"type": "cyclic_lookup", "name": "color",        "values": ["Red","Blue","Green","Black","White","Gray","Silver","Gold","Yellow","Orange"]},
    {"type": "exponential_distribution", "name": "cost_noise", "lambda": 0.045, "seed": 1201},
    {"type": "cyclic_lookup", "name": "abc_class",    "values": ["A","B","C","D"]},
    {"type": "boolean_cond",  "name": "is_active",    "modulo": 50, "ne": 0},
    {"type": "boolean_cond",  "name": "is_seasonal",  "modulo": 4,  "eq": 0},
    {"type": "date",          "name": "launch_date",  "base": "2012-01-01", "multiplier": 7, "modulo": 4380}
]') g;

-- --------------------------------------------------------------------------
-- Populate dim_customer (5,000,000 rows). THE HEADLINE FIX:
--   * geo_rank (seed 1301) is a Zipf rank over the 20 metros, so cities have
--     proportionally different customer counts (population skew).
--   * children_rank (seed 1303) is a SEPARATE seeded Zipf draw, so
--     number_of_children is statistically independent of city. It is no
--     longer pinned to geography, and it follows a realistic right-skewed
--     household distribution (most 0, fewer 1, etc.).
-- lifetime_revenue is right-skewed. age_band is derived from birth_date.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_customer
SELECT
    g.customer_key,
    g.customer_code,
    concat(g.first_name, ' ', g.last_name)                              AS full_name,
    lower(concat(g.first_name, '.', g.last_name, CAST(g.customer_key AS STRING), '@', g.email_domain, '.com')) AS email,
    g.gender,
    g.birth_date,
    CASE
        WHEN 2025 - year(g.birth_date) < 25 THEN '18-24'
        WHEN 2025 - year(g.birth_date) < 35 THEN '25-34'
        WHEN 2025 - year(g.birth_date) < 45 THEN '35-44'
        WHEN 2025 - year(g.birth_date) < 55 THEN '45-54'
        WHEN 2025 - year(g.birth_date) < 65 THEN '55-64'
        ELSE '65+'
    END                                                                 AS age_band,
    element_at(array('New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','Columbus','Charlotte','Indianapolis','Seattle','Denver','Boston','Nashville','Detroit'), g.geo_rank) AS city,
    element_at(array('NY','CA','IL','TX','AZ','PA','TX','CA','TX','CA','TX','FL','OH','NC','IN','WA','CO','MA','TN','MI'), g.geo_rank) AS state_code,
    element_at(array('Northeast','West','Midwest','South','West','Northeast','South','West','South','West','South','South','Midwest','South','Midwest','West','West','Northeast','South','Midwest'), g.geo_rank) AS region,
    g.children_rank - 1                                                 AS number_of_children,
    (g.children_rank - 1) + g.adults_rank                               AS household_size,
    g.loyalty_tier,
    g.segment,
    g.signup_date,
    CAST(ROUND(LEAST(GREATEST(g.ltr_noise, 0.0), 250000.0), 2) AS DECIMAL(18,2)) AS lifetime_revenue_usd,
    g.marketing_opt_in
FROM df_generate_table(5000000, '[
    {"type": "row_index",     "name": "customer_key",   "start": 1},
    {"type": "id_sequence",   "name": "customer_code", "prefix": "CUST-", "pad": 8, "start": 1},
    {"type": "cyclic_lookup", "name": "first_name",    "values": ["Alice","Bob","Carol","David","Emma","Frank","Grace","Henry","Iris","Jack","Karen","Leo","Maya","Noah","Olivia","Peter","Quinn","Rachel","Steve","Tina","Uma","Victor","Wendy","Xander","Yara","Zoe","Aaron","Beth","Chris","Diana","Ethan","Fiona","George","Hannah","Ian","Julia","Kevin","Laura","Mike","Nina","Oscar","Paula","Quentin","Rose","Sam","Tara","Umar","Vera","Will","Xenia"]},
    {"type": "cyclic_lookup", "name": "last_name",     "values": ["Smith","Jones","Brown","Davis","Miller","Wilson","Moore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez"], "offset_multiplier": 7},
    {"type": "cyclic_lookup", "name": "email_domain",  "values": ["gmail","outlook","yahoo","icloud","proton"]},
    {"type": "cyclic_lookup", "name": "gender",        "values": ["Female","Male","Nonbinary"]},
    {"type": "date",          "name": "birth_date",    "base": "1945-01-01", "multiplier": 13, "modulo": 27375},
    {"type": "zipf_int",      "name": "geo_rank",      "n": 20, "alpha": 0.65, "seed": 1301},
    {"type": "zipf_int",      "name": "children_rank", "n": 5,  "alpha": 1.15, "seed": 1303},
    {"type": "zipf_int",      "name": "adults_rank",   "n": 2,  "alpha": 0.9,  "seed": 1304},
    {"type": "cyclic_lookup", "name": "loyalty_tier",  "values": ["Bronze","Silver","Gold","Platinum","Diamond"]},
    {"type": "cyclic_lookup", "name": "segment",       "values": ["New","Active","AtRisk","VIP","Inactive"]},
    {"type": "date",          "name": "signup_date",   "base": "2018-01-01", "multiplier": 7, "modulo": 2557},
    {"type": "exponential_distribution", "name": "ltr_noise", "lambda": 0.0004, "seed": 1306},
    {"type": "boolean_cond",  "name": "marketing_opt_in", "modulo": 3, "eq": 0}
]') g;

-- --------------------------------------------------------------------------
-- Populate fact_sales (200,000,000 rows). Order-line grain.
--   * Keys reference all four dims via (i * prime) % card + 1, staying in
--     range so every FK lands on a real dim row.
--   * quantity is a Zipf draw (most lines 1-2 units), price is right-skewed.
--   * total_amount_usd is built from those seeded measures, so its SUM over
--     any dimension (city, channel, brand, ...) fluctuates the way real
--     sales do, instead of collapsing to one identical number per group.
--   * order_date is recency-skewed (exponential days-ago), so recent periods
--     carry more volume, like a growing business.
--   * return_flag stays deterministic (sale_id % 20 = 0) so its count is
--     closed-form (exactly N / 20).
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.fact_sales
SELECT
    sale_id,
    CAST((sale_id - 1) / 3 AS BIGINT) + 1                               AS order_id,
    CAST((sale_id - 1) % 3 AS INT) + 1                                  AS line_number,
    CAST(year(order_date) * 10000 + month(order_date) * 100 + dayofmonth(order_date) AS INT) AS date_key,
    order_date,
    hour_of_day,
    customer_key,
    product_key,
    store_key,
    quantity,
    CAST(ROUND(unit_price, 2) AS DECIMAL(18,4))                         AS unit_price_usd,
    ROUND(discount_pct, 4)                                              AS discount_pct,
    CAST(ROUND(quantity * unit_price, 4) AS DECIMAL(18,4))              AS gross_revenue_usd,
    CAST(ROUND(quantity * unit_price * discount_pct, 4) AS DECIMAL(18,4)) AS discount_amt_usd,
    CAST(ROUND(quantity * unit_price * (1.0 - discount_pct) * 0.08, 4) AS DECIMAL(18,4)) AS tax_amt_usd,
    CAST(ROUND(quantity * unit_price * (1.0 - discount_pct) * 1.08, 4) AS DECIMAL(18,4)) AS total_amount_usd,
    sales_channel,
    payment_method,
    order_status,
    return_flag,
    'USD'                                                               AS currency_code
FROM (
    SELECT
        g.sale_id,
        DATE '2020-01-01' + CAST(1824 - LEAST(GREATEST(CAST(g.days_ago AS INT), 0), 1824) AS INT) AS order_date,
        element_at(array(12,13,18,19,11,14,17,20,15,16,10,21,9,22,8,23,7,0,1,6,2,5,3,4), g.hour_rank) AS hour_of_day,
        g.customer_key,
        g.product_key,
        g.store_key,
        g.quantity,
        LEAST(GREATEST(g.price_noise, 1.0), 1500.0)                     AS unit_price,
        LEAST(GREATEST(g.discount_noise, 0.0), 0.6)                     AS discount_pct,
        g.sales_channel,
        g.payment_method,
        g.order_status,
        g.return_flag
    FROM df_generate_table(200000000, '[
        {"type": "row_index",     "name": "sale_id", "start": 1},
        {"type": "arithmetic",    "name": "customer_key", "multiplier": 17, "modulo": 5000000, "offset": 1},
        {"type": "arithmetic",    "name": "product_key",  "multiplier": 13, "modulo": 1000000, "offset": 1},
        {"type": "arithmetic",    "name": "store_key",    "multiplier": 7,  "modulo": 25000,   "offset": 1},
        {"type": "exponential_distribution", "name": "days_ago",  "lambda": 0.00274, "seed": 1401},
        {"type": "zipf_int",      "name": "hour_rank",    "n": 24, "alpha": 0.4, "seed": 1402},
        {"type": "zipf_int",      "name": "quantity",     "n": 8,  "alpha": 1.2, "seed": 1403},
        {"type": "exponential_distribution", "name": "price_noise",    "lambda": 0.025, "seed": 1404},
        {"type": "exponential_distribution", "name": "discount_noise", "lambda": 16.0,  "seed": 1405},
        {"type": "cyclic_lookup", "name": "sales_channel",  "values": ["In-Store","Online","Mobile App","Phone","Marketplace"]},
        {"type": "cyclic_lookup", "name": "payment_method", "values": ["Credit Card","Debit Card","Cash","Mobile Wallet","Gift Card","Bank Transfer"]},
        {"type": "cyclic_lookup", "name": "order_status",   "values": ["Pending","Confirmed","Shipped","Delivered"]},
        {"type": "boolean_cond",  "name": "return_flag",    "modulo": 20, "eq": 0}
    ]') g
) t;

-- --------------------------------------------------------------------------
-- Populate fact_inventory_snapshot (100,000,000 rows). Daily store-and-product
-- snapshots over a 365 day window starting 2024-01-01. on_hand is right-skewed;
-- reorder_point and retail_value are derived from it.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.fact_inventory_snapshot
SELECT
    inventory_snapshot_id,
    snapshot_date,
    CAST(year(snapshot_date) * 10000 + month(snapshot_date) * 100 + dayofmonth(snapshot_date) AS INT) AS date_key,
    store_key,
    product_key,
    on_hand_units,
    CAST(LEAST(GREATEST(on_hand_units / 5, 10), 500) AS INT)            AS reorder_point,
    CAST(GREATEST(on_hand_units - allocated_units, 0) AS INT)          AS available_units,
    CAST(ROUND(on_hand_units * retail_unit, 2) AS DECIMAL(18,4))        AS retail_value_usd,
    stock_status,
    abc_classification,
    store_region
FROM (
    SELECT
        g.inventory_snapshot_id,
        g.snapshot_date,
        g.store_key,
        g.product_key,
        CAST(LEAST(GREATEST(g.onhand_noise, 0.0), 5000.0) AS INT)       AS on_hand_units,
        g.allocated_units,
        LEAST(GREATEST(g.retail_noise, 1.0), 800.0)                     AS retail_unit,
        g.stock_status,
        g.abc_classification,
        element_at(array('Northeast','West','Midwest','South','Online'), g.region_rank) AS store_region
    FROM df_generate_table(100000000, '[
        {"type": "row_index",     "name": "inventory_snapshot_id", "start": 1},
        {"type": "date",          "name": "snapshot_date", "base": "2024-01-01", "multiplier": 1, "modulo": 365},
        {"type": "arithmetic",    "name": "store_key",     "multiplier": 7,  "modulo": 25000,   "offset": 1},
        {"type": "arithmetic",    "name": "product_key",   "multiplier": 13, "modulo": 1000000, "offset": 1},
        {"type": "exponential_distribution", "name": "onhand_noise", "lambda": 0.004, "seed": 1501},
        {"type": "arithmetic",    "name": "allocated_units", "multiplier": 3, "modulo": 100, "offset": 0},
        {"type": "exponential_distribution", "name": "retail_noise", "lambda": 0.033, "seed": 1503},
        {"type": "cyclic_lookup", "name": "stock_status",  "values": ["In Stock","Low Stock","Out of Stock","Overstock","Discontinued"]},
        {"type": "cyclic_lookup", "name": "abc_classification", "values": ["A","B","C","D"]},
        {"type": "zipf_int",      "name": "region_rank",   "n": 5, "alpha": 0.5, "seed": 1505}
    ]') g
) s;

-- --------------------------------------------------------------------------
-- Populate fact_web_events (200,000,000 rows). Clickstream over 365 days
-- starting 2024-01-01.
--   * event_type follows a realistic funnel skew (page_view most common,
--     checkout_complete rare) via a Zipf rank over a frequency-ordered array.
--   * device_type and browser follow market-share skew.
--   * is_bounce stays deterministic (event_id % 10 = 0) so its count is
--     closed-form (exactly N / 10).
--   * conversion_value is non-zero only on checkout_complete events.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.fact_web_events
SELECT
    event_id,
    concat('SESS-', lpad(CAST(event_id / 5 + 1 AS STRING), 10, '0'))    AS session_id,
    customer_key,
    event_date,
    CAST(year(event_date) * 10000 + month(event_date) * 100 + dayofmonth(event_date) AS INT) AS date_key,
    event_type,
    device_type,
    browser,
    time_on_page_sec,
    is_bounce,
    CASE WHEN event_type = 'checkout_complete'
        THEN CAST(ROUND(conv_value, 2) AS DECIMAL(18,4))
        ELSE CAST(0 AS DECIMAL(18,4))
    END                                                                 AS conversion_value_usd,
    products_viewed_count,
    CASE WHEN event_type = 'search'
        THEN concat('q-', CAST(search_q_id AS STRING))
        ELSE NULL
    END                                                                 AS search_query
FROM (
    SELECT
        g.event_id,
        DATE '2024-01-01' + CAST(364 - LEAST(GREATEST(CAST(g.days_ago AS INT), 0), 364) AS INT) AS event_date,
        g.customer_key,
        element_at(array('page_view','product_view','search','add_to_cart','click_recommendation','wishlist_add','remove_from_cart','checkout_start','checkout_complete','share'), g.event_type_rank) AS event_type,
        element_at(array('Mobile','Desktop','Tablet','Smart TV'), g.device_rank) AS device_type,
        element_at(array('Chrome','Safari','Edge','Firefox','Samsung','Opera'), g.browser_rank) AS browser,
        CAST(LEAST(GREATEST(g.time_noise, 1.0), 3600.0) AS INT)         AS time_on_page_sec,
        g.is_bounce,
        LEAST(GREATEST(g.conv_noise, 1.0), 5000.0)                      AS conv_value,
        g.products_viewed_count,
        g.search_q_id
    FROM df_generate_table(200000000, '[
        {"type": "row_index",     "name": "event_id", "start": 1},
        {"type": "arithmetic",    "name": "customer_key", "multiplier": 17, "modulo": 5000000, "offset": 1},
        {"type": "exponential_distribution", "name": "days_ago", "lambda": 0.0083, "seed": 1601},
        {"type": "zipf_int",      "name": "event_type_rank", "n": 10, "alpha": 1.1, "seed": 1602},
        {"type": "zipf_int",      "name": "device_rank",     "n": 4,  "alpha": 0.6, "seed": 1603},
        {"type": "zipf_int",      "name": "browser_rank",    "n": 6,  "alpha": 0.6, "seed": 1604},
        {"type": "exponential_distribution", "name": "time_noise", "lambda": 0.011, "seed": 1605},
        {"type": "boolean_cond",  "name": "is_bounce",       "modulo": 10, "eq": 0},
        {"type": "exponential_distribution", "name": "conv_noise", "lambda": 0.008, "seed": 1607},
        {"type": "arithmetic",    "name": "products_viewed_count", "multiplier": 1, "modulo": 20, "offset": 0},
        {"type": "arithmetic",    "name": "search_q_id",     "multiplier": 1, "modulo": 1000, "offset": 0}
    ]') g
) t;

-- ==========================================================================
-- Schema Detection
-- ==========================================================================

DETECT SCHEMA FOR TABLE {{zone_name}}.retail.dim_date;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.dim_store;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.dim_product;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.dim_customer;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.fact_sales;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.fact_inventory_snapshot;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.fact_web_events;
