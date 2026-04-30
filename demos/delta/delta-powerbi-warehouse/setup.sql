-- ==========================================================================
-- Demo: Pacific Retail Group: Power BI Star Warehouse Benchmark
-- ==========================================================================
-- Realistic star schema sized for serious Power BI workloads. Four wide
-- dimensions (date, store, product, customer) and three wide fact tables
-- (sales, inventory snapshots, web events) totalling ~506 million rows
-- and ~265 columns. Every value is row-index-derived so two runs are
-- deterministic and any drift is real.
--
-- Synthesis path: the dim and fact INSERTs use df_generate_table, a
-- streaming TableProvider that builds Arrow batches in tight Rust loops
-- (parallelised across cores) and yields them on demand to the Delta
-- writer. Memory in flight is bounded to one chunk per partition, so a
-- 200M row fact does not OOM and a 1B row variant is feasible. Star
-- schema integrity is preserved by keeping the same `(rn * M) % N + 1`
-- formulas as the dim primary keys: every fact FK lands on a real dim
-- row.
--
-- The few compound strings (full_name, address_line, fiscal_year_label)
-- and decimal casts that the spec composer cannot express natively are
-- derived in SQL on top of the streamed batches at vectorised speed.
--
-- File sizing inherits the workspace default (delta.targetFileSize = 256 MB,
-- the value Databricks autotune targets for tables under 2.56 TB). The
-- writer rotates files at that size by direct measurement of bytes-on-disk;
-- no per-table override is needed.
--
-- Setup time target: minutes, not hours. The demo exists to drive ODBC
-- perf measurement against a workload that looks like a production
-- Power BI warehouse, and the previous SQL-only synthesis path made
-- iterating on the warehouse contents itself painful.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Pacific Retail Group analytics warehouse (Power BI benchmark)';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.retail
    COMMENT 'Star schema retail warehouse: 4 dims and 3 wide fact tables for Power BI';

-- ==========================================================================
-- DIMENSION TABLES
-- ==========================================================================

-- --------------------------------------------------------------------------
-- dim_date (7,305 rows, 24 cols)
-- 20 years of dates: 2010-01-01 through 2029-12-31. Standard PBI date table.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.dim_date (
    date_key            INT  NOT NULL,
    full_date           DATE NOT NULL,
    year                INT  NOT NULL,
    quarter             INT  NOT NULL,
    quarter_name        STRING NOT NULL,
    quarter_year        STRING NOT NULL,
    month               INT  NOT NULL,
    month_name          STRING NOT NULL,
    month_short         STRING NOT NULL,
    month_year          STRING NOT NULL,
    day_of_month        INT  NOT NULL,
    day_of_year         INT  NOT NULL,
    day_of_week         INT  NOT NULL,
    day_name            STRING NOT NULL,
    day_short           STRING NOT NULL,
    week_of_year        INT  NOT NULL,
    fiscal_year         INT  NOT NULL,
    fiscal_quarter      INT  NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_month_end        BOOLEAN NOT NULL,
    is_quarter_end      BOOLEAN NOT NULL,
    is_year_end         BOOLEAN NOT NULL,
    is_holiday          BOOLEAN NOT NULL,
    season              STRING NOT NULL
)
LOCATION '{{data_path}}/retail/dim_date';

-- --------------------------------------------------------------------------
-- dim_store (25,000 rows, 30 cols)
-- Physical and online store master.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.dim_store (
    store_id            BIGINT NOT NULL,
    store_code          STRING NOT NULL,
    store_name          STRING NOT NULL,
    store_type          STRING NOT NULL,
    banner              STRING NOT NULL,
    format              STRING NOT NULL,
    address_line        STRING NOT NULL,
    city                STRING NOT NULL,
    state_code          STRING NOT NULL,
    state_name          STRING NOT NULL,
    postal_code         STRING NOT NULL,
    country_code        STRING NOT NULL,
    country_name        STRING NOT NULL,
    region              STRING NOT NULL,
    district            STRING NOT NULL,
    division            STRING NOT NULL,
    latitude            DOUBLE NOT NULL,
    longitude           DOUBLE NOT NULL,
    square_feet         INT NOT NULL,
    opening_date        DATE NOT NULL,
    closing_date        DATE,
    is_active           BOOLEAN NOT NULL,
    manager_name        STRING NOT NULL,
    employee_count      INT NOT NULL,
    annual_lease_usd    DECIMAL(18,2) NOT NULL,
    has_pharmacy        BOOLEAN NOT NULL,
    has_grocery         BOOLEAN NOT NULL,
    has_electronics     BOOLEAN NOT NULL,
    has_garden          BOOLEAN NOT NULL,
    has_cafe            BOOLEAN NOT NULL,
    target_segment      STRING NOT NULL
)
LOCATION '{{data_path}}/retail/dim_store';

-- --------------------------------------------------------------------------
-- dim_product (1,000,000 rows, 36 cols)
-- Product master with 3-level category hierarchy.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.dim_product (
    product_id              BIGINT NOT NULL,
    sku                     STRING NOT NULL,
    upc_barcode             STRING NOT NULL,
    product_name            STRING NOT NULL,
    short_description       STRING NOT NULL,
    brand                   STRING NOT NULL,
    manufacturer            STRING NOT NULL,
    category_l1             STRING NOT NULL,
    category_l2             STRING NOT NULL,
    category_l3             STRING NOT NULL,
    department              STRING NOT NULL,
    subdepartment           STRING NOT NULL,
    color                   STRING NOT NULL,
    color_family            STRING NOT NULL,
    size_label              STRING NOT NULL,
    weight_grams            DOUBLE NOT NULL,
    length_cm               DOUBLE NOT NULL,
    width_cm                DOUBLE NOT NULL,
    height_cm               DOUBLE NOT NULL,
    package_count           INT NOT NULL,
    unit_cost_usd           DECIMAL(18,4) NOT NULL,
    list_price_usd          DECIMAL(18,4) NOT NULL,
    msrp_usd                DECIMAL(18,4) NOT NULL,
    default_margin_pct      DOUBLE NOT NULL,
    supplier_id             BIGINT NOT NULL,
    supplier_name           STRING NOT NULL,
    country_of_origin       STRING NOT NULL,
    hs_tariff_code          STRING NOT NULL,
    energy_rating           STRING NOT NULL,
    package_type            STRING NOT NULL,
    units_per_case          INT NOT NULL,
    lead_time_days          INT NOT NULL,
    launch_date             DATE NOT NULL,
    discontinued_date       DATE,
    is_active               BOOLEAN NOT NULL,
    is_seasonal             BOOLEAN NOT NULL,
    is_eco_certified        BOOLEAN NOT NULL,
    is_taxable              BOOLEAN NOT NULL,
    abc_class               STRING NOT NULL
)
LOCATION '{{data_path}}/retail/dim_product';

-- --------------------------------------------------------------------------
-- dim_customer (5,000,000 rows, 40 cols)
-- Customer master with demographics, geography, loyalty, and segment.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.dim_customer (
    customer_id                 BIGINT NOT NULL,
    customer_code               STRING NOT NULL,
    salutation                  STRING NOT NULL,
    first_name                  STRING NOT NULL,
    middle_initial              STRING NOT NULL,
    last_name                   STRING NOT NULL,
    full_name                   STRING NOT NULL,
    email                       STRING NOT NULL,
    phone_e164                  STRING NOT NULL,
    gender                      STRING NOT NULL,
    birth_date                  DATE NOT NULL,
    age_band                    STRING NOT NULL,
    marital_status              STRING NOT NULL,
    education_level             STRING NOT NULL,
    occupation                  STRING NOT NULL,
    employer_name               STRING NOT NULL,
    annual_income_usd           DECIMAL(18,2) NOT NULL,
    income_band                 STRING NOT NULL,
    household_size              INT NOT NULL,
    number_of_children          INT NOT NULL,
    address_line_1              STRING NOT NULL,
    address_line_2              STRING,
    city                        STRING NOT NULL,
    state_code                  STRING NOT NULL,
    state_name                  STRING NOT NULL,
    postal_code                 STRING NOT NULL,
    country_code                STRING NOT NULL,
    country_name                STRING NOT NULL,
    region                      STRING NOT NULL,
    latitude                    DOUBLE NOT NULL,
    longitude                   DOUBLE NOT NULL,
    signup_date                 DATE NOT NULL,
    signup_channel              STRING NOT NULL,
    preferred_contact_channel   STRING NOT NULL,
    marketing_opt_in            BOOLEAN NOT NULL,
    sms_opt_in                  BOOLEAN NOT NULL,
    loyalty_tier                STRING NOT NULL,
    loyalty_points_balance      INT NOT NULL,
    lifetime_orders             INT NOT NULL,
    lifetime_revenue_usd        DECIMAL(18,2) NOT NULL,
    last_purchase_date          DATE NOT NULL,
    churn_risk_score            DOUBLE NOT NULL,
    segment                     STRING NOT NULL
)
LOCATION '{{data_path}}/retail/dim_customer';

-- ==========================================================================
-- FACT TABLES
-- ==========================================================================

-- --------------------------------------------------------------------------
-- fact_sales (200,000,000 rows, 75 cols)
-- Order-line grain. Includes denormalized customer / product / store
-- columns so PBI Import slicers do not require joins.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.fact_sales (
    sale_id                     BIGINT NOT NULL,
    order_id                    BIGINT NOT NULL,
    line_number                 INT NOT NULL,
    receipt_number              STRING NOT NULL,
    transaction_uuid            STRING NOT NULL,
    date_key                    INT NOT NULL,
    customer_key                BIGINT NOT NULL,
    product_key                 BIGINT NOT NULL,
    store_key                   BIGINT NOT NULL,
    employee_key                BIGINT NOT NULL,
    promotion_key               BIGINT NOT NULL,
    ship_to_geography_key       BIGINT NOT NULL,
    order_date                  DATE NOT NULL,
    order_ts                    TIMESTAMP NOT NULL,
    ship_date                   DATE NOT NULL,
    ship_ts                     TIMESTAMP NOT NULL,
    delivery_date               DATE NOT NULL,
    delivery_ts                 TIMESTAMP NOT NULL,
    hour_of_day                 INT NOT NULL,
    day_of_week                 INT NOT NULL,
    quantity                    INT NOT NULL,
    unit_price_usd              DECIMAL(18,4) NOT NULL,
    list_price_usd              DECIMAL(18,4) NOT NULL,
    unit_cost_usd               DECIMAL(18,4) NOT NULL,
    discount_pct                DOUBLE NOT NULL,
    discount_amt_usd            DECIMAL(18,4) NOT NULL,
    line_subtotal_usd           DECIMAL(18,4) NOT NULL,
    tax_pct                     DOUBLE NOT NULL,
    tax_amt_usd                 DECIMAL(18,4) NOT NULL,
    shipping_cost_usd           DECIMAL(18,4) NOT NULL,
    handling_fee_usd            DECIMAL(18,4) NOT NULL,
    gross_revenue_usd           DECIMAL(18,4) NOT NULL,
    net_revenue_usd             DECIMAL(18,4) NOT NULL,
    total_amount_usd            DECIMAL(18,4) NOT NULL,
    cogs_usd                    DECIMAL(18,4) NOT NULL,
    gross_profit_usd            DECIMAL(18,4) NOT NULL,
    gross_margin_pct            DOUBLE NOT NULL,
    loyalty_points_earned       INT NOT NULL,
    loyalty_points_redeemed     INT NOT NULL,
    gift_card_amount_usd        DECIMAL(18,4) NOT NULL,
    store_credit_amount_usd     DECIMAL(18,4) NOT NULL,
    tip_amount_usd              DECIMAL(18,4) NOT NULL,
    refund_amount_usd           DECIMAL(18,4) NOT NULL,
    exchange_rate_to_usd        DOUBLE NOT NULL,
    sales_channel               STRING NOT NULL,
    payment_method              STRING NOT NULL,
    payment_card_type           STRING NOT NULL,
    currency_code               STRING NOT NULL,
    fulfillment_method          STRING NOT NULL,
    return_flag                 BOOLEAN NOT NULL,
    order_status                STRING NOT NULL,
    payment_status              STRING NOT NULL,
    fulfillment_status          STRING NOT NULL,
    return_reason_code          STRING NOT NULL,
    customer_segment            STRING NOT NULL,
    customer_country_code       STRING NOT NULL,
    customer_state_code         STRING NOT NULL,
    customer_city               STRING NOT NULL,
    customer_loyalty_tier       STRING NOT NULL,
    product_category_l1         STRING NOT NULL,
    product_category_l2         STRING NOT NULL,
    product_brand               STRING NOT NULL,
    product_color_family        STRING NOT NULL,
    product_department          STRING NOT NULL,
    store_region                STRING NOT NULL,
    store_format                STRING NOT NULL,
    store_country_code          STRING NOT NULL,
    store_banner                STRING NOT NULL,
    store_type                  STRING NOT NULL,
    device_type                 STRING NOT NULL,
    browser                     STRING NOT NULL,
    source_traffic_channel      STRING NOT NULL,
    marketing_campaign_code     STRING NOT NULL,
    sales_associate_team        STRING NOT NULL,
    fiscal_year_label           STRING NOT NULL
)
LOCATION '{{data_path}}/retail/fact_sales';

-- --------------------------------------------------------------------------
-- fact_inventory_snapshot (100,000,000 rows, 28 cols)
-- Daily store-and-product inventory snapshots.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.fact_inventory_snapshot (
    inventory_snapshot_id           BIGINT NOT NULL,
    snapshot_date                   DATE NOT NULL,
    snapshot_date_key               INT NOT NULL,
    store_key                       BIGINT NOT NULL,
    product_key                     BIGINT NOT NULL,
    on_hand_units                   INT NOT NULL,
    on_order_units                  INT NOT NULL,
    in_transit_units                INT NOT NULL,
    allocated_units                 INT NOT NULL,
    available_units                 INT NOT NULL,
    days_of_supply                  DOUBLE NOT NULL,
    reorder_point                   INT NOT NULL,
    max_stock_level                 INT NOT NULL,
    min_stock_level                 INT NOT NULL,
    last_received_date              DATE NOT NULL,
    last_received_qty               INT NOT NULL,
    last_sold_date                  DATE NOT NULL,
    days_since_last_sale            INT NOT NULL,
    valuation_unit_cost_usd         DECIMAL(18,4) NOT NULL,
    valuation_total_cost_usd        DECIMAL(18,4) NOT NULL,
    retail_value_usd                DECIMAL(18,4) NOT NULL,
    shrink_units_mtd                INT NOT NULL,
    shrink_value_usd_mtd            DECIMAL(18,4) NOT NULL,
    sell_through_pct_mtd            DOUBLE NOT NULL,
    abc_classification              STRING NOT NULL,
    stock_status                    STRING NOT NULL,
    store_region                    STRING NOT NULL,
    product_category_l1             STRING NOT NULL
)
LOCATION '{{data_path}}/retail/fact_inventory_snapshot';

-- --------------------------------------------------------------------------
-- fact_web_events (200,000,000 rows, 32 cols)
-- Clickstream event log. Session, customer, page, attribution, device, geo.
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.retail.fact_web_events (
    event_id                BIGINT NOT NULL,
    session_id              STRING NOT NULL,
    customer_key            BIGINT NOT NULL,
    event_ts                TIMESTAMP NOT NULL,
    event_date_key          INT NOT NULL,
    event_type              STRING NOT NULL,
    page_path               STRING NOT NULL,
    page_title              STRING NOT NULL,
    page_category           STRING NOT NULL,
    referrer                STRING NOT NULL,
    utm_source              STRING NOT NULL,
    utm_medium              STRING NOT NULL,
    utm_campaign            STRING NOT NULL,
    device_type             STRING NOT NULL,
    device_brand            STRING NOT NULL,
    browser                 STRING NOT NULL,
    browser_version         STRING NOT NULL,
    os                      STRING NOT NULL,
    os_version              STRING NOT NULL,
    country_code            STRING NOT NULL,
    region                  STRING NOT NULL,
    city                    STRING NOT NULL,
    ip_hash                 STRING NOT NULL,
    user_agent_hash         STRING NOT NULL,
    time_on_page_sec        INT NOT NULL,
    scroll_depth_pct        INT NOT NULL,
    viewport_width          INT NOT NULL,
    viewport_height         INT NOT NULL,
    is_bounce               BOOLEAN NOT NULL,
    conversion_value_usd    DECIMAL(18,4) NOT NULL,
    products_viewed_count   INT NOT NULL,
    search_query            STRING
)
LOCATION '{{data_path}}/retail/fact_web_events';

-- ==========================================================================
-- POPULATION
-- Every cell is row_number-derived through deterministic array lookups.
-- Two runs are bit-identical.
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Populate dim_date (7,305 rows). Dates 2010-01-01 .. 2029-12-31.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_date
SELECT
    CAST(year(d) * 10000 + month(d) * 100 + dayofmonth(d) AS INT)              AS date_key,
    d                                                                   AS full_date,
    CAST(year(d) AS INT)                                                AS year,
    CAST(quarter(d) AS INT)                                             AS quarter,
    concat('Q', CAST(quarter(d) AS STRING))                             AS quarter_name,
    concat(CAST(year(d) AS STRING), '-Q', CAST(quarter(d) AS STRING))   AS quarter_year,
    CAST(month(d) AS INT)                                               AS month,
    date_format(d, 'MMMM')                                              AS month_name,
    date_format(d, 'MMM')                                               AS month_short,
    concat(CAST(year(d) AS STRING), '-', lpad(CAST(month(d) AS STRING), 2, '0')) AS month_year,
    CAST(dayofmonth(d) AS INT)                                                 AS day_of_month,
    CAST(dayofyear(d) AS INT)                                           AS day_of_year,
    CAST(dayofweek(d) AS INT)                                           AS day_of_week,
    date_format(d, 'EEEE')                                              AS day_name,
    date_format(d, 'EEE')                                               AS day_short,
    CAST(weekofyear(d) AS INT)                                          AS week_of_year,
    CAST(CASE WHEN month(d) >= 4 THEN year(d) + 1 ELSE year(d) END AS INT) AS fiscal_year,
    CAST(CASE
        WHEN month(d) IN (4, 5, 6)  THEN 1
        WHEN month(d) IN (7, 8, 9)  THEN 2
        WHEN month(d) IN (10, 11, 12) THEN 3
        ELSE 4
    END AS INT)                                                         AS fiscal_quarter,
    dayofweek(d) IN (1, 7)                                              AS is_weekend,
    d = last_day(d)                                                     AS is_month_end,
    d = last_day(d) AND month(d) IN (3, 6, 9, 12)                       AS is_quarter_end,
    month(d) = 12 AND dayofmonth(d) = 31                                       AS is_year_end,
    (month(d) = 1  AND dayofmonth(d) = 1)
        OR (month(d) = 7  AND dayofmonth(d) = 4)
        OR (month(d) = 12 AND dayofmonth(d) = 25)
        OR (month(d) = 11 AND dayofmonth(d) BETWEEN 22 AND 28 AND dayofweek(d) = 5)
        OR (month(d) = 5  AND dayofmonth(d) BETWEEN 25 AND 31 AND dayofweek(d) = 2)
        OR (month(d) = 9  AND dayofmonth(d) BETWEEN 1  AND 7  AND dayofweek(d) = 2)
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
-- Populate dim_store (25,000 rows). 5 banners x 5 store types x 5 regions
-- cycle in lockstep so every closed-form COUNT-by-attribute is exact.
-- Native synthesis: df_generate_table streams the simple columns directly
-- from row index in chunks; the few compound strings (store_name,
-- address_line, manager_name) and the decimal cast are derived in SQL on
-- top of those chunks at vectorised speed.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_store
SELECT
    g.store_id,
    g.store_code,
    concat(g.banner, ' #', CAST(g.store_id AS STRING))                  AS store_name,
    g.store_type,
    g.banner,
    g.format,
    concat(CAST(g.store_id AS STRING), ' ', g.street_part)              AS address_line,
    g.city,
    g.state_code,
    g.state_name,
    g.postal_code,
    g.country_code,
    g.country_name,
    g.region,
    g.district,
    g.division,
    g.latitude,
    g.longitude,
    g.square_feet,
    g.opening_date,
    CAST(NULL AS DATE)                                                  AS closing_date,
    g.is_active,
    concat(g.first_name_part, ' ', g.last_name_part)                    AS manager_name,
    g.employee_count,
    CAST(g.annual_lease_int AS DECIMAL(18,2))                           AS annual_lease_usd,
    g.has_pharmacy,
    g.has_grocery,
    g.has_electronics,
    g.has_garden,
    g.has_cafe,
    g.target_segment
FROM df_generate_table(25000, '[
    {"type": "row_index",     "name": "store_id", "start": 1},
    {"type": "id_sequence",   "name": "store_code", "prefix": "STORE-", "pad": 6, "start": 1},
    {"type": "cyclic_lookup", "name": "store_type", "values": ["Hypermarket","Supermarket","Express","Online","Marketplace"]},
    {"type": "cyclic_lookup", "name": "banner",     "values": ["PacificMart","PacificFresh","PacificDirect","PacificClub","PacificExpress"]},
    {"type": "cyclic_lookup", "name": "format",     "values": ["Big Box","Neighborhood","Convenience","E-commerce","Wholesale"]},
    {"type": "cyclic_lookup", "name": "street_part","values": ["Main St","Oak Ave","Maple Dr","Cedar Ln","Elm St","Pine Rd","Birch Way","Walnut Ct","Spruce Pl","Willow Ln","Cherry St","Park Ave","Lake Dr","River Rd","Hill St","Forest Ave","Meadow Ln","Sunset Blvd","Highland Dr","Valley View"]},
    {"type": "cyclic_lookup", "name": "city",       "values": ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia","San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville","Fort Worth","Columbus","Charlotte","San Francisco","Indianapolis","Seattle","Denver","Washington","Boston","El Paso","Detroit","Nashville","Memphis","Portland","Oklahoma City","Las Vegas","Louisville","Baltimore","Milwaukee","Albuquerque","Tucson","Fresno","Sacramento","Mesa","Kansas City","Atlanta","Miami","Raleigh","Omaha","Long Beach","Virginia Beach","Oakland","Minneapolis","Tulsa","Arlington","Tampa","New Orleans","Wichita"]},
    {"type": "cyclic_lookup", "name": "state_code", "values": ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"]},
    {"type": "cyclic_lookup", "name": "state_name", "values": ["Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming"]},
    {"type": "padded_hash",   "name": "postal_code","multiplier": 1, "pad": 5, "modulo": 99999},
    {"type": "cyclic_lookup", "name": "country_code","values": ["US","CA","MX","UK","DE","FR","JP","AU","BR","IN"]},
    {"type": "cyclic_lookup", "name": "country_name","values": ["United States","Canada","Mexico","United Kingdom","Germany","France","Japan","Australia","Brazil","India"]},
    {"type": "cyclic_lookup", "name": "region",     "values": ["NA","EU","APAC","LATAM","MEA"]},
    {"type": "cyclic_lookup", "name": "district",   "values": ["District-01","District-02","District-03","District-04","District-05","District-06","District-07","District-08","District-09","District-10"]},
    {"type": "cyclic_lookup", "name": "division",   "values": ["North","South","East","West","Central"]},
    {"type": "double",        "name": "latitude",   "min": -90.0,  "modulo": 18000, "divisor": 100.0},
    {"type": "double",        "name": "longitude",  "min": -180.0, "modulo": 36000, "divisor": 100.0},
    {"type": "arithmetic",    "name": "square_feet","multiplier": 1, "modulo": 95000, "offset": 5000},
    {"type": "date",          "name": "opening_date","base": "1990-01-01", "multiplier": 11, "modulo": 12000},
    {"type": "boolean_cond",  "name": "is_active",  "modulo": 100, "ne": 0},
    {"type": "cyclic_lookup", "name": "first_name_part","values": ["Alice","Bob","Carol","David","Emma","Frank","Grace","Henry","Iris","Jack","Karen","Leo","Maya","Noah","Olivia","Peter","Quinn","Rachel","Steve","Tina","Uma","Victor","Wendy","Xander","Yara","Zoe","Aaron","Beth","Chris","Diana","Ethan","Fiona","George","Hannah","Ian","Julia","Kevin","Laura","Mike","Nina","Oscar","Paula","Quentin","Rose","Sam","Tara","Umar","Vera","Will","Xenia"]},
    {"type": "cyclic_lookup", "name": "last_name_part","values": ["Smith","Jones","Brown","Davis","Miller","Wilson","Moore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez"], "offset_multiplier": 7},
    {"type": "arithmetic",    "name": "employee_count","multiplier": 1, "modulo": 495, "offset": 5},
    {"type": "arithmetic",    "name": "annual_lease_int","multiplier": 1, "modulo": 950000, "offset": 50000},
    {"type": "boolean_cond",  "name": "has_pharmacy", "modulo": 3, "eq": 0},
    {"type": "boolean_cond",  "name": "has_grocery",  "modulo": 2, "eq": 0},
    {"type": "boolean_cond",  "name": "has_electronics","modulo": 4, "eq": 0},
    {"type": "boolean_cond",  "name": "has_garden",   "modulo": 5, "eq": 0},
    {"type": "boolean_cond",  "name": "has_cafe",     "modulo": 7, "eq": 0},
    {"type": "cyclic_lookup", "name": "target_segment","values": ["Mass","Premium","Value","Discount","Luxury"]}
]') g;

-- --------------------------------------------------------------------------
-- Populate dim_product (1,000,000 rows). 50 brands x 10 L1 x 20 L2 x 50 L3
-- cycle independently. Closed-form: 100K per L1, 50K per L2, 20K per L3.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_product
SELECT
    g.product_id,
    g.sku,
    g.upc_barcode,
    concat(g.brand, ' ', g.category_l1, ' #', CAST(g.product_id AS STRING)) AS product_name,
    concat('Premium ', g.category_l1, ' from ', g.brand)                AS short_description,
    g.brand,
    g.manufacturer,
    g.category_l1,
    g.category_l2,
    g.category_l3,
    g.department,
    g.subdepartment,
    g.color,
    g.color_family,
    g.size_label,
    CAST(g.weight_grams_int AS DOUBLE)                                  AS weight_grams,
    CAST(g.length_cm_int   AS DOUBLE)                                   AS length_cm,
    CAST(g.width_cm_int    AS DOUBLE)                                   AS width_cm,
    CAST(g.height_cm_int   AS DOUBLE)                                   AS height_cm,
    g.package_count,
    CAST(g.unit_cost_int AS DECIMAL(18,4))                              AS unit_cost_usd,
    CAST(g.unit_cost_int AS DECIMAL(18,4)) * CAST(1.5 AS DECIMAL(18,4)) AS list_price_usd,
    CAST(g.unit_cost_int AS DECIMAL(18,4)) * CAST(1.8 AS DECIMAL(18,4)) AS msrp_usd,
    g.default_margin_pct,
    g.supplier_id,
    concat('Supplier #', CAST(g.supplier_id AS STRING))                 AS supplier_name,
    g.country_of_origin,
    g.hs_tariff_code,
    g.energy_rating,
    g.package_type,
    g.units_per_case,
    g.lead_time_days,
    g.launch_date,
    CAST(NULL AS DATE)                                                  AS discontinued_date,
    g.is_active,
    g.is_seasonal,
    g.is_eco_certified,
    g.is_taxable,
    g.abc_class
FROM df_generate_table(1000000, '[
    {"type": "row_index",     "name": "product_id", "start": 1},
    {"type": "id_sequence",   "name": "sku", "prefix": "SKU-", "pad": 8, "start": 1},
    {"type": "padded_hash",   "name": "upc_barcode", "multiplier": 13, "pad": 14, "modulo": 100000000000000},
    {"type": "cyclic_lookup", "name": "brand",        "values": ["Acme","Globex","Initech","Umbrella","Wayne","Stark","Soylent","Cyberdyne","Tyrell","Wonka","Hooli","Vandelay","Gringotts","Massive","Oscorp","LexCorp","PymTech","Roxxon","Frobozz","Aperture","PaperStreet","DunderMifflin","Sterling","Pawnee","Cogswell","Spacely","Yoyodyne","Strickland","VaultTec","PizzaPlanet","Bluth","Costanza","Kramerica","SterlingCooper","Pendant","InGen","Tyrell","Weyland","OmniCorp","Massive Dynamic","Gekko","Initrode","Buy More","Rekall","Macguffin","Wonka","Spadina","Vance","Zorin","Zapf"]},
    {"type": "cyclic_lookup", "name": "manufacturer", "values": ["Manufacturer-01","Manufacturer-02","Manufacturer-03","Manufacturer-04","Manufacturer-05","Manufacturer-06","Manufacturer-07","Manufacturer-08","Manufacturer-09","Manufacturer-10","Manufacturer-11","Manufacturer-12","Manufacturer-13","Manufacturer-14","Manufacturer-15","Manufacturer-16","Manufacturer-17","Manufacturer-18","Manufacturer-19","Manufacturer-20","Manufacturer-21","Manufacturer-22","Manufacturer-23","Manufacturer-24","Manufacturer-25","Manufacturer-26","Manufacturer-27","Manufacturer-28","Manufacturer-29","Manufacturer-30","Manufacturer-31","Manufacturer-32","Manufacturer-33","Manufacturer-34","Manufacturer-35","Manufacturer-36","Manufacturer-37","Manufacturer-38","Manufacturer-39","Manufacturer-40","Manufacturer-41","Manufacturer-42","Manufacturer-43","Manufacturer-44","Manufacturer-45","Manufacturer-46","Manufacturer-47","Manufacturer-48","Manufacturer-49","Manufacturer-50"]},
    {"type": "cyclic_lookup", "name": "category_l1",  "values": ["Electronics","Apparel","Home","Grocery","Toys","Sports","Beauty","Books","Office","Pet"]},
    {"type": "cyclic_lookup", "name": "category_l2",  "values": ["Phones","Computers","Audio","TVs","Cameras","Tops","Bottoms","Shoes","Furniture","Decor","Beverages","Snacks","Frozen","Outdoor Toys","Board Games","Fitness","Outdoor Gear","Skincare","Haircare","Fiction"]},
    {"type": "cyclic_lookup", "name": "category_l3",  "values": ["Smartphones","Tablets","Laptops","Desktops","Headphones","Speakers","LED TVs","OLED TVs","DSLR","Mirrorless","T-Shirts","Polos","Jeans","Shorts","Sneakers","Boots","Sofas","Tables","Wall Art","Vases","Sodas","Juices","Chips","Cookies","Ice Cream","Frozen Meals","Action Figures","Building Blocks","Card Games","Strategy Games","Yoga","Cardio","Tents","Backpacks","Cleansers","Moisturizers","Shampoos","Conditioners","Novels","Biographies","Pens","Notebooks","Toys","Treats","Beds","Carriers","Vitamins","Skincare","Wraps","Cookware"]},
    {"type": "cyclic_lookup", "name": "department",   "values": ["Hardlines","Softlines","Consumables","Services","Specialty"]},
    {"type": "cyclic_lookup", "name": "subdepartment","values": ["SubA","SubB","SubC","SubD","SubE","SubF","SubG","SubH","SubI","SubJ"]},
    {"type": "cyclic_lookup", "name": "color",        "values": ["Red","Blue","Green","Black","White","Gray","Silver","Gold","Yellow","Orange"]},
    {"type": "cyclic_lookup", "name": "color_family", "values": ["Warm","Cool","Neutral","Metallic","Earth"]},
    {"type": "cyclic_lookup", "name": "size_label",   "values": ["XS","S","M","L","XL","XXL","One Size","N/A"]},
    {"type": "arithmetic",    "name": "weight_grams_int", "multiplier": 1, "modulo": 4950, "offset": 50},
    {"type": "arithmetic",    "name": "length_cm_int",    "multiplier": 1, "modulo": 95,   "offset": 5},
    {"type": "arithmetic",    "name": "width_cm_int",     "multiplier": 1, "modulo": 95,   "offset": 5},
    {"type": "arithmetic",    "name": "height_cm_int",    "multiplier": 1, "modulo": 99,   "offset": 1},
    {"type": "arithmetic",    "name": "package_count",    "multiplier": 1, "modulo": 12,   "offset": 1},
    {"type": "arithmetic",    "name": "unit_cost_int",    "multiplier": 1, "modulo": 1000, "offset": 5},
    {"type": "double",        "name": "default_margin_pct","min": 0.0, "modulo": 100, "divisor": 100.0},
    {"type": "arithmetic",    "name": "supplier_id",     "multiplier": 1, "modulo": 1000, "offset": 1},
    {"type": "cyclic_lookup", "name": "country_of_origin","values": ["US","CA","MX","UK","DE","FR","JP","AU","BR","IN"]},
    {"type": "padded_hash",   "name": "hs_tariff_code",  "multiplier": 7, "pad": 10, "modulo": 9999999999},
    {"type": "cyclic_lookup", "name": "energy_rating",   "values": ["A++","A+","A","B","C"]},
    {"type": "cyclic_lookup", "name": "package_type",    "values": ["Box","Bag","Bottle","Bulk","Wrapped"]},
    {"type": "arithmetic",    "name": "units_per_case",  "multiplier": 1, "modulo": 99, "offset": 1},
    {"type": "arithmetic",    "name": "lead_time_days",  "multiplier": 1, "modulo": 60, "offset": 1},
    {"type": "date",          "name": "launch_date",     "base": "2010-01-01", "multiplier": 7, "modulo": 5475},
    {"type": "boolean_cond",  "name": "is_active",       "modulo": 50, "ne": 0},
    {"type": "boolean_cond",  "name": "is_seasonal",     "modulo": 4,  "eq": 0},
    {"type": "boolean_cond",  "name": "is_eco_certified","modulo": 5,  "eq": 0},
    {"type": "boolean_cond",  "name": "is_taxable",      "modulo": 10, "ne": 0},
    {"type": "cyclic_lookup", "name": "abc_class",       "values": ["A","B","C","D"]}
]') g;

-- --------------------------------------------------------------------------
-- Populate dim_customer (5,000,000 rows). Closed-form: 1M per loyalty tier
-- (5), 1M per segment (5), 500K per occupation (10), 500K per country (10).
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_customer
SELECT
    g.customer_id,
    g.customer_code,
    g.salutation,
    g.first_name,
    g.middle_initial,
    g.last_name,
    concat(g.first_name, ' ', g.middle_initial, '. ', g.last_name)      AS full_name,
    concat('cust', CAST(g.customer_id AS STRING), '@', g.email_domain, '.com') AS email,
    concat('+1', g.phone_digits)                                        AS phone_e164,
    g.gender,
    g.birth_date,
    g.age_band,
    g.marital_status,
    g.education_level,
    g.occupation,
    concat(g.employer_short, ' Industries')                             AS employer_name,
    CAST(g.annual_income_int AS DECIMAL(18,2))                          AS annual_income_usd,
    g.income_band,
    g.household_size,
    g.number_of_children,
    concat(CAST(g.customer_id AS STRING), ' ', g.street_part)           AS address_line_1,
    CASE WHEN g.customer_id % 3 = 0
         THEN concat('Apt #', CAST(g.apt_num AS STRING))
         ELSE NULL
    END                                                                 AS address_line_2,
    g.city,
    g.state_code,
    g.state_name,
    g.postal_code,
    g.country_code,
    g.country_name,
    g.region,
    g.latitude,
    g.longitude,
    g.signup_date,
    g.signup_channel,
    g.preferred_contact_channel,
    g.marketing_opt_in,
    g.sms_opt_in,
    g.loyalty_tier,
    g.loyalty_points_balance,
    g.lifetime_orders,
    CAST(g.lifetime_revenue_int AS DECIMAL(18,2)) + CAST(0.99 AS DECIMAL(18,2)) AS lifetime_revenue_usd,
    g.last_purchase_date,
    g.churn_risk_score,
    g.segment
FROM df_generate_table(5000000, '[
    {"type": "row_index",     "name": "customer_id",   "start": 1},
    {"type": "id_sequence",   "name": "customer_code", "prefix": "CUST-", "pad": 8, "start": 1},
    {"type": "cyclic_lookup", "name": "salutation",    "values": ["Mr.","Mrs.","Ms.","Dr.","Prof."]},
    {"type": "cyclic_lookup", "name": "first_name",    "values": ["Alice","Bob","Carol","David","Emma","Frank","Grace","Henry","Iris","Jack","Karen","Leo","Maya","Noah","Olivia","Peter","Quinn","Rachel","Steve","Tina","Uma","Victor","Wendy","Xander","Yara","Zoe","Aaron","Beth","Chris","Diana","Ethan","Fiona","George","Hannah","Ian","Julia","Kevin","Laura","Mike","Nina","Oscar","Paula","Quentin","Rose","Sam","Tara","Umar","Vera","Will","Xenia"]},
    {"type": "cyclic_lookup", "name": "middle_initial","values": ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]},
    {"type": "cyclic_lookup", "name": "last_name",     "values": ["Smith","Jones","Brown","Davis","Miller","Wilson","Moore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez"], "offset_multiplier": 7},
    {"type": "cyclic_lookup", "name": "email_domain",  "values": ["example","retail","demo","test","sample"]},
    {"type": "padded_hash",   "name": "phone_digits",  "multiplier": 1, "pad": 10, "modulo": 10000000000},
    {"type": "cyclic_lookup", "name": "gender",        "values": ["M","F","X","U"]},
    {"type": "date",          "name": "birth_date",    "base": "1940-01-01", "multiplier": 13, "modulo": 25000},
    {"type": "cyclic_lookup", "name": "age_band",      "values": ["18-24","25-34","35-44","45-54","55+"]},
    {"type": "cyclic_lookup", "name": "marital_status","values": ["Single","Married","Divorced","Widowed","Partner"]},
    {"type": "cyclic_lookup", "name": "education_level","values": ["High School","Associate","Bachelor","Master","Doctorate"]},
    {"type": "cyclic_lookup", "name": "occupation",    "values": ["Engineer","Teacher","Manager","Sales","Healthcare","Retail","Service","Technician","Analyst","Professional"]},
    {"type": "cyclic_lookup", "name": "employer_short","values": ["Acme","Globex","Initech","Umbrella","Wayne","Stark","Soylent","Cyberdyne","Tyrell","Wonka"]},
    {"type": "arithmetic",    "name": "annual_income_int","multiplier": 1, "modulo": 475000, "offset": 25000},
    {"type": "cyclic_lookup", "name": "income_band",   "values": ["Under $25K","$25K-$50K","$50K-$100K","$100K-$200K","$200K+"]},
    {"type": "arithmetic",    "name": "household_size","multiplier": 1, "modulo": 6, "offset": 1},
    {"type": "arithmetic",    "name": "number_of_children","multiplier": 1, "modulo": 5, "offset": 0},
    {"type": "cyclic_lookup", "name": "street_part",   "values": ["Main St","Oak Ave","Maple Dr","Cedar Ln","Elm St","Pine Rd","Birch Way","Walnut Ct","Spruce Pl","Willow Ln","Cherry St","Park Ave","Lake Dr","River Rd","Hill St","Forest Ave","Meadow Ln","Sunset Blvd","Highland Dr","Valley View"]},
    {"type": "arithmetic",    "name": "apt_num",       "multiplier": 1, "modulo": 999, "offset": 1},
    {"type": "cyclic_lookup", "name": "city",          "values": ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia","San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville","Fort Worth","Columbus","Charlotte","San Francisco","Indianapolis","Seattle","Denver","Washington","Boston","El Paso","Detroit","Nashville","Memphis","Portland","Oklahoma City","Las Vegas","Louisville","Baltimore","Milwaukee","Albuquerque","Tucson","Fresno","Sacramento","Mesa","Kansas City","Atlanta","Miami","Raleigh","Omaha","Long Beach","Virginia Beach","Oakland","Minneapolis","Tulsa","Arlington","Tampa","New Orleans","Wichita"]},
    {"type": "cyclic_lookup", "name": "state_code",    "values": ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"]},
    {"type": "cyclic_lookup", "name": "state_name",    "values": ["Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming"]},
    {"type": "padded_hash",   "name": "postal_code",   "multiplier": 1, "pad": 5, "modulo": 99999},
    {"type": "cyclic_lookup", "name": "country_code",  "values": ["US","CA","MX","UK","DE","FR","JP","AU","BR","IN"]},
    {"type": "cyclic_lookup", "name": "country_name",  "values": ["United States","Canada","Mexico","United Kingdom","Germany","France","Japan","Australia","Brazil","India"]},
    {"type": "cyclic_lookup", "name": "region",        "values": ["NA","EU","APAC","LATAM","MEA"]},
    {"type": "double",        "name": "latitude",      "min": -90.0,  "modulo": 18000, "divisor": 100.0},
    {"type": "double",        "name": "longitude",     "min": -180.0, "modulo": 36000, "divisor": 100.0},
    {"type": "date",          "name": "signup_date",   "base": "2018-01-01", "multiplier": 7, "modulo": 2192},
    {"type": "cyclic_lookup", "name": "signup_channel","values": ["Web","Mobile","Store","Phone","Referral","Social","Email","Partner"]},
    {"type": "cyclic_lookup", "name": "preferred_contact_channel","values": ["Email","SMS","Phone","Mail"]},
    {"type": "boolean_cond",  "name": "marketing_opt_in","modulo": 4, "eq": 0},
    {"type": "boolean_cond",  "name": "sms_opt_in",    "modulo": 5, "eq": 0},
    {"type": "cyclic_lookup", "name": "loyalty_tier",  "values": ["Bronze","Silver","Gold","Platinum","Diamond"]},
    {"type": "arithmetic",    "name": "loyalty_points_balance","multiplier": 11, "modulo": 50000, "offset": 0},
    {"type": "arithmetic",    "name": "lifetime_orders","multiplier": 1, "modulo": 100, "offset": 0},
    {"type": "arithmetic",    "name": "lifetime_revenue_int","multiplier": 1, "modulo": 100000, "offset": 0},
    {"type": "date",          "name": "last_purchase_date","base": "2024-01-01", "multiplier": 3, "modulo": 365},
    {"type": "double",        "name": "churn_risk_score","min": 0.0, "modulo": 100, "divisor": 100.0},
    {"type": "cyclic_lookup", "name": "segment",       "values": ["New","Active","At Risk","VIP","Inactive"]}
]') g;

-- --------------------------------------------------------------------------
-- Populate fact_sales (200,000,000 rows). Order grain.
-- 75 columns including 15 denormalized customer/product/store columns so
-- Power BI Import slicers do not require joins.
-- Closed-form distributions: every cycling array divides 200M evenly so
-- each value gets an exact row count.
-- FK integrity is preserved via the same `(rn * M) % N + 1` formulas as
-- the dim primary keys: customer_key in [1, 5000000], product_key in
-- [1, 1000000], store_key in [1, 25000].
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.fact_sales
SELECT
    g.sale_id,
    CAST((g.sale_id - 1) / 4 AS BIGINT) + 1                             AS order_id,
    CAST((g.sale_id - 1) % 4 AS INT) + 1                                AS line_number,
    concat('RCPT-', lpad(CAST(CAST((g.sale_id - 1) / 4 AS BIGINT) + 1 AS STRING), 10, '0')) AS receipt_number,
    g.transaction_uuid,
    CAST(year(g.order_date) * 10000 + month(g.order_date) * 100 + dayofmonth(g.order_date) AS INT) AS date_key,
    g.customer_key,
    g.product_key,
    g.store_key,
    g.employee_key,
    g.promotion_key,
    g.ship_to_geography_key,
    g.order_date,
    g.order_ts,
    g.order_date + 1                                                    AS ship_date,
    g.order_ts + INTERVAL '1' HOUR                                      AS ship_ts,
    g.order_date + 5                                                    AS delivery_date,
    g.order_ts + INTERVAL '2' HOUR                                      AS delivery_ts,
    g.hour_of_day,
    CAST(dayofweek(g.order_date) AS INT)                                AS day_of_week,
    g.quantity,
    CAST(g.unit_price_int AS DECIMAL(18,4))                             AS unit_price_usd,
    CAST(g.unit_price_int AS DECIMAL(18,4)) * CAST(1.2 AS DECIMAL(18,4)) AS list_price_usd,
    CAST(g.unit_price_int AS DECIMAL(18,4)) * CAST(0.6 AS DECIMAL(18,4)) AS unit_cost_usd,
    g.discount_pct,
    CAST(g.discount_amt_int AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4))     AS discount_amt_usd,
    CAST(g.line_subtotal_int AS DECIMAL(18,4))                          AS line_subtotal_usd,
    CAST(0.08 AS DOUBLE)                                                AS tax_pct,
    CAST(g.line_subtotal_int AS DECIMAL(18,4)) * CAST(0.08 AS DECIMAL(18,4))   AS tax_amt_usd,
    CAST(g.shipping_cost_int AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4))     AS shipping_cost_usd,
    CAST(g.handling_fee_int AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4))      AS handling_fee_usd,
    CAST(g.line_subtotal_int AS DECIMAL(18,4))                          AS gross_revenue_usd,
    CAST(g.line_subtotal_int AS DECIMAL(18,4))
        - (CAST(g.discount_amt_int AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4))) AS net_revenue_usd,
    CAST(g.line_subtotal_int AS DECIMAL(18,4))
        + CAST(g.line_subtotal_int AS DECIMAL(18,4)) * CAST(0.08 AS DECIMAL(18,4))
        + (CAST(g.shipping_cost_int AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)))
        + (CAST(g.handling_fee_int AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4))) AS total_amount_usd,
    CAST(g.line_subtotal_int AS DECIMAL(18,4)) * CAST(0.6 AS DECIMAL(18,4))    AS cogs_usd,
    CAST(g.line_subtotal_int AS DECIMAL(18,4)) * CAST(0.4 AS DECIMAL(18,4))    AS gross_profit_usd,
    g.gross_margin_pct,
    g.loyalty_points_earned,
    g.loyalty_points_redeemed,
    CASE WHEN g.sale_id % 20 = 0 THEN CAST(25 AS DECIMAL(18,4)) ELSE CAST(0 AS DECIMAL(18,4)) END AS gift_card_amount_usd,
    CASE WHEN g.sale_id % 30 = 0 THEN CAST(10 AS DECIMAL(18,4)) ELSE CAST(0 AS DECIMAL(18,4)) END AS store_credit_amount_usd,
    CAST(g.tip_int AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4))       AS tip_amount_usd,
    CASE WHEN g.sale_id % 25 = 0
        THEN CAST(g.line_subtotal_int AS DECIMAL(18,4)) / CAST(2 AS DECIMAL(18,4))
        ELSE CAST(0 AS DECIMAL(18,4))
    END                                                                 AS refund_amount_usd,
    CAST(1.0 AS DOUBLE)                                                 AS exchange_rate_to_usd,
    g.sales_channel,
    g.payment_method,
    g.payment_card_type,
    g.currency_code,
    g.fulfillment_method,
    g.return_flag,
    g.order_status,
    g.payment_status,
    g.fulfillment_status,
    CASE WHEN g.sale_id % 20 = 0 THEN g.return_reason_lookup ELSE 'NONE' END AS return_reason_code,
    g.customer_segment,
    g.customer_country_code,
    g.customer_state_code,
    g.customer_city,
    g.customer_loyalty_tier,
    g.product_category_l1,
    g.product_category_l2,
    g.product_brand,
    g.product_color_family,
    g.product_department,
    g.store_region,
    g.store_format,
    g.store_country_code,
    g.store_banner,
    g.store_type,
    g.device_type,
    g.browser,
    g.source_traffic_channel,
    g.marketing_campaign_code,
    g.sales_associate_team,
    concat('FY', CAST(year(g.order_date)
        + CASE WHEN month(g.order_date) >= 4 THEN 1 ELSE 0 END AS STRING)) AS fiscal_year_label
FROM df_generate_table(200000000, '[
    {"type": "row_index",     "name": "sale_id", "start": 1},
    {"type": "id_sequence",   "name": "transaction_uuid", "prefix": "", "pad": 32, "start": 1},
    {"type": "arithmetic",    "name": "customer_key",          "multiplier": 17, "modulo": 5000000, "offset": 1},
    {"type": "arithmetic",    "name": "product_key",           "multiplier": 13, "modulo": 1000000, "offset": 1},
    {"type": "arithmetic",    "name": "store_key",             "multiplier": 7,  "modulo": 25000,   "offset": 1},
    {"type": "arithmetic",    "name": "employee_key",          "multiplier": 11, "modulo": 50000,   "offset": 1},
    {"type": "arithmetic",    "name": "promotion_key",         "multiplier": 19, "modulo": 5000,    "offset": 1},
    {"type": "arithmetic",    "name": "ship_to_geography_key", "multiplier": 23, "modulo": 100000,  "offset": 1},
    {"type": "date",          "name": "order_date",  "base": "2020-01-01", "multiplier": 1, "modulo": 1825},
    {"type": "timestamp",     "name": "order_ts",    "base_micros": 1704067200000000, "step_micros": 1000000},
    {"type": "arithmetic",    "name": "hour_of_day", "multiplier": 1, "modulo": 24, "offset": 0},
    {"type": "arithmetic",    "name": "quantity",    "multiplier": 1, "modulo": 10, "offset": 1},
    {"type": "arithmetic",    "name": "unit_price_int",     "multiplier": 1, "modulo": 1000, "offset": 5},
    {"type": "double",        "name": "discount_pct",       "min": 0.0, "modulo": 25, "divisor": 100.0},
    {"type": "arithmetic",    "name": "discount_amt_int",   "multiplier": 1, "modulo": 24999, "offset": 0},
    {"type": "arithmetic",    "name": "line_subtotal_int",  "multiplier": 11, "modulo": 9999, "offset": 5},
    {"type": "arithmetic",    "name": "shipping_cost_int",  "multiplier": 1, "modulo": 30, "offset": 0},
    {"type": "arithmetic",    "name": "handling_fee_int",   "multiplier": 1, "modulo": 5, "offset": 0},
    {"type": "double",        "name": "gross_margin_pct",   "min": 0.0, "modulo": 100, "divisor": 100.0},
    {"type": "arithmetic",    "name": "loyalty_points_earned",  "multiplier": 10, "modulo": 100, "offset": 10},
    {"type": "arithmetic",    "name": "loyalty_points_redeemed","multiplier": 1,  "modulo": 50,  "offset": 0},
    {"type": "arithmetic",    "name": "tip_int",            "multiplier": 5, "modulo": 500, "offset": 0},
    {"type": "cyclic_lookup", "name": "sales_channel",     "values": ["In-Store","Online","Mobile App","Phone","Marketplace"]},
    {"type": "cyclic_lookup", "name": "payment_method",    "values": ["Credit Card","Debit Card","Cash","Mobile Wallet","Gift Card","Bank Transfer","BNPL","Crypto"]},
    {"type": "cyclic_lookup", "name": "payment_card_type", "values": ["Visa","Mastercard","Amex","Discover","Other"]},
    {"type": "cyclic_lookup", "name": "currency_code",     "values": ["USD","EUR","GBP","CAD","JPY"]},
    {"type": "cyclic_lookup", "name": "fulfillment_method","values": ["Ship","Pickup","Delivery","Locker"]},
    {"type": "boolean_cond",  "name": "return_flag",       "modulo": 20, "eq": 0},
    {"type": "cyclic_lookup", "name": "order_status",      "values": ["Pending","Confirmed","Shipped","Delivered"]},
    {"type": "cyclic_lookup", "name": "payment_status",    "values": ["Pending","Authorized","Captured","Settled"]},
    {"type": "cyclic_lookup", "name": "fulfillment_status","values": ["Pending","Picking","InTransit","Delivered"]},
    {"type": "cyclic_lookup", "name": "return_reason_lookup","values": ["Defective","Wrong Item","Not As Described","Damaged","Late","Changed Mind","Better Price","Quality"]},
    {"type": "cyclic_lookup", "name": "customer_segment",      "values": ["New","Active","At Risk","VIP","Inactive"]},
    {"type": "cyclic_lookup", "name": "customer_country_code", "values": ["US","CA","MX","UK","DE","FR","JP","AU","BR","IN"]},
    {"type": "cyclic_lookup", "name": "customer_state_code",   "values": ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"]},
    {"type": "cyclic_lookup", "name": "customer_city",         "values": ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia","San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville","Fort Worth","Columbus","Charlotte","San Francisco","Indianapolis","Seattle","Denver","Washington","Boston","El Paso","Detroit","Nashville","Memphis","Portland","Oklahoma City","Las Vegas","Louisville","Baltimore","Milwaukee","Albuquerque","Tucson","Fresno","Sacramento","Mesa","Kansas City","Atlanta","Miami","Raleigh","Omaha","Long Beach","Virginia Beach","Oakland","Minneapolis","Tulsa","Arlington","Tampa","New Orleans","Wichita"]},
    {"type": "cyclic_lookup", "name": "customer_loyalty_tier", "values": ["Bronze","Silver","Gold","Platinum","Diamond"]},
    {"type": "cyclic_lookup", "name": "product_category_l1",   "values": ["Electronics","Apparel","Home","Grocery","Toys","Sports","Beauty","Books","Office","Pet"]},
    {"type": "cyclic_lookup", "name": "product_category_l2",   "values": ["Phones","Computers","Audio","TVs","Cameras","Tops","Bottoms","Shoes","Furniture","Decor","Beverages","Snacks","Frozen","Outdoor Toys","Board Games","Fitness","Outdoor Gear","Skincare","Haircare","Fiction"]},
    {"type": "cyclic_lookup", "name": "product_brand",         "values": ["Acme","Globex","Initech","Umbrella","Wayne","Stark","Soylent","Cyberdyne","Tyrell","Wonka","Hooli","Vandelay","Gringotts","Massive","Oscorp","LexCorp","PymTech","Roxxon","Frobozz","Aperture","PaperStreet","DunderMifflin","Sterling","Pawnee","Cogswell","Spacely","Yoyodyne","Strickland","VaultTec","PizzaPlanet","Bluth","Costanza","Kramerica","SterlingCooper","Pendant","InGen","Tyrell","Weyland","OmniCorp","Massive Dynamic","Gekko","Initrode","Buy More","Rekall","Macguffin","Wonka","Spadina","Vance","Zorin","Zapf"]},
    {"type": "cyclic_lookup", "name": "product_color_family",  "values": ["Warm","Cool","Neutral","Metallic","Earth"]},
    {"type": "cyclic_lookup", "name": "product_department",    "values": ["Hardlines","Softlines","Consumables","Services","Specialty"]},
    {"type": "cyclic_lookup", "name": "store_region",          "values": ["NA","EU","APAC","LATAM","MEA"]},
    {"type": "cyclic_lookup", "name": "store_format",          "values": ["Big Box","Neighborhood","Convenience","E-commerce","Wholesale"]},
    {"type": "cyclic_lookup", "name": "store_country_code",    "values": ["US","CA","MX","UK","DE","FR","JP","AU","BR","IN"]},
    {"type": "cyclic_lookup", "name": "store_banner",          "values": ["PacificMart","PacificFresh","PacificDirect","PacificClub","PacificExpress"]},
    {"type": "cyclic_lookup", "name": "store_type",            "values": ["Hypermarket","Supermarket","Express","Online","Marketplace"]},
    {"type": "cyclic_lookup", "name": "device_type",           "values": ["Desktop","Mobile","Tablet","Smart TV","Wearable"]},
    {"type": "cyclic_lookup", "name": "browser",               "values": ["Chrome","Firefox","Safari","Edge","Opera","Samsung","Brave","UC"]},
    {"type": "cyclic_lookup", "name": "source_traffic_channel","values": ["Organic","Paid Search","Social","Email","Direct","Referral","Display","Affiliate"]},
    {"type": "cyclic_lookup", "name": "marketing_campaign_code","values": ["CAMP-001","CAMP-002","CAMP-003","CAMP-004","CAMP-005","CAMP-006","CAMP-007","CAMP-008","CAMP-009","CAMP-010"]},
    {"type": "cyclic_lookup", "name": "sales_associate_team", "values": ["Team-A","Team-B","Team-C","Team-D","Team-E"]}
]') g;

-- --------------------------------------------------------------------------
-- Populate fact_inventory_snapshot (100,000,000 rows). Daily store and
-- product snapshots over a 365 day window starting 2024-01-01.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.fact_inventory_snapshot
SELECT
    g.inventory_snapshot_id,
    g.snapshot_date,
    CAST(year(g.snapshot_date) * 10000 + month(g.snapshot_date) * 100 + dayofmonth(g.snapshot_date) AS INT) AS snapshot_date_key,
    g.store_key,
    g.product_key,
    g.on_hand_units,
    g.on_order_units,
    g.in_transit_units,
    g.allocated_units,
    g.on_hand_units - g.allocated_units                                 AS available_units,
    CAST(g.days_of_supply_int AS DOUBLE) / 10.0                         AS days_of_supply,
    g.reorder_point,
    g.max_stock_level,
    g.min_stock_level,
    g.last_received_date,
    g.last_received_qty,
    g.last_sold_date,
    g.days_since_last_sale,
    CAST(g.valuation_unit_cost_int AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)) AS valuation_unit_cost_usd,
    CAST(g.valuation_total_cost_int AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)) AS valuation_total_cost_usd,
    CAST(g.retail_value_int AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4))       AS retail_value_usd,
    g.shrink_units_mtd,
    CAST(g.shrink_value_int AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)) AS shrink_value_usd_mtd,
    g.sell_through_pct_mtd,
    g.abc_classification,
    g.stock_status,
    g.store_region,
    g.product_category_l1
FROM df_generate_table(100000000, '[
    {"type": "row_index",     "name": "inventory_snapshot_id", "start": 1},
    {"type": "date",          "name": "snapshot_date",      "base": "2024-01-01", "multiplier": 1, "modulo": 365},
    {"type": "arithmetic",    "name": "store_key",           "multiplier": 7,  "modulo": 25000,   "offset": 1},
    {"type": "arithmetic",    "name": "product_key",         "multiplier": 13, "modulo": 1000000, "offset": 1},
    {"type": "arithmetic",    "name": "on_hand_units",       "multiplier": 1, "modulo": 1000, "offset": 0},
    {"type": "arithmetic",    "name": "on_order_units",      "multiplier": 1, "modulo": 500,  "offset": 0},
    {"type": "arithmetic",    "name": "in_transit_units",    "multiplier": 1, "modulo": 200,  "offset": 0},
    {"type": "arithmetic",    "name": "allocated_units",     "multiplier": 1, "modulo": 100,  "offset": 0},
    {"type": "arithmetic",    "name": "days_of_supply_int",  "multiplier": 1, "modulo": 1000, "offset": 0},
    {"type": "arithmetic",    "name": "reorder_point",       "multiplier": 1, "modulo": 200,  "offset": 50},
    {"type": "arithmetic",    "name": "max_stock_level",     "multiplier": 1, "modulo": 1500, "offset": 500},
    {"type": "arithmetic",    "name": "min_stock_level",     "multiplier": 1, "modulo": 50,   "offset": 10},
    {"type": "date",          "name": "last_received_date",  "base": "2023-12-01", "multiplier": 1, "modulo": 395},
    {"type": "arithmetic",    "name": "last_received_qty",   "multiplier": 1, "modulo": 500,  "offset": 0},
    {"type": "date",          "name": "last_sold_date",      "base": "2023-11-01", "multiplier": 1, "modulo": 425},
    {"type": "arithmetic",    "name": "days_since_last_sale","multiplier": 1, "modulo": 60,   "offset": 0},
    {"type": "arithmetic",    "name": "valuation_unit_cost_int", "multiplier": 1, "modulo": 1000, "offset": 0},
    {"type": "arithmetic",    "name": "valuation_total_cost_int","multiplier": 999, "modulo": 999000, "offset": 0},
    {"type": "arithmetic",    "name": "retail_value_int",    "multiplier": 1499, "modulo": 14990000, "offset": 0},
    {"type": "arithmetic",    "name": "shrink_units_mtd",    "multiplier": 1, "modulo": 10, "offset": 0},
    {"type": "arithmetic",    "name": "shrink_value_int",    "multiplier": 1, "modulo": 9990, "offset": 0},
    {"type": "double",        "name": "sell_through_pct_mtd","min": 0.0, "modulo": 100, "divisor": 100.0},
    {"type": "cyclic_lookup", "name": "abc_classification",  "values": ["A","B","C","D"]},
    {"type": "cyclic_lookup", "name": "stock_status",        "values": ["In Stock","Low Stock","Out of Stock","Overstock","Discontinued"]},
    {"type": "cyclic_lookup", "name": "store_region",        "values": ["NA","EU","APAC","LATAM","MEA"]},
    {"type": "cyclic_lookup", "name": "product_category_l1", "values": ["Electronics","Apparel","Home","Grocery","Toys","Sports","Beauty","Books","Office","Pet"]}
]') g;

-- --------------------------------------------------------------------------
-- Populate fact_web_events (200,000,000 rows). Clickstream over 365 days
-- starting 2024-01-01. 5 events per session = 40M sessions.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.fact_web_events
SELECT
    g.event_id,
    concat('SESS-', lpad(CAST(g.event_id / 5 + 1 AS STRING), 10, '0'))  AS session_id,
    g.customer_key,
    g.event_ts,
    CAST(year(g.event_date) * 10000 + month(g.event_date) * 100 + dayofmonth(g.event_date) AS INT) AS event_date_key,
    g.event_type,
    concat('/', g.page_section, '/', CAST(g.page_id AS STRING))         AS page_path,
    concat('Page ', CAST(g.event_id AS STRING))                         AS page_title,
    g.page_category,
    g.referrer,
    g.utm_source,
    g.utm_medium,
    g.utm_campaign,
    g.device_type,
    g.device_brand,
    g.browser,
    concat(CAST(g.browser_major AS STRING), '.0')                       AS browser_version,
    g.os,
    concat(CAST(g.os_major AS STRING), '.', CAST(g.os_minor AS STRING)) AS os_version,
    g.country_code,
    g.region,
    g.city,
    g.ip_hash,
    g.user_agent_hash,
    g.time_on_page_sec,
    g.scroll_depth_pct,
    g.viewport_width,
    g.viewport_height,
    g.is_bounce,
    CASE WHEN g.event_id % 50 = 0
        THEN CAST(g.conversion_int AS DECIMAL(18,4))
        ELSE CAST(0 AS DECIMAL(18,4))
    END                                                                 AS conversion_value_usd,
    g.products_viewed_count,
    CASE WHEN g.event_id % 10 = 0
        THEN concat('q-', CAST(g.search_q_id AS STRING))
        ELSE NULL
    END                                                                 AS search_query
FROM df_generate_table(200000000, '[
    {"type": "row_index",     "name": "event_id", "start": 1},
    {"type": "arithmetic",    "name": "customer_key",  "multiplier": 17, "modulo": 5000000, "offset": 1},
    {"type": "timestamp",     "name": "event_ts",      "base_micros": 1704067200000000, "step_micros": 1000000},
    {"type": "date",          "name": "event_date",    "base": "2024-01-01", "multiplier": 1, "modulo": 365},
    {"type": "cyclic_lookup", "name": "event_type",    "values": ["page_view","add_to_cart","remove_from_cart","checkout_start","checkout_complete","search","product_view","click_recommendation","share","wishlist_add"]},
    {"type": "cyclic_lookup", "name": "page_section",  "values": ["home","category","product","search","cart","checkout","account","help","blog","offers"]},
    {"type": "arithmetic",    "name": "page_id",       "multiplier": 1, "modulo": 10000, "offset": 0},
    {"type": "cyclic_lookup", "name": "page_category", "values": ["Home","Category","Product","Search","Cart","Account","Help","Editorial"]},
    {"type": "cyclic_lookup", "name": "referrer",      "values": ["https://google.com","https://facebook.com","https://twitter.com","https://instagram.com","https://reddit.com","https://youtube.com","https://linkedin.com","https://tiktok.com","direct","email"]},
    {"type": "cyclic_lookup", "name": "utm_source",    "values": ["google","facebook","twitter","instagram","reddit","youtube","linkedin","tiktok"]},
    {"type": "cyclic_lookup", "name": "utm_medium",    "values": ["cpc","organic","social","email","referral"]},
    {"type": "cyclic_lookup", "name": "utm_campaign",  "values": ["CAMP-001","CAMP-002","CAMP-003","CAMP-004","CAMP-005","CAMP-006","CAMP-007","CAMP-008","CAMP-009","CAMP-010"]},
    {"type": "cyclic_lookup", "name": "device_type",   "values": ["Desktop","Mobile","Tablet","Smart TV","Wearable"]},
    {"type": "cyclic_lookup", "name": "device_brand",  "values": ["Apple","Samsung","Google","Microsoft","Lenovo","Dell","HP","Asus","Sony","Other"]},
    {"type": "cyclic_lookup", "name": "browser",       "values": ["Chrome","Firefox","Safari","Edge","Opera","Samsung","Brave","UC"]},
    {"type": "arithmetic",    "name": "browser_major", "multiplier": 1, "modulo": 40, "offset": 80},
    {"type": "cyclic_lookup", "name": "os",            "values": ["Windows","macOS","iOS","Android","Linux"]},
    {"type": "arithmetic",    "name": "os_major",      "multiplier": 1, "modulo": 10, "offset": 10},
    {"type": "arithmetic",    "name": "os_minor",      "multiplier": 1, "modulo": 10, "offset": 0},
    {"type": "cyclic_lookup", "name": "country_code",  "values": ["US","CA","MX","UK","DE","FR","JP","AU","BR","IN"]},
    {"type": "cyclic_lookup", "name": "region",        "values": ["NA","EU","APAC","LATAM","MEA"]},
    {"type": "cyclic_lookup", "name": "city",          "values": ["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia","San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville","Fort Worth","Columbus","Charlotte","San Francisco","Indianapolis","Seattle","Denver","Washington","Boston","El Paso","Detroit","Nashville","Memphis","Portland","Oklahoma City","Las Vegas","Louisville","Baltimore","Milwaukee","Albuquerque","Tucson","Fresno","Sacramento","Mesa","Kansas City","Atlanta","Miami","Raleigh","Omaha","Long Beach","Virginia Beach","Oakland","Minneapolis","Tulsa","Arlington","Tampa","New Orleans","Wichita"]},
    {"type": "padded_hash",   "name": "ip_hash",         "multiplier": 31, "pad": 16, "modulo": 1000000000000},
    {"type": "padded_hash",   "name": "user_agent_hash", "multiplier": 37, "pad": 16, "modulo": 1000000000000},
    {"type": "arithmetic",    "name": "time_on_page_sec","multiplier": 1, "modulo": 3600, "offset": 0},
    {"type": "arithmetic",    "name": "scroll_depth_pct","multiplier": 1, "modulo": 100,  "offset": 0},
    {"type": "arithmetic",    "name": "viewport_width", "multiplier": 1, "modulo": 1280, "offset": 768},
    {"type": "arithmetic",    "name": "viewport_height","multiplier": 1, "modulo": 800,  "offset": 600},
    {"type": "boolean_cond",  "name": "is_bounce",      "modulo": 10, "eq": 0},
    {"type": "arithmetic",    "name": "conversion_int", "multiplier": 1, "modulo": 1000, "offset": 0},
    {"type": "arithmetic",    "name": "products_viewed_count","multiplier": 1, "modulo": 20, "offset": 0},
    {"type": "arithmetic",    "name": "search_q_id",    "multiplier": 1, "modulo": 1000, "offset": 0}
]') g;

-- ==========================================================================
-- Schema Detection & Permissions
-- ==========================================================================

DETECT SCHEMA FOR TABLE {{zone_name}}.retail.dim_date;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.dim_store;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.dim_product;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.dim_customer;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.fact_sales;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.fact_inventory_snapshot;
DETECT SCHEMA FOR TABLE {{zone_name}}.retail.fact_web_events;

GRANT ADMIN ON TABLE {{zone_name}}.retail.dim_date                  TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.retail.dim_store                 TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.retail.dim_product               TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.retail.dim_customer              TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.retail.fact_sales                TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.retail.fact_inventory_snapshot   TO USER {{current_user}};
GRANT ADMIN ON TABLE {{zone_name}}.retail.fact_web_events           TO USER {{current_user}};
