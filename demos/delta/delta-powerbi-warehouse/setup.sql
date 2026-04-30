-- ==========================================================================
-- Demo: Pacific Retail Group: Power BI Star Warehouse Benchmark
-- ==========================================================================
-- Realistic star schema sized for serious Power BI workloads. Four wide
-- dimensions (date, store, product, customer) and three wide fact tables
-- (sales, inventory snapshots, web events) totalling ~506 million rows
-- and ~265 columns. Every value is row_number-derived through array
-- lookups so two runs are bit-identical and any drift is real.
--
-- File sizing inherits the workspace default (delta.targetFileSize = 256 MB,
-- the value Databricks autotune targets for tables under 2.56 TB). The
-- writer rotates files at that size by direct measurement of bytes-on-disk;
-- no per-table override is needed.
--
-- Setup time on local SSD is measured in hours, not minutes. This is
-- intentional: the demo exists to drive ODBC perf measurement against
-- a workload that looks like a production Power BI warehouse.
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
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_store
SELECT
    b.v                                                                 AS store_id,
    concat('STORE-', lpad(CAST(b.v AS STRING), 6, '0'))                 AS store_code,
    concat(
        element_at(array('PacificMart','PacificFresh','PacificDirect','PacificClub','PacificExpress'),
                   1 + CAST((b.v - 1) % 5 AS INT)),
        ' #', CAST(b.v AS STRING)
    )                                                                   AS store_name,
    element_at(array('Hypermarket','Supermarket','Express','Online','Marketplace'),
               1 + CAST((b.v - 1) % 5 AS INT))                          AS store_type,
    element_at(array('PacificMart','PacificFresh','PacificDirect','PacificClub','PacificExpress'),
               1 + CAST((b.v - 1) % 5 AS INT))                          AS banner,
    element_at(array('Big Box','Neighborhood','Convenience','E-commerce','Wholesale'),
               1 + CAST((b.v - 1) % 5 AS INT))                          AS format,
    concat(CAST(b.v AS STRING), ' ',
        element_at(array('Main St','Oak Ave','Maple Dr','Cedar Ln','Elm St','Pine Rd','Birch Way','Walnut Ct','Spruce Pl','Willow Ln','Cherry St','Park Ave','Lake Dr','River Rd','Hill St','Forest Ave','Meadow Ln','Sunset Blvd','Highland Dr','Valley View'),
            1 + CAST((b.v - 1) % 20 AS INT)))                           AS address_line,
    element_at(array('New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','Fort Worth','Columbus','Charlotte','San Francisco','Indianapolis','Seattle','Denver','Washington','Boston','El Paso','Detroit','Nashville','Memphis','Portland','Oklahoma City','Las Vegas','Louisville','Baltimore','Milwaukee','Albuquerque','Tucson','Fresno','Sacramento','Mesa','Kansas City','Atlanta','Miami','Raleigh','Omaha','Long Beach','Virginia Beach','Oakland','Minneapolis','Tulsa','Arlington','Tampa','New Orleans','Wichita'),
        1 + CAST((b.v - 1) % 50 AS INT))                                AS city,
    element_at(array('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'),
        1 + CAST((b.v - 1) % 50 AS INT))                                AS state_code,
    element_at(array('Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida','Georgia','Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine','Maryland','Massachusetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire','New Jersey','New Mexico','New York','North Carolina','North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont','Virginia','Washington','West Virginia','Wisconsin','Wyoming'),
        1 + CAST((b.v - 1) % 50 AS INT))                                AS state_name,
    lpad(CAST(b.v % 99999 AS STRING), 5, '0')                           AS postal_code,
    element_at(array('US','CA','MX','UK','DE','FR','JP','AU','BR','IN'),
        1 + CAST((b.v - 1) % 10 AS INT))                                AS country_code,
    element_at(array('United States','Canada','Mexico','United Kingdom','Germany','France','Japan','Australia','Brazil','India'),
        1 + CAST((b.v - 1) % 10 AS INT))                                AS country_name,
    element_at(array('NA','EU','APAC','LATAM','MEA'),
        1 + CAST((b.v - 1) % 5 AS INT))                                 AS region,
    element_at(array('District-01','District-02','District-03','District-04','District-05','District-06','District-07','District-08','District-09','District-10'),
        1 + CAST((b.v - 1) % 10 AS INT))                                AS district,
    element_at(array('North','South','East','West','Central'),
        1 + CAST((b.v - 1) % 5 AS INT))                                 AS division,
    -90.0 + CAST(b.v % 18000 AS DOUBLE) / 100.0                         AS latitude,
    -180.0 + CAST(b.v % 36000 AS DOUBLE) / 100.0                        AS longitude,
    CAST(5000 + (b.v % 95000) AS INT)                                   AS square_feet,
    DATE '1990-01-01' + CAST((b.v * 11) % 12000 AS INT)                 AS opening_date,
    NULL                                                                AS closing_date,
    b.v % 100 != 0                                                      AS is_active,
    concat(
        element_at(array('Alice','Bob','Carol','David','Emma','Frank','Grace','Henry','Iris','Jack','Karen','Leo','Maya','Noah','Olivia','Peter','Quinn','Rachel','Steve','Tina','Uma','Victor','Wendy','Xander','Yara','Zoe','Aaron','Beth','Chris','Diana','Ethan','Fiona','George','Hannah','Ian','Julia','Kevin','Laura','Mike','Nina','Oscar','Paula','Quentin','Rose','Sam','Tara','Umar','Vera','Will','Xenia'),
            1 + CAST((b.v - 1) % 50 AS INT)),
        ' ',
        element_at(array('Smith','Jones','Brown','Davis','Miller','Wilson','Moore','Taylor','Anderson','Thomas','Jackson','White','Harris','Martin','Thompson','Garcia','Martinez','Robinson','Clark','Rodriguez','Lewis','Lee','Walker','Hall','Allen','Young','Hernandez','King','Wright','Lopez','Hill','Scott','Green','Adams','Baker','Gonzalez','Nelson','Carter','Mitchell','Perez','Roberts','Turner','Phillips','Campbell','Parker','Evans','Edwards','Collins','Stewart','Sanchez'),
            1 + CAST((b.v * 7 - 1) % 50 AS INT))
    )                                                                   AS manager_name,
    CAST(5 + (b.v % 495) AS INT)                                        AS employee_count,
    CAST(50000 + (b.v % 950000) AS DECIMAL(18,2))                       AS annual_lease_usd,
    b.v % 3 = 0                                                         AS has_pharmacy,
    b.v % 2 = 0                                                         AS has_grocery,
    b.v % 4 = 0                                                         AS has_electronics,
    b.v % 5 = 0                                                         AS has_garden,
    b.v % 7 = 0                                                         AS has_cafe,
    element_at(array('Mass','Premium','Value','Discount','Luxury'),
        1 + CAST((b.v - 1) % 5 AS INT))                                 AS target_segment
FROM generate_series(1, 25000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate dim_product (1,000,000 rows). 50 brands x 10 L1 x 20 L2 x 50 L3
-- cycle independently. Closed-form: 100K per L1, 50K per L2, 20K per L3.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_product
SELECT
    b.v                                                                 AS product_id,
    concat('SKU-', lpad(CAST(b.v AS STRING), 8, '0'))                   AS sku,
    lpad(CAST((b.v * 13) % 100000000000000 AS STRING), 14, '0')         AS upc_barcode,
    concat(
        element_at(array('Acme','Globex','Initech','Umbrella','Wayne','Stark','Soylent','Cyberdyne','Tyrell','Wonka','Hooli','Vandelay','Gringotts','Massive','Oscorp','LexCorp','PymTech','Roxxon','Frobozz','Aperture','PaperStreet','DunderMifflin','Sterling','Pawnee','Cogswell','Spacely','Yoyodyne','Strickland','VaultTec','PizzaPlanet','Bluth','Costanza','Kramerica','SterlingCooper','Pendant','InGen','Tyrell','Weyland','OmniCorp','Massive Dynamic','Gekko','Initrode','Buy More','Rekall','Macguffin','Wonka','Spadina','Vance','Zorin','Zapf'),
            1 + CAST((b.v - 1) % 50 AS INT)),
        ' ',
        element_at(array('Electronics','Apparel','Home','Grocery','Toys','Sports','Beauty','Books','Office','Pet'),
            1 + CAST((b.v - 1) % 10 AS INT)),
        ' #', CAST(b.v AS STRING)
    )                                                                   AS product_name,
    concat(
        'Premium ',
        element_at(array('Electronics','Apparel','Home','Grocery','Toys','Sports','Beauty','Books','Office','Pet'),
            1 + CAST((b.v - 1) % 10 AS INT)),
        ' from ',
        element_at(array('Acme','Globex','Initech','Umbrella','Wayne','Stark','Soylent','Cyberdyne','Tyrell','Wonka','Hooli','Vandelay','Gringotts','Massive','Oscorp','LexCorp','PymTech','Roxxon','Frobozz','Aperture','PaperStreet','DunderMifflin','Sterling','Pawnee','Cogswell','Spacely','Yoyodyne','Strickland','VaultTec','PizzaPlanet','Bluth','Costanza','Kramerica','SterlingCooper','Pendant','InGen','Tyrell','Weyland','OmniCorp','Massive Dynamic','Gekko','Initrode','Buy More','Rekall','Macguffin','Wonka','Spadina','Vance','Zorin','Zapf'),
            1 + CAST((b.v - 1) % 50 AS INT))
    )                                                                   AS short_description,
    element_at(array('Acme','Globex','Initech','Umbrella','Wayne','Stark','Soylent','Cyberdyne','Tyrell','Wonka','Hooli','Vandelay','Gringotts','Massive','Oscorp','LexCorp','PymTech','Roxxon','Frobozz','Aperture','PaperStreet','DunderMifflin','Sterling','Pawnee','Cogswell','Spacely','Yoyodyne','Strickland','VaultTec','PizzaPlanet','Bluth','Costanza','Kramerica','SterlingCooper','Pendant','InGen','Tyrell','Weyland','OmniCorp','Massive Dynamic','Gekko','Initrode','Buy More','Rekall','Macguffin','Wonka','Spadina','Vance','Zorin','Zapf'),
        1 + CAST((b.v - 1) % 50 AS INT))                                AS brand,
    element_at(array('Manufacturer-01','Manufacturer-02','Manufacturer-03','Manufacturer-04','Manufacturer-05','Manufacturer-06','Manufacturer-07','Manufacturer-08','Manufacturer-09','Manufacturer-10','Manufacturer-11','Manufacturer-12','Manufacturer-13','Manufacturer-14','Manufacturer-15','Manufacturer-16','Manufacturer-17','Manufacturer-18','Manufacturer-19','Manufacturer-20','Manufacturer-21','Manufacturer-22','Manufacturer-23','Manufacturer-24','Manufacturer-25','Manufacturer-26','Manufacturer-27','Manufacturer-28','Manufacturer-29','Manufacturer-30','Manufacturer-31','Manufacturer-32','Manufacturer-33','Manufacturer-34','Manufacturer-35','Manufacturer-36','Manufacturer-37','Manufacturer-38','Manufacturer-39','Manufacturer-40','Manufacturer-41','Manufacturer-42','Manufacturer-43','Manufacturer-44','Manufacturer-45','Manufacturer-46','Manufacturer-47','Manufacturer-48','Manufacturer-49','Manufacturer-50'),
        1 + CAST((b.v - 1) % 50 AS INT))                                AS manufacturer,
    element_at(array('Electronics','Apparel','Home','Grocery','Toys','Sports','Beauty','Books','Office','Pet'),
        1 + CAST((b.v - 1) % 10 AS INT))                                AS category_l1,
    element_at(array('Phones','Computers','Audio','TVs','Cameras','Tops','Bottoms','Shoes','Furniture','Decor','Beverages','Snacks','Frozen','Outdoor Toys','Board Games','Fitness','Outdoor Gear','Skincare','Haircare','Fiction'),
        1 + CAST((b.v - 1) % 20 AS INT))                                AS category_l2,
    element_at(array('Smartphones','Tablets','Laptops','Desktops','Headphones','Speakers','LED TVs','OLED TVs','DSLR','Mirrorless','T-Shirts','Polos','Jeans','Shorts','Sneakers','Boots','Sofas','Tables','Wall Art','Vases','Sodas','Juices','Chips','Cookies','Ice Cream','Frozen Meals','Action Figures','Building Blocks','Card Games','Strategy Games','Yoga','Cardio','Tents','Backpacks','Cleansers','Moisturizers','Shampoos','Conditioners','Novels','Biographies','Pens','Notebooks','Toys','Treats','Beds','Carriers','Vitamins','Skincare','Wraps','Cookware'),
        1 + CAST((b.v - 1) % 50 AS INT))                                AS category_l3,
    element_at(array('Hardlines','Softlines','Consumables','Services','Specialty'),
        1 + CAST((b.v - 1) % 5 AS INT))                                 AS department,
    element_at(array('SubA','SubB','SubC','SubD','SubE','SubF','SubG','SubH','SubI','SubJ'),
        1 + CAST((b.v - 1) % 10 AS INT))                                AS subdepartment,
    element_at(array('Red','Blue','Green','Black','White','Gray','Silver','Gold','Yellow','Orange'),
        1 + CAST((b.v - 1) % 10 AS INT))                                AS color,
    element_at(array('Warm','Cool','Neutral','Metallic','Earth'),
        1 + CAST((b.v - 1) % 5 AS INT))                                 AS color_family,
    element_at(array('XS','S','M','L','XL','XXL','One Size','N/A'),
        1 + CAST((b.v - 1) % 8 AS INT))                                 AS size_label,
    CAST(50 + (b.v % 4950) AS DOUBLE)                                   AS weight_grams,
    CAST(5 + (b.v % 95) AS DOUBLE)                                      AS length_cm,
    CAST(5 + (b.v % 95) AS DOUBLE)                                      AS width_cm,
    CAST(1 + (b.v % 99) AS DOUBLE)                                      AS height_cm,
    CAST(1 + (b.v % 12) AS INT)                                         AS package_count,
    CAST(5 + (b.v % 1000) AS DECIMAL(18,4))                             AS unit_cost_usd,
    CAST((5 + (b.v % 1000)) * 15 AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)) AS list_price_usd,
    CAST((5 + (b.v % 1000)) * 18 AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)) AS msrp_usd,
    CAST(b.v % 100 AS DOUBLE) / 100.0                                   AS default_margin_pct,
    CAST((b.v - 1) % 1000 + 1 AS BIGINT)                                AS supplier_id,
    concat('Supplier #', CAST((b.v - 1) % 1000 + 1 AS STRING))          AS supplier_name,
    element_at(array('US','CA','MX','UK','DE','FR','JP','AU','BR','IN'),
        1 + CAST((b.v - 1) % 10 AS INT))                                AS country_of_origin,
    lpad(CAST((b.v * 7) % 9999999999 AS STRING), 10, '0')               AS hs_tariff_code,
    element_at(array('A++','A+','A','B','C'),
        1 + CAST((b.v - 1) % 5 AS INT))                                 AS energy_rating,
    element_at(array('Box','Bag','Bottle','Bulk','Wrapped'),
        1 + CAST((b.v - 1) % 5 AS INT))                                 AS package_type,
    CAST(1 + (b.v % 99) AS INT)                                         AS units_per_case,
    CAST(1 + (b.v % 60) AS INT)                                         AS lead_time_days,
    DATE '2010-01-01' + CAST((b.v * 7) % 5475 AS INT)                   AS launch_date,
    NULL                                                                AS discontinued_date,
    b.v % 50 != 0                                                       AS is_active,
    b.v % 4 = 0                                                         AS is_seasonal,
    b.v % 5 = 0                                                         AS is_eco_certified,
    b.v % 10 != 0                                                       AS is_taxable,
    element_at(array('A','B','C','D'),
        1 + CAST((b.v - 1) % 4 AS INT))                                 AS abc_class
FROM generate_series(1, 1000000) AS b(v);

-- --------------------------------------------------------------------------
-- Populate dim_customer (5,000,000 rows). Closed-form: 1M per loyalty tier
-- (5), 1M per segment (5), 500K per occupation (10), 500K per country (10).
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.dim_customer
SELECT
    rn                                                                  AS customer_id,
    concat('CUST-', lpad(CAST(rn AS STRING), 8, '0'))                   AS customer_code,
    element_at(array('Mr.','Mrs.','Ms.','Dr.','Prof.'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS salutation,
    element_at(array('Alice','Bob','Carol','David','Emma','Frank','Grace','Henry','Iris','Jack','Karen','Leo','Maya','Noah','Olivia','Peter','Quinn','Rachel','Steve','Tina','Uma','Victor','Wendy','Xander','Yara','Zoe','Aaron','Beth','Chris','Diana','Ethan','Fiona','George','Hannah','Ian','Julia','Kevin','Laura','Mike','Nina','Oscar','Paula','Quentin','Rose','Sam','Tara','Umar','Vera','Will','Xenia'),
        1 + CAST((rn - 1) % 50 AS INT))                                 AS first_name,
    element_at(array('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'),
        1 + CAST((rn - 1) % 26 AS INT))                                 AS middle_initial,
    element_at(array('Smith','Jones','Brown','Davis','Miller','Wilson','Moore','Taylor','Anderson','Thomas','Jackson','White','Harris','Martin','Thompson','Garcia','Martinez','Robinson','Clark','Rodriguez','Lewis','Lee','Walker','Hall','Allen','Young','Hernandez','King','Wright','Lopez','Hill','Scott','Green','Adams','Baker','Gonzalez','Nelson','Carter','Mitchell','Perez','Roberts','Turner','Phillips','Campbell','Parker','Evans','Edwards','Collins','Stewart','Sanchez'),
        1 + CAST((rn * 7 - 1) % 50 AS INT))                             AS last_name,
    concat(
        element_at(array('Alice','Bob','Carol','David','Emma','Frank','Grace','Henry','Iris','Jack','Karen','Leo','Maya','Noah','Olivia','Peter','Quinn','Rachel','Steve','Tina','Uma','Victor','Wendy','Xander','Yara','Zoe','Aaron','Beth','Chris','Diana','Ethan','Fiona','George','Hannah','Ian','Julia','Kevin','Laura','Mike','Nina','Oscar','Paula','Quentin','Rose','Sam','Tara','Umar','Vera','Will','Xenia'),
            1 + CAST((rn - 1) % 50 AS INT)),
        ' ',
        element_at(array('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'),
            1 + CAST((rn - 1) % 26 AS INT)),
        '. ',
        element_at(array('Smith','Jones','Brown','Davis','Miller','Wilson','Moore','Taylor','Anderson','Thomas','Jackson','White','Harris','Martin','Thompson','Garcia','Martinez','Robinson','Clark','Rodriguez','Lewis','Lee','Walker','Hall','Allen','Young','Hernandez','King','Wright','Lopez','Hill','Scott','Green','Adams','Baker','Gonzalez','Nelson','Carter','Mitchell','Perez','Roberts','Turner','Phillips','Campbell','Parker','Evans','Edwards','Collins','Stewart','Sanchez'),
            1 + CAST((rn * 7 - 1) % 50 AS INT))
    )                                                                   AS full_name,
    concat('cust', CAST(rn AS STRING), '@',
        element_at(array('example','retail','demo','test','sample'),
            1 + CAST((rn - 1) % 5 AS INT)),
        '.com')                                                         AS email,
    concat('+1', lpad(CAST(rn % 10000000000 AS STRING), 10, '0'))       AS phone_e164,
    element_at(array('M','F','X','U'),
        1 + CAST((rn - 1) % 4 AS INT))                                  AS gender,
    DATE '1940-01-01' + CAST((rn * 13) % 25000 AS INT)                  AS birth_date,
    element_at(array('18-24','25-34','35-44','45-54','55+'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS age_band,
    element_at(array('Single','Married','Divorced','Widowed','Partner'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS marital_status,
    element_at(array('High School','Associate','Bachelor','Master','Doctorate'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS education_level,
    element_at(array('Engineer','Teacher','Manager','Sales','Healthcare','Retail','Service','Technician','Analyst','Professional'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS occupation,
    concat(
        element_at(array('Acme','Globex','Initech','Umbrella','Wayne','Stark','Soylent','Cyberdyne','Tyrell','Wonka'),
            1 + CAST((rn - 1) % 10 AS INT)),
        ' Industries')                                                  AS employer_name,
    CAST(25000 + (rn % 475000) AS DECIMAL(18,2))                        AS annual_income_usd,
    element_at(array('Under $25K','$25K-$50K','$50K-$100K','$100K-$200K','$200K+'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS income_band,
    CAST(1 + (rn % 6) AS INT)                                           AS household_size,
    CAST(rn % 5 AS INT)                                                 AS number_of_children,
    concat(CAST(rn AS STRING), ' ',
        element_at(array('Main St','Oak Ave','Maple Dr','Cedar Ln','Elm St','Pine Rd','Birch Way','Walnut Ct','Spruce Pl','Willow Ln','Cherry St','Park Ave','Lake Dr','River Rd','Hill St','Forest Ave','Meadow Ln','Sunset Blvd','Highland Dr','Valley View'),
            1 + CAST((rn - 1) % 20 AS INT)))                            AS address_line_1,
    CASE WHEN rn % 3 = 0 THEN concat('Apt #', CAST(rn % 999 + 1 AS STRING)) ELSE NULL END AS address_line_2,
    element_at(array('New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','Fort Worth','Columbus','Charlotte','San Francisco','Indianapolis','Seattle','Denver','Washington','Boston','El Paso','Detroit','Nashville','Memphis','Portland','Oklahoma City','Las Vegas','Louisville','Baltimore','Milwaukee','Albuquerque','Tucson','Fresno','Sacramento','Mesa','Kansas City','Atlanta','Miami','Raleigh','Omaha','Long Beach','Virginia Beach','Oakland','Minneapolis','Tulsa','Arlington','Tampa','New Orleans','Wichita'),
        1 + CAST((rn - 1) % 50 AS INT))                                 AS city,
    element_at(array('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'),
        1 + CAST((rn - 1) % 50 AS INT))                                 AS state_code,
    element_at(array('Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida','Georgia','Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine','Maryland','Massachusetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire','New Jersey','New Mexico','New York','North Carolina','North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont','Virginia','Washington','West Virginia','Wisconsin','Wyoming'),
        1 + CAST((rn - 1) % 50 AS INT))                                 AS state_name,
    lpad(CAST(rn % 99999 AS STRING), 5, '0')                            AS postal_code,
    element_at(array('US','CA','MX','UK','DE','FR','JP','AU','BR','IN'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS country_code,
    element_at(array('United States','Canada','Mexico','United Kingdom','Germany','France','Japan','Australia','Brazil','India'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS country_name,
    element_at(array('NA','EU','APAC','LATAM','MEA'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS region,
    -90.0 + CAST(rn % 18000 AS DOUBLE) / 100.0                          AS latitude,
    -180.0 + CAST(rn % 36000 AS DOUBLE) / 100.0                         AS longitude,
    DATE '2018-01-01' + CAST((rn * 7) % 2192 AS INT)                    AS signup_date,
    element_at(array('Web','Mobile','Store','Phone','Referral','Social','Email','Partner'),
        1 + CAST((rn - 1) % 8 AS INT))                                  AS signup_channel,
    element_at(array('Email','SMS','Phone','Mail'),
        1 + CAST((rn - 1) % 4 AS INT))                                  AS preferred_contact_channel,
    rn % 4 = 0                                                          AS marketing_opt_in,
    rn % 5 = 0                                                          AS sms_opt_in,
    element_at(array('Bronze','Silver','Gold','Platinum','Diamond'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS loyalty_tier,
    CAST((rn * 11) % 50000 AS INT)                                      AS loyalty_points_balance,
    CAST(rn % 100 AS INT)                                               AS lifetime_orders,
    CAST(rn % 100000 AS DECIMAL(18,2)) + CAST(0.99 AS DECIMAL(18,2))    AS lifetime_revenue_usd,
    DATE '2024-01-01' + CAST((rn * 3) % 365 AS INT)                     AS last_purchase_date,
    CAST(rn % 100 AS DOUBLE) / 100.0                                    AS churn_risk_score,
    element_at(array('New','Active','At Risk','VIP','Inactive'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS segment
FROM (
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 4) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate fact_sales (200,000,000 rows). Order grain.
-- 75 columns including 15 denormalized customer/product/store columns so
-- Power BI Import slicers do not require joins.
-- Closed-form distributions: every cycling array divides 200M evenly so
-- each value gets an exact row count.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.fact_sales
SELECT
    rn                                                                  AS sale_id,
    CAST((rn - 1) / 4 AS BIGINT) + 1                                    AS order_id,
    CAST((rn - 1) % 4 AS INT) + 1                                       AS line_number,
    concat('RCPT-', lpad(CAST(CAST((rn - 1) / 4 AS BIGINT) + 1 AS STRING), 10, '0')) AS receipt_number,
    lpad(CAST(rn AS STRING), 32, '0')                                   AS transaction_uuid,
    CAST(year(DATE '2020-01-01' + CAST(rn % 1825 AS INT)) * 10000
       + month(DATE '2020-01-01' + CAST(rn % 1825 AS INT)) * 100
       + dayofmonth(DATE '2020-01-01' + CAST(rn % 1825 AS INT)) AS INT)        AS date_key,
    CAST((rn * 17) % 5000000 + 1 AS BIGINT)                             AS customer_key,
    CAST((rn * 13) % 1000000 + 1 AS BIGINT)                             AS product_key,
    CAST((rn * 7)  % 25000 + 1 AS BIGINT)                               AS store_key,
    CAST((rn * 11) % 50000 + 1 AS BIGINT)                               AS employee_key,
    CAST((rn * 19) % 5000 + 1 AS BIGINT)                                AS promotion_key,
    CAST((rn * 23) % 100000 + 1 AS BIGINT)                              AS ship_to_geography_key,
    DATE '2020-01-01' + CAST(rn % 1825 AS INT)                          AS order_date,
    make_timestamp(2024, 1 + CAST(rn % 12 AS INT), 1 + CAST(rn % 28 AS INT),
                   CAST((rn % 86400) / 3600 AS INT),
                   CAST(((rn % 86400) % 3600) / 60 AS INT),
                   CAST((rn % 86400) % 60 AS DOUBLE))                   AS order_ts,
    DATE '2020-01-01' + CAST(rn % 1825 AS INT) + 1                      AS ship_date,
    make_timestamp(2024, 1 + CAST(rn % 12 AS INT), 1 + CAST(rn % 28 AS INT),
                   CAST(((rn + 3600) % 86400) / 3600 AS INT),
                   CAST((((rn + 3600) % 86400) % 3600) / 60 AS INT),
                   CAST(((rn + 3600) % 86400) % 60 AS DOUBLE))          AS ship_ts,
    DATE '2020-01-01' + CAST(rn % 1825 AS INT) + 5                      AS delivery_date,
    make_timestamp(2024, 1 + CAST(rn % 12 AS INT), 1 + CAST(rn % 28 AS INT),
                   CAST(((rn + 7200) % 86400) / 3600 AS INT),
                   CAST((((rn + 7200) % 86400) % 3600) / 60 AS INT),
                   CAST(((rn + 7200) % 86400) % 60 AS DOUBLE))          AS delivery_ts,
    CAST(rn % 24 AS INT)                                                AS hour_of_day,
    CAST(dayofweek(DATE '2020-01-01' + CAST(rn % 1825 AS INT)) AS INT)  AS day_of_week,
    CAST(1 + (rn % 10) AS INT)                                          AS quantity,
    CAST(5 + (rn % 1000) AS DECIMAL(18,4))                              AS unit_price_usd,
    CAST((5 + (rn % 1000)) * 12 AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)) AS list_price_usd,
    CAST((5 + (rn % 1000)) * 6 AS DECIMAL(18,4))  / CAST(10 AS DECIMAL(18,4)) AS unit_cost_usd,
    CAST(rn % 25 AS DOUBLE) / 100.0                                     AS discount_pct,
    CAST((5 + (rn % 1000)) * (rn % 25) AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4)) AS discount_amt_usd,
    CAST((5 + (rn % 1000)) * (1 + (rn % 10)) AS DECIMAL(18,4))          AS line_subtotal_usd,
    CAST(0.08 AS DOUBLE)                                                AS tax_pct,
    CAST((5 + (rn % 1000)) * (1 + (rn % 10)) * 8 AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4)) AS tax_amt_usd,
    CAST(rn % 30 AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4))          AS shipping_cost_usd,
    CAST(rn % 5 AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4))           AS handling_fee_usd,
    CAST((5 + (rn % 1000)) * (1 + (rn % 10)) AS DECIMAL(18,4))          AS gross_revenue_usd,
    CAST((5 + (rn % 1000)) * (1 + (rn % 10)) AS DECIMAL(18,4))
        - (CAST((5 + (rn % 1000)) * (rn % 25) AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4))) AS net_revenue_usd,
    CAST((5 + (rn % 1000)) * (1 + (rn % 10)) AS DECIMAL(18,4))
        - (CAST((5 + (rn % 1000)) * (rn % 25) AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4)))
        + (CAST((5 + (rn % 1000)) * (1 + (rn % 10)) * 8 AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4)))
        + (CAST(rn % 30 AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)))
        + (CAST(rn % 5 AS DECIMAL(18,4))  / CAST(10 AS DECIMAL(18,4))) AS total_amount_usd,
    CAST((5 + (rn % 1000)) * (1 + (rn % 10)) * 6 AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)) AS cogs_usd,
    CAST((5 + (rn % 1000)) * (1 + (rn % 10)) AS DECIMAL(18,4))
        - (CAST((5 + (rn % 1000)) * (rn % 25) AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4)))
        - (CAST((5 + (rn % 1000)) * (1 + (rn % 10)) * 6 AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4))) AS gross_profit_usd,
    CAST(rn % 100 AS DOUBLE) / 100.0                                    AS gross_margin_pct,
    CAST((1 + (rn % 10)) * 10 AS INT)                                   AS loyalty_points_earned,
    CAST(rn % 50 AS INT)                                                AS loyalty_points_redeemed,
    CASE WHEN rn % 20 = 0 THEN CAST(25 AS DECIMAL(18,4)) ELSE CAST(0 AS DECIMAL(18,4)) END AS gift_card_amount_usd,
    CASE WHEN rn % 30 = 0 THEN CAST(10 AS DECIMAL(18,4)) ELSE CAST(0 AS DECIMAL(18,4)) END AS store_credit_amount_usd,
    CAST((rn % 100) * 5 AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4))  AS tip_amount_usd,
    CASE WHEN rn % 25 = 0
        THEN CAST((5 + (rn % 1000)) * (1 + (rn % 10)) AS DECIMAL(18,4)) / CAST(2 AS DECIMAL(18,4))
        ELSE CAST(0 AS DECIMAL(18,4))
    END                                                                 AS refund_amount_usd,
    CAST(1.0 AS DOUBLE)                                                 AS exchange_rate_to_usd,
    element_at(array('In-Store','Online','Mobile App','Phone','Marketplace'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS sales_channel,
    element_at(array('Credit Card','Debit Card','Cash','Mobile Wallet','Gift Card','Bank Transfer','BNPL','Crypto'),
        1 + CAST((rn - 1) % 8 AS INT))                                  AS payment_method,
    element_at(array('Visa','Mastercard','Amex','Discover','Other'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS payment_card_type,
    element_at(array('USD','EUR','GBP','CAD','JPY'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS currency_code,
    element_at(array('Ship','Pickup','Delivery','Locker'),
        1 + CAST((rn - 1) % 4 AS INT))                                  AS fulfillment_method,
    rn % 20 = 0                                                         AS return_flag,
    element_at(array('Pending','Confirmed','Shipped','Delivered'),
        1 + CAST((rn - 1) % 4 AS INT))                                  AS order_status,
    element_at(array('Pending','Authorized','Captured','Settled'),
        1 + CAST((rn - 1) % 4 AS INT))                                  AS payment_status,
    element_at(array('Pending','Picking','InTransit','Delivered'),
        1 + CAST((rn - 1) % 4 AS INT))                                  AS fulfillment_status,
    CASE WHEN rn % 20 = 0
        THEN element_at(array('Defective','Wrong Item','Not As Described','Damaged','Late','Changed Mind','Better Price','Quality'),
                1 + CAST(((rn / 20) - 1) % 8 AS INT))
        ELSE 'NONE'
    END                                                                 AS return_reason_code,
    element_at(array('New','Active','At Risk','VIP','Inactive'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS customer_segment,
    element_at(array('US','CA','MX','UK','DE','FR','JP','AU','BR','IN'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS customer_country_code,
    element_at(array('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'),
        1 + CAST((rn - 1) % 50 AS INT))                                 AS customer_state_code,
    element_at(array('New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','Fort Worth','Columbus','Charlotte','San Francisco','Indianapolis','Seattle','Denver','Washington','Boston','El Paso','Detroit','Nashville','Memphis','Portland','Oklahoma City','Las Vegas','Louisville','Baltimore','Milwaukee','Albuquerque','Tucson','Fresno','Sacramento','Mesa','Kansas City','Atlanta','Miami','Raleigh','Omaha','Long Beach','Virginia Beach','Oakland','Minneapolis','Tulsa','Arlington','Tampa','New Orleans','Wichita'),
        1 + CAST((rn - 1) % 50 AS INT))                                 AS customer_city,
    element_at(array('Bronze','Silver','Gold','Platinum','Diamond'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS customer_loyalty_tier,
    element_at(array('Electronics','Apparel','Home','Grocery','Toys','Sports','Beauty','Books','Office','Pet'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS product_category_l1,
    element_at(array('Phones','Computers','Audio','TVs','Cameras','Tops','Bottoms','Shoes','Furniture','Decor','Beverages','Snacks','Frozen','Outdoor Toys','Board Games','Fitness','Outdoor Gear','Skincare','Haircare','Fiction'),
        1 + CAST((rn - 1) % 20 AS INT))                                 AS product_category_l2,
    element_at(array('Acme','Globex','Initech','Umbrella','Wayne','Stark','Soylent','Cyberdyne','Tyrell','Wonka','Hooli','Vandelay','Gringotts','Massive','Oscorp','LexCorp','PymTech','Roxxon','Frobozz','Aperture','PaperStreet','DunderMifflin','Sterling','Pawnee','Cogswell','Spacely','Yoyodyne','Strickland','VaultTec','PizzaPlanet','Bluth','Costanza','Kramerica','SterlingCooper','Pendant','InGen','Tyrell','Weyland','OmniCorp','Massive Dynamic','Gekko','Initrode','Buy More','Rekall','Macguffin','Wonka','Spadina','Vance','Zorin','Zapf'),
        1 + CAST((rn - 1) % 50 AS INT))                                 AS product_brand,
    element_at(array('Warm','Cool','Neutral','Metallic','Earth'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS product_color_family,
    element_at(array('Hardlines','Softlines','Consumables','Services','Specialty'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS product_department,
    element_at(array('NA','EU','APAC','LATAM','MEA'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS store_region,
    element_at(array('Big Box','Neighborhood','Convenience','E-commerce','Wholesale'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS store_format,
    element_at(array('US','CA','MX','UK','DE','FR','JP','AU','BR','IN'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS store_country_code,
    element_at(array('PacificMart','PacificFresh','PacificDirect','PacificClub','PacificExpress'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS store_banner,
    element_at(array('Hypermarket','Supermarket','Express','Online','Marketplace'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS store_type,
    element_at(array('Desktop','Mobile','Tablet','Smart TV','Wearable'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS device_type,
    element_at(array('Chrome','Firefox','Safari','Edge','Opera','Samsung','Brave','UC'),
        1 + CAST((rn - 1) % 8 AS INT))                                  AS browser,
    element_at(array('Organic','Paid Search','Social','Email','Direct','Referral','Display','Affiliate'),
        1 + CAST((rn - 1) % 8 AS INT))                                  AS source_traffic_channel,
    element_at(array('CAMP-001','CAMP-002','CAMP-003','CAMP-004','CAMP-005','CAMP-006','CAMP-007','CAMP-008','CAMP-009','CAMP-010'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS marketing_campaign_code,
    element_at(array('Team-A','Team-B','Team-C','Team-D','Team-E'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS sales_associate_team,
    concat('FY', CAST(year(DATE '2020-01-01' + CAST(rn % 1825 AS INT))
        + CASE WHEN month(DATE '2020-01-01' + CAST(rn % 1825 AS INT)) >= 4 THEN 1 ELSE 0 END AS STRING)) AS fiscal_year_label
FROM (
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 199) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate fact_inventory_snapshot (100,000,000 rows). Daily store and
-- product snapshots over a 365 day window starting 2024-01-01.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.fact_inventory_snapshot
SELECT
    rn                                                                  AS inventory_snapshot_id,
    DATE '2024-01-01' + CAST(rn % 365 AS INT)                           AS snapshot_date,
    CAST(year(DATE '2024-01-01' + CAST(rn % 365 AS INT)) * 10000
       + month(DATE '2024-01-01' + CAST(rn % 365 AS INT)) * 100
       + dayofmonth(DATE '2024-01-01' + CAST(rn % 365 AS INT)) AS INT)         AS snapshot_date_key,
    CAST((rn * 7)  % 25000 + 1 AS BIGINT)                               AS store_key,
    CAST((rn * 13) % 1000000 + 1 AS BIGINT)                             AS product_key,
    CAST(rn % 1000 AS INT)                                              AS on_hand_units,
    CAST(rn % 500 AS INT)                                               AS on_order_units,
    CAST(rn % 200 AS INT)                                               AS in_transit_units,
    CAST(rn % 100 AS INT)                                               AS allocated_units,
    CAST((rn % 1000) - (rn % 100) AS INT)                               AS available_units,
    CAST(rn % 1000 AS DOUBLE) / 10.0                                    AS days_of_supply,
    CAST(50 + (rn % 200) AS INT)                                        AS reorder_point,
    CAST(500 + (rn % 1500) AS INT)                                      AS max_stock_level,
    CAST(10 + (rn % 50) AS INT)                                         AS min_stock_level,
    DATE '2024-01-01' + CAST(rn % 365 AS INT) - CAST(rn % 30 AS INT)    AS last_received_date,
    CAST(rn % 500 AS INT)                                               AS last_received_qty,
    DATE '2024-01-01' + CAST(rn % 365 AS INT) - CAST(rn % 60 AS INT)    AS last_sold_date,
    CAST(rn % 60 AS INT)                                                AS days_since_last_sale,
    CAST(rn % 1000 AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4))        AS valuation_unit_cost_usd,
    CAST((rn % 1000) * (rn % 1000) AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)) AS valuation_total_cost_usd,
    CAST((rn % 1000) * (rn % 1000) * 15 AS DECIMAL(18,4)) / CAST(100 AS DECIMAL(18,4)) AS retail_value_usd,
    CAST(rn % 10 AS INT)                                                AS shrink_units_mtd,
    CAST((rn % 10) * (rn % 1000) AS DECIMAL(18,4)) / CAST(10 AS DECIMAL(18,4)) AS shrink_value_usd_mtd,
    CAST(rn % 100 AS DOUBLE) / 100.0                                    AS sell_through_pct_mtd,
    element_at(array('A','B','C','D'),
        1 + CAST((rn - 1) % 4 AS INT))                                  AS abc_classification,
    element_at(array('In Stock','Low Stock','Out of Stock','Overstock','Discontinued'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS stock_status,
    element_at(array('NA','EU','APAC','LATAM','MEA'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS store_region,
    element_at(array('Electronics','Apparel','Home','Grocery','Toys','Sports','Beauty','Books','Office','Pet'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS product_category_l1
FROM (
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 99) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

-- --------------------------------------------------------------------------
-- Populate fact_web_events (200,000,000 rows). Clickstream over 365 days
-- starting 2024-01-01. 5 events per session = 40M sessions.
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.retail.fact_web_events
SELECT
    rn                                                                  AS event_id,
    concat('SESS-', lpad(CAST(rn / 5 + 1 AS STRING), 10, '0'))          AS session_id,
    CAST((rn * 17) % 5000000 + 1 AS BIGINT)                             AS customer_key,
    make_timestamp(2024, 1 + CAST(rn % 12 AS INT), 1 + CAST(rn % 28 AS INT),
                   CAST((rn % 86400) / 3600 AS INT),
                   CAST(((rn % 86400) % 3600) / 60 AS INT),
                   CAST((rn % 86400) % 60 AS DOUBLE))                   AS event_ts,
    CAST(year(DATE '2024-01-01' + CAST(rn % 365 AS INT)) * 10000
       + month(DATE '2024-01-01' + CAST(rn % 365 AS INT)) * 100
       + dayofmonth(DATE '2024-01-01' + CAST(rn % 365 AS INT)) AS INT)         AS event_date_key,
    element_at(array('page_view','add_to_cart','remove_from_cart','checkout_start','checkout_complete','search','product_view','click_recommendation','share','wishlist_add'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS event_type,
    concat('/',
        element_at(array('home','category','product','search','cart','checkout','account','help','blog','offers'),
            1 + CAST((rn - 1) % 10 AS INT)),
        '/', CAST(rn % 10000 AS STRING))                                AS page_path,
    concat('Page ', CAST(rn AS STRING))                                 AS page_title,
    element_at(array('Home','Category','Product','Search','Cart','Account','Help','Editorial'),
        1 + CAST((rn - 1) % 8 AS INT))                                  AS page_category,
    element_at(array('https://google.com','https://facebook.com','https://twitter.com','https://instagram.com','https://reddit.com','https://youtube.com','https://linkedin.com','https://tiktok.com','direct','email'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS referrer,
    element_at(array('google','facebook','twitter','instagram','reddit','youtube','linkedin','tiktok'),
        1 + CAST((rn - 1) % 8 AS INT))                                  AS utm_source,
    element_at(array('cpc','organic','social','email','referral'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS utm_medium,
    element_at(array('CAMP-001','CAMP-002','CAMP-003','CAMP-004','CAMP-005','CAMP-006','CAMP-007','CAMP-008','CAMP-009','CAMP-010'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS utm_campaign,
    element_at(array('Desktop','Mobile','Tablet','Smart TV','Wearable'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS device_type,
    element_at(array('Apple','Samsung','Google','Microsoft','Lenovo','Dell','HP','Asus','Sony','Other'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS device_brand,
    element_at(array('Chrome','Firefox','Safari','Edge','Opera','Samsung','Brave','UC'),
        1 + CAST((rn - 1) % 8 AS INT))                                  AS browser,
    concat(CAST(80 + (rn % 40) AS STRING), '.0')                        AS browser_version,
    element_at(array('Windows','macOS','iOS','Android','Linux'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS os,
    concat(CAST(10 + (rn % 10) AS STRING), '.', CAST(rn % 10 AS STRING)) AS os_version,
    element_at(array('US','CA','MX','UK','DE','FR','JP','AU','BR','IN'),
        1 + CAST((rn - 1) % 10 AS INT))                                 AS country_code,
    element_at(array('NA','EU','APAC','LATAM','MEA'),
        1 + CAST((rn - 1) % 5 AS INT))                                  AS region,
    element_at(array('New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego','Dallas','San Jose','Austin','Jacksonville','Fort Worth','Columbus','Charlotte','San Francisco','Indianapolis','Seattle','Denver','Washington','Boston','El Paso','Detroit','Nashville','Memphis','Portland','Oklahoma City','Las Vegas','Louisville','Baltimore','Milwaukee','Albuquerque','Tucson','Fresno','Sacramento','Mesa','Kansas City','Atlanta','Miami','Raleigh','Omaha','Long Beach','Virginia Beach','Oakland','Minneapolis','Tulsa','Arlington','Tampa','New Orleans','Wichita'),
        1 + CAST((rn - 1) % 50 AS INT))                                 AS city,
    lpad(CAST((rn * 31) % 1000000000000 AS STRING), 16, '0')            AS ip_hash,
    lpad(CAST((rn * 37) % 1000000000000 AS STRING), 16, '0')            AS user_agent_hash,
    CAST(rn % 3600 AS INT)                                              AS time_on_page_sec,
    CAST(rn % 100 AS INT)                                               AS scroll_depth_pct,
    CAST(768 + (rn % 1280) AS INT)                                      AS viewport_width,
    CAST(600 + (rn % 800) AS INT)                                       AS viewport_height,
    rn % 10 = 0                                                         AS is_bounce,
    CASE WHEN rn % 50 = 0
        THEN CAST(rn % 1000 AS DECIMAL(18,4))
        ELSE CAST(0 AS DECIMAL(18,4))
    END                                                                 AS conversion_value_usd,
    CAST(rn % 20 AS INT)                                                AS products_viewed_count,
    CASE WHEN rn % 10 = 0 THEN concat('q-', CAST(rn % 1000 AS STRING)) ELSE NULL END AS search_query
FROM (
    SELECT a.v * 1000000 + b.v AS rn
    FROM generate_series(0, 199) AS a(v)
    CROSS JOIN generate_series(1, 1000000) AS b(v)
) t;

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
