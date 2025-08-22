CREATE DATABASE Amazon_Sales;
GO

USE Amazon_Sales;

CREATE TABLE Orders (
    OrderID            VARCHAR(MAX),
    Date_              VARCHAR(MAX),
    Status             VARCHAR(MAX),
    Fulfilment         VARCHAR(MAX),
    SalesChannel       VARCHAR(MAX),
    ShipServiceLevel   VARCHAR(MAX),
    Style              VARCHAR(MAX),
    SKU                VARCHAR(MAX),
    Category           VARCHAR(MAX),
    Size               VARCHAR(MAX),
    ASIN               VARCHAR(MAX),
    CourierStatus      VARCHAR(MAX),
    Qty                VARCHAR(MAX),
    Currency           VARCHAR(MAX),
    Amount             VARCHAR(MAX),
    ShipCity           VARCHAR(MAX),
    ShipState          VARCHAR(MAX),
    ShipPostalCode     VARCHAR(MAX),
    ShipCountry        VARCHAR(MAX),
    PromotionIDs       VARCHAR(MAX),
    B2B                VARCHAR(MAX),
    FulfilledBy        VARCHAR(MAX)
);



CREATE TABLE stg_orders (
    [Order ID] NVARCHAR(255),
    [Date] NVARCHAR(255),
    [Qty] NVARCHAR(255),
    [Amount] NVARCHAR(255),
    [currency] NVARCHAR(50),
    [Status] NVARCHAR(100),
    [Courier Status] NVARCHAR(100),
    [Fulfilment] NVARCHAR(100),
    [fulfilled-by] NVARCHAR(100),
    [Sales Channel ] NVARCHAR(100),
    [ship-service-level] NVARCHAR(100),
    [ASIN] NVARCHAR(50),
    [SKU] NVARCHAR(100),
    [Style] NVARCHAR(100),
    [Category] NVARCHAR(100),
    [Size] NVARCHAR(50),
    [ship-city] NVARCHAR(100),
    [ship-state] NVARCHAR(100),
    [ship-postal-code] NVARCHAR(50),
    [ship-country] NVARCHAR(50),
    [promotion-ids] NVARCHAR(MAX),
    [B2B] NVARCHAR(50)
);


BULK INSERT Orders
FROM 'C:\Users\Roshan\Desktop\data\archive\Amazon Sale Report.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK,
    CODEPAGE = '65001'
);

CREATE OR ALTER VIEW v_orders_clean AS
SELECT
    [Order ID] AS order_id,

    COALESCE(
        TRY_CONVERT(DATETIME, [Date], 120),  -- yyyy-mm-dd hh:mi:ss
        TRY_CONVERT(DATETIME, [Date], 121),  -- yyyy-mm-dd hh:mi:ss.mmm
        TRY_CONVERT(DATETIME, [Date], 101),  -- mm/dd/yyyy
        TRY_CONVERT(DATETIME, [Date], 103),  -- dd/mm/yyyy
        TRY_CONVERT(DATETIME, [Date], 105)   -- dd-mm-yyyy
    ) AS order_ts,

    TRY_CAST(NULLIF([Qty], '') AS INT) AS qty,

    TRY_CAST(REPLACE(NULLIF([Amount], ''), ',', '') AS DECIMAL(14,2)) AS amount,

    NULLIF([currency], '') AS currency,
    NULLIF([Status], '') AS order_status,
    NULLIF([Courier Status], '') AS courier_status,
    NULLIF([Fulfilment], '') AS fulfilment,
    NULLIF([fulfilled-by], '') AS fulfilled_by,
    NULLIF([Sales Channel], '') AS sales_channel,
    NULLIF([ship-service-level], '') AS ship_service_level,
    NULLIF([ASIN], '') AS asin,
    NULLIF([SKU], '') AS sku,
    NULLIF([Style], '') AS style,
    NULLIF([Category], '') AS category,
    NULLIF([Size], '') AS size,
    NULLIF([ship-city], '') AS ship_city,
    NULLIF([ship-state], '') AS ship_state,
    NULLIF([ship-postal-code], '') AS ship_postal_code,
    NULLIF([ship-country], '') AS ship_country,
    NULLIF([promotion-ids], '') AS promotion_ids,

    CASE
        WHEN LOWER(LTRIM(RTRIM(COALESCE([B2B], '')))) IN ('1','y','yes','true','t') THEN CAST(1 AS BIT)
        WHEN LOWER(LTRIM(RTRIM(COALESCE([B2B], '')))) IN ('0','n','no','false','f') THEN CAST(0 AS BIT)
        ELSE NULL
    END AS is_b2b

FROM stg_orders
WHERE COALESCE([Order ID], '') <> '';


--Dimension tables
-- Date Dimension
CREATE TABLE dim_date (
    date_id      INT IDENTITY(1,1) PRIMARY KEY,
    dte          DATE NOT NULL UNIQUE,
    year         INT,
    month        INT,
    day          INT,
    week_year    INT,
    month_name   NVARCHAR(20),
    quarter      INT
);

-- Currency Dimension
CREATE TABLE dim_currency (
    currency_code NVARCHAR(10) PRIMARY KEY
);

-- Status Dimension
CREATE TABLE dim_status (
    status_id      INT IDENTITY(1,1) PRIMARY KEY,
    order_status   NVARCHAR(50),
    courier_status NVARCHAR(50),
    CONSTRAINT UQ_dim_status UNIQUE (order_status, courier_status)
);

-- Channel Dimension
CREATE TABLE dim_channel (
    channel_id         INT IDENTITY(1,1) PRIMARY KEY,
    sales_channel      NVARCHAR(100),
    fulfilment         NVARCHAR(100),
    fulfilled_by       NVARCHAR(100),
    ship_service_level NVARCHAR(100),
    is_b2b             BIT,
    CONSTRAINT UQ_dim_channel UNIQUE (sales_channel, fulfilment, fulfilled_by, ship_service_level, is_b2b)
);

-- Customer Location Dimension
CREATE TABLE dim_customer_location (
    customer_loc_id INT IDENTITY(1,1) PRIMARY KEY,
    ship_city       NVARCHAR(100),
    ship_state      NVARCHAR(100),
    ship_postal     NVARCHAR(20),
    ship_country    NVARCHAR(100),
    CONSTRAINT UQ_dim_customer_location UNIQUE (ship_city, ship_state, ship_postal, ship_country)
);

-- Product Dimension
CREATE TABLE dim_product (
    product_id INT IDENTITY(1,1) PRIMARY KEY,
    asin       NVARCHAR(50)  NULL,
    sku        NVARCHAR(100) NULL,
    style      NVARCHAR(100) NULL,
    category   NVARCHAR(100) NULL,
    size       NVARCHAR(50)  NULL,

    -- computed columns to handle NULLs
    asin_u     AS ISNULL(asin,'')     PERSISTED,
    sku_u      AS ISNULL(sku,'')      PERSISTED,
    style_u    AS ISNULL(style,'')    PERSISTED,
    category_u AS ISNULL(category,'') PERSISTED,
    size_u     AS ISNULL(size,'')     PERSISTED
);

-- unique index on computed columns
CREATE UNIQUE INDEX UQ_dim_product_composite
ON dim_product (asin_u, sku_u, style_u, category_u, size_u);

-- currency dimension
CREATE TABLE dim_currency (
    currency_code VARCHAR(50) PRIMARY KEY
);

-- currency dimension
DROP TABLE IF EXISTS dim_currency;
CREATE TABLE dim_currency (
    currency_code NVARCHAR(50) PRIMARY KEY
);

-- fact table
DROP TABLE IF EXISTS fact_order_lines;
CREATE TABLE fact_order_lines (
    fact_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    order_id          NVARCHAR(255) NOT NULL,
    date_id           INT FOREIGN KEY REFERENCES dim_date(date_id),
    currency_code     NVARCHAR(50) FOREIGN KEY REFERENCES dim_currency(currency_code),
    status_id         INT FOREIGN KEY REFERENCES dim_status(status_id),
    channel_id        INT FOREIGN KEY REFERENCES dim_channel(channel_id),
    customer_loc_id   INT FOREIGN KEY REFERENCES dim_customer_location(customer_loc_id),
    product_id        INT FOREIGN KEY REFERENCES dim_product(product_id),
    qty               INT,
    amount            NUMERIC(14,2)
);


--Populate dims

-- Date Dimension
INSERT INTO dim_date (dte, year, month, day, week_year, month_name, quarter)
SELECT DISTINCT
    CAST(order_ts AS DATE) AS dte,
    DATEPART(YEAR, order_ts)   AS year,
    DATEPART(MONTH, order_ts)  AS month,
    DATEPART(DAY, order_ts)    AS day,
    DATEPART(WEEK, order_ts)   AS week_year,
    DATENAME(MONTH, order_ts)  AS month_name,
    DATEPART(QUARTER, order_ts) AS quarter
FROM v_orders_clean
WHERE order_ts IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dim_date dd
      WHERE dd.dte = CAST(v_orders_clean.order_ts AS DATE)
  );

-- Currency Dimension
INSERT INTO dim_currency (currency_code)
SELECT DISTINCT currency
FROM v_orders_clean v
WHERE currency IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dim_currency c
      WHERE c.currency_code = v.currency
  );

-- Status Dimension
INSERT INTO dim_status (order_status, courier_status)
SELECT DISTINCT order_status, courier_status
FROM v_orders_clean v
WHERE NOT EXISTS (
    SELECT 1 FROM dim_status s
    WHERE s.order_status = v.order_status
      AND s.courier_status = v.courier_status
);

-- Channel Dimension
INSERT INTO dim_channel (sales_channel, fulfilment, fulfilled_by, ship_service_level, is_b2b)
SELECT DISTINCT sales_channel, fulfilment, fulfilled_by, ship_service_level, is_b2b
FROM v_orders_clean v
WHERE NOT EXISTS (
    SELECT 1 FROM dim_channel c
    WHERE c.sales_channel = v.sales_channel
      AND c.fulfilment = v.fulfilment
      AND c.fulfilled_by = v.fulfilled_by
      AND c.ship_service_level = v.ship_service_level
      AND ISNULL(c.is_b2b,0) = ISNULL(v.is_b2b,0)
);

-- Customer Location Dimension
INSERT INTO dim_customer_location (ship_city, ship_state, ship_postal, ship_country)
SELECT DISTINCT ship_city, ship_state, ship_postal_code, ship_country
FROM v_orders_clean v
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer_location cl
    WHERE cl.ship_city = v.ship_city
      AND cl.ship_state = v.ship_state
      AND cl.ship_postal = v.ship_postal_code
      AND cl.ship_country = v.ship_country
);

-- Product Dimension
INSERT INTO dim_product (asin, sku, style, category, size)
SELECT DISTINCT asin, sku, style, category, size
FROM v_orders_clean v
WHERE NOT EXISTS (
    SELECT 1 FROM dim_product p
    WHERE p.asin = v.asin
      AND p.sku = v.sku
      AND p.style = v.style
      AND p.category = v.category
      AND p.size = v.size
);


--Populate fact table
-- 7) Populate fact table
INSERT INTO fact_order_lines (
  order_id, date_id, currency_code, status_id, channel_id, customer_loc_id, product_id, qty, amount
)
SELECT
  v.order_id,
  d.date_id,
  v.currency,
  s.status_id,
  c.channel_id,
  l.customer_loc_id,
  p.product_id,
  v.qty,
  v.amount
FROM v_orders_clean v
LEFT JOIN dim_date d 
       ON d.dte = CAST(v.order_ts AS DATE)
LEFT JOIN dim_currency cur 
       ON cur.currency_code = v.currency
LEFT JOIN dim_status s 
       ON s.order_status = v.order_status 
      AND s.courier_status = v.courier_status
LEFT JOIN dim_channel c 
       ON c.sales_channel = v.sales_channel
      AND c.fulfilment = v.fulfilment
      AND c.fulfilled_by = v.fulfilled_by
      AND c.ship_service_level = v.ship_service_level
      AND ( (c.is_b2b IS NULL AND v.is_b2b IS NULL) OR c.is_b2b = v.is_b2b )
LEFT JOIN dim_customer_location l 
       ON l.ship_city = v.ship_city
      AND l.ship_state = v.ship_state
      AND l.ship_postal = v.ship_postal_code
      AND l.ship_country = v.ship_country
LEFT JOIN dim_product p 
       ON ISNULL(p.asin,'') = ISNULL(v.asin,'')
      AND ISNULL(p.sku,'') = ISNULL(v.sku,'')
      AND ISNULL(p.style,'') = ISNULL(v.style,'')
      AND ISNULL(p.category,'') = ISNULL(v.category,'')
      AND ISNULL(p.size,'') = ISNULL(v.size,'');

-- 8) Helpful indexes (SQL Server syntax)
CREATE INDEX idx_fact_order_id        ON fact_order_lines (order_id);
CREATE INDEX idx_fact_date_id         ON fact_order_lines (date_id);
CREATE INDEX idx_fact_product_id      ON fact_order_lines (product_id);
CREATE INDEX idx_fact_customer_loc_id ON fact_order_lines (customer_loc_id);
CREATE INDEX idx_fact_channel_id      ON fact_order_lines (channel_id);
CREATE INDEX idx_fact_status_id       ON fact_order_lines (status_id);
