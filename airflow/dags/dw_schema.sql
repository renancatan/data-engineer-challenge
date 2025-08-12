-- ==============================================
-- Data Warehouse Star Schema (PostgreSQL)
-- Grain: one row per order_item
-- Base currency: USD
-- ==============================================

CREATE SCHEMA IF NOT EXISTS dw;

-- ---------------------------
-- Dimensions
-- ---------------------------
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key        INTEGER PRIMARY KEY,          -- yyyymmdd
    "date"          DATE NOT NULL,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    day             SMALLINT NOT NULL,
    day_of_week     SMALLINT NOT NULL,            -- 1=Mon..7=Sun
    is_weekend      BOOLEAN NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_dim_date_date ON dw.dim_date("date");

CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_key        SMALLINT PRIMARY KEY,         -- minute of day (0..1439)
    "hour"          SMALLINT NOT NULL CHECK ("hour" BETWEEN 0 AND 23),
    minute          SMALLINT NOT NULL CHECK (minute BETWEEN 0 AND 59)
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_dim_time_hour_minute ON dw.dim_time("hour", minute);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_sk     BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT UNIQUE NOT NULL,
    full_name       TEXT,
    email           TEXT,
    country         TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_sk      BIGSERIAL PRIMARY KEY,
    product_id      BIGINT UNIQUE NOT NULL,
    name            TEXT,
    category        TEXT,
    description     TEXT
);

-- Daily FX rates to USD (one row per date x currency)
CREATE TABLE IF NOT EXISTS dw.dim_fx_rate (
    fx_date         DATE NOT NULL,
    currency        CHAR(3) NOT NULL,
    rate_to_usd     NUMERIC(18,8) NOT NULL,       -- multiply original_amount * rate_to_usd
    source          TEXT,
    fetched_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (fx_date, currency)
);

-- ---------------------------
-- Fact: one row per order_item
-- ---------------------------
CREATE TABLE IF NOT EXISTS dw.fact_sales_item (
    sales_item_sk   BIGSERIAL PRIMARY KEY,
    order_item_id   BIGINT NOT NULL UNIQUE,
    order_id        BIGINT NOT NULL,
    product_id      BIGINT NOT NULL,
    customer_id     BIGINT NOT NULL,

    date_key        INTEGER NOT NULL REFERENCES dw.dim_date(date_key),
    time_key        SMALLINT NOT NULL REFERENCES dw.dim_time(time_key),

    quantity        NUMERIC(18,4) NOT NULL,
    unit_price_orig NUMERIC(18,4) NOT NULL,
    currency        CHAR(3) NOT NULL,

    fx_rate_to_usd  NUMERIC(18,8) NOT NULL,
    unit_price_usd  NUMERIC(18,4) GENERATED ALWAYS AS (unit_price_orig * fx_rate_to_usd) STORED,
    line_amount_usd NUMERIC(18,4) GENERATED ALWAYS AS (quantity * unit_price_orig * fx_rate_to_usd) STORED,

    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_fact_sales_item_date_key ON dw.fact_sales_item(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_item_time_key ON dw.fact_sales_item(time_key);
CREATE INDEX IF NOT EXISTS ix_fact_sales_item_product ON dw.fact_sales_item(product_id);
CREATE INDEX IF NOT EXISTS ix_fact_sales_item_customer ON dw.fact_sales_item(customer_id);

-- ---------------------------
-- Fast aggregate for promo timing
-- ---------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS dw.mv_hourly_sales AS
SELECT
    d."date",
    t."hour",
    SUM(f.line_amount_usd) AS revenue_usd,
    COUNT(*)               AS items_count
FROM dw.fact_sales_item f
JOIN dw.dim_date d  ON d.date_key = f.date_key
JOIN dw.dim_time t  ON t.time_key = f.time_key
GROUP BY d."date", t."hour";
CREATE INDEX IF NOT EXISTS ix_mv_hourly_sales_date_hour ON dw.mv_hourly_sales("date", "hour");

-- ---------------------------
-- Seed time & date dimensions
-- ---------------------------
-- time: 24*60 = 1440 rows
INSERT INTO dw.dim_time(time_key, "hour", minute)
SELECT (h*60 + m) AS time_key, h AS "hour", m AS minute
FROM generate_series(0,23) AS h
CROSS JOIN generate_series(0,59) AS m
ON CONFLICT (time_key) DO NOTHING;

-- date: last 5y + 1y future for safety
WITH d AS (
  SELECT gs::date AS dt
  FROM generate_series((CURRENT_DATE - INTERVAL '5 years')::date,
                       (CURRENT_DATE + INTERVAL '365 days')::date,
                       INTERVAL '1 day') gs
)
INSERT INTO dw.dim_date(date_key, "date", year, quarter, month, day, day_of_week, is_weekend)
SELECT
  EXTRACT(YEAR FROM dt)::int * 10000 + EXTRACT(MONTH FROM dt)::int * 100 + EXTRACT(DAY FROM dt)::int AS date_key,
  dt AS "date",
  EXTRACT(YEAR FROM dt)::int,
  EXTRACT(QUARTER FROM dt)::int,
  EXTRACT(MONTH FROM dt)::int,
  EXTRACT(DAY FROM dt)::int,
  EXTRACT(ISODOW FROM dt)::int,
  (EXTRACT(ISODOW FROM dt)::int IN (6,7))::boolean
FROM d
ON CONFLICT (date_key) DO NOTHING;
