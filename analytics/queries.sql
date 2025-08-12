--remove old views
DROP VIEW IF EXISTS dw.vw_sales_clean;
DROP VIEW IF EXISTS dw.vw_sales_enriched;
DROP VIEW IF EXISTS dw.vw_sales_base;

-- filter at the DW layer
-- 1) Base fact view with flags
CREATE OR REPLACE VIEW dw.vw_sales_base AS
SELECT
  f.*,
  -- Flags
  (f.currency NOT IN ('USD','EUR','GBP'))          AS is_non_iso_currency,
  (f.fx_rate_to_usd = 1.0 AND f.currency <> 'USD') AS is_fx_fallback
FROM dw.fact_sales_item f;

-- 2) Enriched with product attributes
CREATE OR REPLACE VIEW dw.vw_sales_enriched AS
SELECT
  b.order_item_id, b.order_id, b.product_id, b.customer_id,
  b.date_key, b.time_key, b.quantity, b.unit_price_orig, b.currency,
  b.fx_rate_to_usd, b.unit_price_usd, b.line_amount_usd,
  b.is_non_iso_currency, b.is_fx_fallback,
  p.category AS product_category,
  p.name     AS product_name
FROM dw.vw_sales_base b
JOIN dw.dim_product p USING (product_id);

-- 3) “Clean” view = drop non-ISO rows (and choose whether to drop FX fallbacks)
-- (a) Drop only non-ISO currencies:
CREATE OR REPLACE VIEW dw.vw_sales_clean AS
SELECT *
FROM dw.vw_sales_enriched
WHERE is_non_iso_currency = FALSE;


-- Top products by revenue (date window + optional category filter)
WITH params AS (
  SELECT
    DATE '2024-01-01' AS start_date,
    DATE '2024-12-31' AS end_date,
    NULL::text[]      AS categories  -- e.g. ARRAY['Electronics','Books'] for a filter
)
SELECT
  s.product_category,
  s.product_name,
  SUM(s.quantity)        AS units,
  SUM(s.line_amount_usd) AS revenue
FROM dw.vw_sales_clean s
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
  AND (p.categories IS NULL OR s.product_category = ANY(p.categories))
GROUP BY s.product_category, s.product_name
ORDER BY revenue DESC
LIMIT 20;


-- Best hour of day for promos (by revenue)
WITH params AS (
  SELECT DATE '2024-01-01' AS start_date, DATE '2024-12-31' AS end_date
)
SELECT
  FLOOR(s.time_key / 60.0)::int AS hour_of_day,
  SUM(s.line_amount_usd)        AS revenue,
  SUM(s.quantity)               AS units
FROM dw.vw_sales_clean s
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
GROUP BY hour_of_day
ORDER BY revenue DESC;


-- Category performance

WITH params AS (
  SELECT DATE '2024-01-01' AS start_date, DATE '2024-12-31' AS end_date
)
SELECT
  s.product_category,
  SUM(s.quantity)        AS units,
  SUM(s.line_amount_usd) AS revenue
FROM dw.vw_sales_clean s
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
GROUP BY s.product_category
ORDER BY revenue DESC;

-- Top 10 products by revenue and units
SELECT
  p.name AS product_name,
  SUM(f.quantity)        AS units,
  SUM(f.line_amount_usd) AS revenue_usd
FROM dw.fact_sales_item f
JOIN dw.dim_product p USING (product_id)
GROUP BY p.name
ORDER BY revenue_usd DESC
LIMIT 10;

-- Currency quality check (should now be 0 non-ISO)
SELECT currency AS WRONG_CURRENCY, COUNT(*)
FROM dw.vw_sales_base
WHERE is_non_iso_currency = TRUE
GROUP BY currency
ORDER BY COUNT(*) DESC;

-- retrieved values from the API + code to check the latest FX rates and non real currency corrections
SELECT fx_date, currency, rate_to_usd, source, fetched_at
FROM dw.dim_fx_rate
ORDER BY fx_date DESC, currency
LIMIT 20;