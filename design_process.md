# design_process.md

## Goals & Questions
- Top products by sales volume and revenue
- Best time of day to run promotions

## Star Schema & Grain
**Grain:** order item.  
**Dims:** product, customer, date (yyyymmdd), time (minute-level).  
**Fact:** quantities, unit price in original currency, FX rate → derived USD columns.  
**FX dim:** daily currency→USD rates keyed by (date, currency) for stable analytics.

## Why this model
- Exact sums without double counting (item-level grain)
- Simple joins for Top-N product lists and hour-of-day revenue
- Scales to category, geography, cohorts, A/B test flags later

## Airflow DAG
`ecommerce_dw_etl`:
1. create_dw_schema
2. upsert_dim_customer
3. upsert_dim_product
4. fetch_daily_fx_rates (API: latest/{base}; we store per-run)
5. load_fact_sales_item (idempotent upserts; refreshes MV)

## Assumptions
- `orders` contains `order_timestamp` and `currency`.
- If FX missing: 1.0 for USD; otherwise 1.0 fallback (acceptable for challenge). Historical FX can be improved.

## Dashboard sketch
- KPI row: Revenue (USD), Units, AOV
- Top products: bar (toggle units vs revenue)
- Revenue by hour: column (0..23)
- Category mix: 100% stacked
- Optional: Revenue by country

## Next if time allowed
SCD2 for dims, proper historical FX by order date, unit tests / Great Expectations, date partitioning, anomaly alerts.
