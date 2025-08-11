# import os
# import pandas as pd
# from sqlalchemy import create_engine
# import matplotlib.pyplot as plt

# DW_URL = os.getenv(
#     "DW_DB_URL",
#     "postgresql+psycopg2://postgres:postgres@localhost:5434/data_warehouse"
# )

# eng = create_engine(DW_URL, pool_pre_ping=True)

# # Revenue by hour
# q_hour = """
# WITH params AS (
#   SELECT DATE '2024-01-01' AS start_date, DATE '2024-12-31' AS end_date
# )
# SELECT FLOOR(s.time_key / 60.0)::int AS hour_of_day,
#        SUM(s.line_amount_usd) AS revenue,
#        SUM(s.quantity)        AS units
# FROM dw.vw_sales_clean s
# CROSS JOIN params p
# WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
# GROUP BY hour_of_day
# ORDER BY hour_of_day;
# """
# df_hour = pd.read_sql(q_hour, eng)

# plt.figure()
# plt.bar(df_hour["hour_of_day"], df_hour["revenue"])
# plt.title("Revenue by Hour (USD)")
# plt.xlabel("Hour of Day")
# plt.ylabel("Revenue")
# plt.tight_layout()
# os.makedirs("analytics/figures", exist_ok=True)
# plt.savefig("analytics/figures/revenue_by_hour.png", dpi=160)

# # Revenue by category
# q_cat = """
# WITH params AS (
#   SELECT DATE '2024-01-01' AS start_date, DATE '2024-12-31' AS end_date
# )
# SELECT s.product_category, SUM(s.line_amount_usd) AS revenue
# FROM dw.vw_sales_clean s
# CROSS JOIN params p
# WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
# GROUP BY s.product_category
# ORDER BY revenue DESC;
# """
# df_cat = pd.read_sql(q_cat, eng)

# plt.figure()
# plt.bar(df_cat["product_category"], df_cat["revenue"])
# plt.title("Revenue by Category (USD)")
# plt.xlabel("Category")
# plt.ylabel("Revenue")
# plt.xticks(rotation=30, ha="right")
# plt.tight_layout()
# plt.savefig("analytics/figures/revenue_by_category.png", dpi=160)
# print("Charts saved under analytics/figures/")




# analytics/make_charts.py
# Runs end-to-end:
# 1) (Re)creates DW views (base/enriched/clean)
# 2) Executes analytics queries
# 3) Saves charts & CSVs under analytics/figures/
#
# Requires: pandas, matplotlib, SQLAlchemy, psycopg2-binary

import os
from pathlib import Path
from datetime import date

import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

# -----------------------
# Config (env overrides)
# -----------------------
PG_USER = os.getenv("DW_PG_USER", "postgres")
PG_PASS = os.getenv("DW_PG_PASSWORD", "postgres")
PG_HOST = os.getenv("DW_PG_HOST", "data_warehouse")
PG_PORT = int(os.getenv("DW_PG_PORT", "5432"))
PG_DB   = os.getenv("DW_PG_DB", "data_warehouse")

START_DATE = os.getenv("AN_START_DATE", "2024-01-01")
END_DATE   = os.getenv("AN_END_DATE",   "2024-12-31")

OUT_DIR = Path(os.getenv("AN_OUT_DIR", "/opt/airflow/analytics/figures")).resolve()

ENGINE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
eng = create_engine(ENGINE_URL, pool_pre_ping=True)

# Make output dir
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------
# SQL blocks
# -----------------------
VIEWS_SQL = """
-- Remove old views (idempotent)
DROP VIEW IF EXISTS dw.vw_sales_clean;
DROP VIEW IF EXISTS dw.vw_sales_enriched;
DROP VIEW IF EXISTS dw.vw_sales_base;

-- Base fact view with flags
CREATE OR REPLACE VIEW dw.vw_sales_base AS
SELECT
  f.*,
  (f.currency NOT IN ('USD','EUR','GBP'))          AS is_non_iso_currency,
  (f.fx_rate_to_usd = 1.0 AND f.currency <> 'USD') AS is_fx_fallback
FROM dw.fact_sales_item f;

-- Enriched with product attributes
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

-- Clean view: drop non-ISO currencies (keep EUR/GBP/USD)
CREATE OR REPLACE VIEW dw.vw_sales_clean AS
SELECT *
FROM dw.vw_sales_enriched
WHERE is_non_iso_currency = FALSE;
"""

Q_REVENUE_BY_CATEGORY = f"""
WITH params AS (
  SELECT DATE '{START_DATE}' AS start_date, DATE '{END_DATE}' AS end_date
)
SELECT
  s.product_category,
  SUM(s.line_amount_usd) AS revenue,
  SUM(s.quantity)        AS units
FROM dw.vw_sales_clean s
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
GROUP BY s.product_category
ORDER BY revenue DESC;
"""

Q_REVENUE_BY_HOUR = f"""
WITH params AS (
  SELECT DATE '{START_DATE}' AS start_date, DATE '{END_DATE}' AS end_date
)
SELECT
  FLOOR(s.time_key / 60.0)::int AS hour_of_day,
  SUM(s.line_amount_usd)        AS revenue,
  SUM(s.quantity)               AS units
FROM dw.vw_sales_clean s
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
GROUP BY hour_of_day
ORDER BY hour_of_day;
"""

Q_DAILY_TREND = f"""
WITH params AS (
  SELECT DATE '{START_DATE}' AS start_date, DATE '{END_DATE}' AS end_date
),
daily AS (
  SELECT
    to_date(s.date_key::text,'YYYYMMDD') AS sales_date,
    SUM(s.line_amount_usd)                AS revenue
  FROM dw.vw_sales_clean s
  CROSS JOIN params p
  WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
  GROUP BY sales_date
)
SELECT * FROM daily ORDER BY sales_date;
"""

Q_WEEKDAY_HOUR = f"""
WITH params AS (
  SELECT DATE '{START_DATE}' AS start_date, DATE '{END_DATE}' AS end_date
)
SELECT
  EXTRACT(ISODOW FROM to_date(s.date_key::text,'YYYYMMDD'))::int AS weekday_iso, -- 1=Mon..7=Sun
  FLOOR(s.time_key/60.0)::int                                    AS hour_of_day,
  SUM(s.line_amount_usd)                                         AS revenue
FROM dw.vw_sales_clean s
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
GROUP BY weekday_iso, hour_of_day
ORDER BY weekday_iso, hour_of_day;
"""

Q_TOP_CUSTOMERS = f"""
WITH params AS (
  SELECT DATE '{START_DATE}' AS start_date, DATE '{END_DATE}' AS end_date
)
SELECT
  COALESCE(c.full_name, 'Unknown') AS customer_name,
  COALESCE(c.country, 'Unknown')   AS country,
  COUNT(DISTINCT s.order_id)       AS orders,
  SUM(s.line_amount_usd)           AS revenue
FROM dw.vw_sales_clean s
LEFT JOIN dw.dim_customer c ON c.customer_id = s.customer_id
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
GROUP BY customer_name, country
ORDER BY revenue DESC
LIMIT 10;
"""

Q_MOM_BY_CATEGORY = f"""
WITH params AS (
  SELECT DATE '{START_DATE}' AS start_date, DATE '{END_DATE}' AS end_date
),
by_month AS (
  SELECT
    DATE_TRUNC('month', to_date(s.date_key::text,'YYYYMMDD'))::date AS month,
    s.product_category,
    SUM(s.line_amount_usd) AS revenue
  FROM dw.vw_sales_clean s
  CROSS JOIN params p
  WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
  GROUP BY month, s.product_category
)
SELECT product_category, month, revenue
FROM by_month
ORDER BY product_category, month;
"""

Q_FX_COVERAGE = f"""
WITH params AS (
  SELECT DATE '{START_DATE}' AS start_date, DATE '{END_DATE}' AS end_date
)
SELECT
  COUNT(*)                               AS lines_total,
  COUNT(*) FILTER (WHERE is_fx_fallback) AS lines_fx_fallback
FROM dw.vw_sales_clean s
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date;
"""

def run_df(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, eng)

def ensure_views():
    with eng.begin() as conn:
        for stmt in VIEWS_SQL.split(";"):
            s = stmt.strip()
            if not s:
                continue
            conn.execute(text(s + ";"))

def save_csv(df: pd.DataFrame, name: str):
    p = OUT_DIR / f"{name}.csv"
    df.to_csv(p, index=False)
    return p

def fig_save(name: str):
    p = OUT_DIR / f"{name}.png"
    plt.tight_layout()
    plt.savefig(p, dpi=130, bbox_inches="tight")
    plt.close()
    return p

# -----------------------
# Charts
# -----------------------
def chart_revenue_by_category():
    df = run_df(Q_REVENUE_BY_CATEGORY)
    save_csv(df, "revenue_by_category")
    plt.figure()
    plt.bar(df["product_category"], df["revenue"])
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("Revenue (USD)")
    plt.title(f"Revenue by Category ({START_DATE} .. {END_DATE})")
    return fig_save("revenue_by_category")

def chart_revenue_by_hour():
    df = run_df(Q_REVENUE_BY_HOUR)
    save_csv(df, "revenue_by_hour")
    plt.figure()
    plt.bar(df["hour_of_day"], df["revenue"])
    plt.xlabel("Hour of Day")
    plt.ylabel("Revenue (USD)")
    plt.title(f"Revenue by Hour ({START_DATE} .. {END_DATE})")
    return fig_save("revenue_by_hour")

def chart_daily_trend_ma():
    df = run_df(Q_DAILY_TREND)
    if df.empty:
        return None
    df["revenue_7d_ma"] = df["revenue"].rolling(7, min_periods=1).mean()
    save_csv(df, "daily_revenue_with_ma")
    plt.figure()
    plt.plot(df["sales_date"], df["revenue"], label="Daily revenue")
    plt.plot(df["sales_date"], df["revenue_7d_ma"], label="7-day MA")
    plt.legend()
    plt.ylabel("Revenue (USD)")
    plt.title(f"Daily Revenue & 7-day MA ({START_DATE} .. {END_DATE})")
    return fig_save("daily_revenue_7dma")

def chart_weekday_hour_heatmap():
    df = run_df(Q_WEEKDAY_HOUR)
    if df.empty:
        return None
    pivot = df.pivot_table(index="weekday_iso", columns="hour_of_day", values="revenue", aggfunc="sum").fillna(0.0)
    # Ensure full grid 1..7 x 0..23
    pivot = pivot.reindex(index=[1,2,3,4,5,6,7], columns=list(range(24)), fill_value=0.0)

    plt.figure()
    im = plt.imshow(pivot.values, aspect="auto", origin="upper")
    plt.colorbar(im, label="Revenue (USD)")
    plt.yticks(range(7), ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"])
    plt.xticks(range(0,24,2))
    plt.xlabel("Hour of Day")
    plt.title(f"Revenue Heatmap (Weekday × Hour) {START_DATE} .. {END_DATE}")
    return fig_save("heatmap_weekday_hour")

def chart_top_customers():
    df = run_df(Q_TOP_CUSTOMERS)
    save_csv(df, "top_customers")
    if df.empty:
        return None
    labels = (df["customer_name"] + " (" + df["country"] + ")").tolist()
    plt.figure()
    plt.barh(labels[::-1], df["revenue"][::-1])
    plt.xlabel("Revenue (USD)")
    plt.title(f"Top 10 Customers by Revenue ({START_DATE} .. {END_DATE})")
    return fig_save("top_customers")

def chart_mom_by_category():
    df = run_df(Q_MOM_BY_CATEGORY)
    if df.empty:
        return None
    # Pick top 5 categories by total revenue to avoid spaghetti chart
    top5 = (df.groupby("product_category")["revenue"]
              .sum()
              .sort_values(ascending=False)
              .head(5)
              .index.tolist())
    plt.figure()
    for cat in top5:
        sub = df[df["product_category"] == cat]
        plt.plot(sub["month"], sub["revenue"], label=cat)
    plt.legend()
    plt.ylabel("Revenue (USD)")
    plt.title(f"Monthly Revenue by Category — top 5 ({START_DATE} .. {END_DATE})")
    return fig_save("mom_by_category_top5")

def chart_fx_coverage():
    df = run_df(Q_FX_COVERAGE)
    if df.empty:
        return None
    total = int(df.loc[0, "lines_total"])
    fbk   = int(df.loc[0, "lines_fx_fallback"])
    ok    = max(total - fbk, 0)
    plt.figure()
    plt.bar(["With FX rate","Fallback"], [ok, fbk])
    plt.ylabel("Rows")
    plt.title(f"FX Coverage ({START_DATE} .. {END_DATE}) — total rows: {total}")
    return fig_save("fx_coverage")

# -----------------------
# Main
# -----------------------
def main():
    print(f"Connecting: {ENGINE_URL}")
    ensure_views()
    print("Views ensured.")

    outputs = []
    outputs.append(chart_revenue_by_category())
    outputs.append(chart_revenue_by_hour())
    outputs.append(chart_daily_trend_ma())
    outputs.append(chart_weekday_hour_heatmap())
    outputs.append(chart_top_customers())
    outputs.append(chart_mom_by_category())
    outputs.append(chart_fx_coverage())

    outputs = [p for p in outputs if p]
    print("CSV & charts written:")
    for p in sorted(OUT_DIR.glob("*.csv")):
        print(" -", p)
    for p in outputs:
        print(" -", p)

if __name__ == "__main__":
    main()
