# analytics/make_charts.py
# Runs end-to-end:
# 1) (Re)creates DW views (base/enriched/clean)
# 2) Executes analytics queries
# 3) Saves charts & CSVs under analytics/figures/
#
# Requires: pandas, matplotlib, SQLAlchemy, psycopg2-binary
# Python 3.8 compatible (no PEP 604 | unions)

import os
import shutil
from pathlib import Path
from typing import Optional, Dict, List

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

ENGINE_URL = "postgresql+psycopg2://{u}:{p}@{h}:{o}/{d}".format(
    u=PG_USER, p=PG_PASS, h=PG_HOST, o=PG_PORT, d=PG_DB
)
eng = create_engine(ENGINE_URL, pool_pre_ping=True)

# Fresh output dir each run
if OUT_DIR.exists():
    shutil.rmtree(OUT_DIR)
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

Q_REVENUE_BY_CATEGORY = """
WITH params AS (
  SELECT DATE %(start)s AS start_date, DATE %(end)s AS end_date
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

Q_REVENUE_BY_HOUR = """
WITH params AS (
  SELECT DATE %(start)s AS start_date, DATE %(end)s AS end_date
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

Q_DAILY_TREND = """
WITH params AS (
  SELECT DATE %(start)s AS start_date, DATE %(end)s AS end_date
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

Q_WEEKDAY_HOUR = """
WITH params AS (
  SELECT DATE %(start)s AS start_date, DATE %(end)s AS end_date
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

Q_TOP_CUSTOMERS = """
WITH params AS (
  SELECT DATE %(start)s AS start_date, DATE %(end)s AS end_date
)
SELECT
  COALESCE(NULLIF(TRIM(c.full_name), ''), 'Unknown') AS customer_name,
  COALESCE(NULLIF(TRIM(c.country), ''), 'Unknown')   AS country,
  COUNT(DISTINCT s.order_id)                         AS orders,
  SUM(s.line_amount_usd)                             AS revenue
FROM dw.vw_sales_clean s
LEFT JOIN dw.dim_customer c ON c.customer_id = s.customer_id
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
GROUP BY customer_name, country
ORDER BY revenue DESC
LIMIT 10;
"""

Q_MOM_BY_CATEGORY = """
WITH params AS (
  SELECT DATE %(start)s AS start_date, DATE %(end)s AS end_date
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

Q_FX_COVERAGE = """
WITH params AS (
  SELECT DATE %(start)s AS start_date, DATE %(end)s AS end_date
)
SELECT
  COUNT(*)                               AS lines_total,
  COUNT(*) FILTER (WHERE is_fx_fallback) AS lines_fx_fallback
FROM dw.vw_sales_clean s
CROSS JOIN params p
WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date;
"""

Q_AVG_REVENUE_PER_ORDER_BY_COUNTRY = """
WITH params AS (
  SELECT DATE %(start)s AS start_date, DATE %(end)s AS end_date
),
base AS (
  SELECT
    s.order_id,
    COALESCE(NULLIF(TRIM(c.country), ''), 'Unknown') AS country,
    SUM(s.line_amount_usd)                            AS order_revenue
  FROM dw.vw_sales_clean s
  LEFT JOIN dw.dim_customer c ON c.customer_id = s.customer_id
  CROSS JOIN params p
  WHERE to_date(s.date_key::text,'YYYYMMDD') BETWEEN p.start_date AND p.end_date
  GROUP BY s.order_id, country
)
SELECT country, AVG(order_revenue) AS avg_order_value
FROM base
GROUP BY country
ORDER BY avg_order_value DESC;
"""

# -----------------------
# Helpers
# -----------------------
def run_df(sql: str, params: Optional[Dict] = None) -> pd.DataFrame:
    return pd.read_sql(sql, eng, params=params or {"start": START_DATE, "end": END_DATE})

def ensure_views() -> None:
    with eng.begin() as conn:
        for stmt in VIEWS_SQL.split(";"):
            s = stmt.strip()
            if not s:
                continue
            conn.execute(text(s + ";"))

def save_csv(df: pd.DataFrame, name: str) -> Path:
    p = OUT_DIR / f"{name}.csv"
    df.to_csv(p, index=False)
    return p

def fig_save(name: str) -> Path:
    p = OUT_DIR / f"{name}.png"
    plt.tight_layout()
    plt.savefig(p, dpi=130, bbox_inches="tight")
    plt.close()
    return p

# -----------------------
# Charts
# -----------------------
def chart_revenue_by_category() -> Optional[Path]:
    df = run_df(Q_REVENUE_BY_CATEGORY)
    save_csv(df, "revenue_by_category")
    if df.empty:
        return None
    plt.figure()
    plt.bar(df["product_category"], df["revenue"])
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("Revenue (USD)")
    plt.title("Revenue by Category ({s} .. {e})".format(s=START_DATE, e=END_DATE))
    return fig_save("revenue_by_category")

def chart_revenue_by_hour() -> Optional[Path]:
    df = run_df(Q_REVENUE_BY_HOUR)
    save_csv(df, "revenue_by_hour")
    if df.empty:
        return None
    plt.figure()
    plt.bar(df["hour_of_day"], df["revenue"])
    plt.xlabel("Hour of Day")
    plt.ylabel("Revenue (USD)")
    plt.title("Revenue by Hour ({s} .. {e})".format(s=START_DATE, e=END_DATE))
    return fig_save("revenue_by_hour")

def chart_daily_trend_ma() -> Optional[Path]:
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
    plt.title("Daily Revenue & 7-day MA ({s} .. {e})".format(s=START_DATE, e=END_DATE))
    return fig_save("daily_revenue_7dma")

def chart_daily_cumulative() -> Optional[Path]:
    df = run_df(Q_DAILY_TREND)
    if df.empty:
        return None
    df["cumulative_revenue"] = df["revenue"].cumsum()
    save_csv(df[["sales_date","cumulative_revenue"]], "daily_revenue_cumulative")
    plt.figure()
    plt.plot(df["sales_date"], df["cumulative_revenue"])
    plt.ylabel("Cumulative Revenue (USD)")
    plt.title("Cumulative Revenue ({s} .. {e})".format(s=START_DATE, e=END_DATE))
    return fig_save("daily_revenue_cumulative")

def chart_weekday_hour_heatmap() -> Optional[Path]:
    df = run_df(Q_WEEKDAY_HOUR)
    if df.empty:
        return None
    pivot = df.pivot_table(index="weekday_iso", columns="hour_of_day", values="revenue", aggfunc="sum").fillna(0.0)
    pivot = pivot.reindex(index=[1,2,3,4,5,6,7], columns=list(range(24)), fill_value=0.0)
    plt.figure()
    im = plt.imshow(pivot.values, aspect="auto", origin="upper")
    plt.colorbar(im, label="Revenue (USD)")
    plt.yticks(range(7), ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"])
    plt.xticks(range(0,24,2))
    plt.xlabel("Hour of Day")
    plt.title("Revenue Heatmap (Weekday × Hour) {s} .. {e}".format(s=START_DATE, e=END_DATE))
    return fig_save("heatmap_weekday_hour")

def _clean_label_series(name_ser: pd.Series, country_ser: pd.Series) -> List[str]:
    name = name_ser.fillna("Unknown").astype(str).str.strip()
    # squash any "~unknown ..." or "unknown..." to a clean token
    name = name.str.replace(r"^\s*~?\s*unknown.*$", "Unknown Customer", regex=True, case=False)
    country = country_ser.fillna("Unknown").astype(str).str.strip().str.title()
    return (name + " — " + country).tolist()

def chart_top_customers() -> Optional[Path]:
    df = run_df(Q_TOP_CUSTOMERS)
    save_csv(df, "top_customers")
    if df.empty:
        return None
    labels = _clean_label_series(df["customer_name"], df["country"])
    plt.figure()
    plt.barh(labels[::-1], df["revenue"][::-1])
    plt.xlabel("Revenue (USD)")
    plt.title("Top 10 Customers by Revenue ({s} .. {e})".format(s=START_DATE, e=END_DATE))
    return fig_save("top_customers")

def chart_monthly_revenue_by_category_bars() -> Optional[Path]:
    # grouped bars for top 5 categories across months (easier than lines)
    df = run_df(Q_MOM_BY_CATEGORY)
    if df.empty:
        return None
    totals = df.groupby("product_category")["revenue"].sum().sort_values(ascending=False)
    top5 = totals.head(5).index.tolist()
    sub = df[df["product_category"].isin(top5)].copy()
    piv = sub.pivot_table(index="month", columns="product_category", values="revenue", aggfunc="sum").fillna(0.0)
    piv = piv.sort_index()
    save_csv(piv.reset_index(), "mom_by_category_top5_bars")
    ax = piv.plot(kind="bar", figsize=(10,5))
    ax.set_ylabel("Revenue (USD)")
    ax.set_title("Monthly Revenue by Category — Top 5 ({s} .. {e})".format(s=START_DATE, e=END_DATE))
    plt.xticks(rotation=30, ha="right")
    return fig_save("mom_by_category_top5_bars")

def chart_fx_coverage() -> Optional[Path]:
    df = run_df(Q_FX_COVERAGE)
    if df.empty:
        return None
    total = int(df.loc[0, "lines_total"])
    fbk   = int(df.loc[0, "lines_fx_fallback"])
    ok    = max(total - fbk, 0)
    plt.figure()
    plt.bar(["With FX rate","Fallback"], [ok, fbk])
    plt.ylabel("Rows")
    plt.title("FX Coverage ({s} .. {e}) — total rows: {t}".format(s=START_DATE, e=END_DATE, t=total))
    return fig_save("fx_coverage")

def chart_avg_revenue_per_order_by_country() -> Optional[Path]:
    df = run_df(Q_AVG_REVENUE_PER_ORDER_BY_COUNTRY)
    save_csv(df, "avg_order_value_by_country")
    if df.empty:
        return None
    plt.figure()
    plt.bar(df["country"], df["avg_order_value"])
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("Avg Revenue per Order (USD)")
    plt.title("Avg Revenue per Order by Country ({s} .. {e})".format(s=START_DATE, e=END_DATE))
    return fig_save("avg_order_value_by_country")

# -----------------------
# Main
# -----------------------
def main() -> None:
    print("Connecting:", ENGINE_URL)
    ensure_views()
    print("Views ensured.")

    outputs = [
        chart_revenue_by_category(),
        chart_revenue_by_hour(),
        chart_daily_trend_ma(),
        chart_daily_cumulative(),                      # NEW
        chart_weekday_hour_heatmap(),
        chart_top_customers(),
        chart_monthly_revenue_by_category_bars(),      # CHANGED to bars
        chart_fx_coverage(),
        chart_avg_revenue_per_order_by_country(),      # NEW
    ]
    outputs = [p for p in outputs if p]

    print("CSV & charts written:")
    for p in sorted(OUT_DIR.glob("*.csv")):
        print(" -", p)
    for p in outputs:
        print(" -", p)

if __name__ == "__main__":
    main()
