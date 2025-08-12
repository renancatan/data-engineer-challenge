from __future__ import annotations
import os
from datetime import datetime, timedelta
from typing import List, Optional

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# --------------------------------------------------------------------
# Connection URLs (overridable via env)
# --------------------------------------------------------------------
def _pg_url_from_env(default: str, env_key: str) -> str:
    return os.environ.get(env_key, default)

ORDERS_URL   = _pg_url_from_env("postgresql+psycopg2://postgres:postgres@ecommerce_db1:5432/ecommerce_orders", "ORDERS_DB_URL")
PRODUCTS_URL = _pg_url_from_env("postgresql+psycopg2://postgres:postgres@ecommerce_db2:5432/ecommerce_products", "PRODUCTS_DB_URL")
DW_URL       = _pg_url_from_env("postgresql+psycopg2://postgres:postgres@data_warehouse:5432/data_warehouse", "DW_DB_URL")

# --------------------------------------------------------------------
# FX API config (v6.exchangerate-api.com)
#   Put in .env: FX_API_KEY=xxxxx, FX_BASE_CURRENCY=USD
# --------------------------------------------------------------------
FX_API_KEY        = os.environ.get("FX_API_KEY", "").strip()
FX_BASE_CURRENCY  = os.environ.get("FX_BASE_CURRENCY", "USD").strip().upper()
FX_PROVIDER_NAME  = "exchangerate-api.com"
FX_API_URL_TPL    = "https://v6.exchangerate-api.com/v6/{key}/latest/{base}"  # 1 <base> -> X <code>

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def _pick_col(engine, table: str, candidates: List[str]) -> str:
    q = text("""
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema IN ('public','dw') AND table_name = :t
    """)
    with engine.begin() as conn:
        cols = {r[0] for r in conn.execute(q, {"t": table}).fetchall()}
    for c in candidates:
        if c in cols:
            return c
    raise RuntimeError(f"No candidate column found in {table}. Tried: {candidates}")

# --------------------------------------------------------------------
# DDL
# --------------------------------------------------------------------
def create_dw_schema() -> None:
    engine = create_engine(DW_URL, pool_pre_ping=True)
    ddl_sql = Variable.get("dw_schema_sql", default_var=None)
    if not ddl_sql:
        for p in [
            "/opt/airflow/dags/dw_schema.sql",
            "/files/dw_schema.sql",
            "/opt/airflow/dags/database/dw_schema.sql",
            "/opt/airflow/dags/airflow/dags/dw_schema.sql",
        ]:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as fh:
                    ddl_sql = fh.read()
                break
    if not ddl_sql:
        raise RuntimeError("DDL SQL not found. Set Airflow Variable 'dw_schema_sql' or mount dw_schema.sql in DAGs.")
    with engine.begin() as conn:
        conn.execute(text(ddl_sql))

# --------------------------------------------------------------------
# Dimensions
# --------------------------------------------------------------------
def upsert_dim_customer() -> None:
    src = create_engine(ORDERS_URL, pool_pre_ping=True)
    tgt = create_engine(DW_URL, pool_pre_ping=True)

    cust_id_col = _pick_col(src, "customers", ["customer_id", "id"])
    name_candidates = []
    try:
        name_candidates.append(_pick_col(src, "customers", ["full_name"]))
    except Exception:
        pass
    first_name = None
    last_name = None
    try:
        first_name = _pick_col(src, "customers", ["first_name", "firstname"])
    except Exception:
        pass
    try:
        last_name = _pick_col(src, "customers", ["last_name", "lastname"])
    except Exception:
        pass
    email_col = None
    country_col = None
    try:
        email_col = _pick_col(src, "customers", ["email"])
    except Exception:
        pass
    try:
        country_col = _pick_col(src, "customers", ["country", "country_code", "nation"])
    except Exception:
        pass

    select_parts = [f"{cust_id_col} AS customer_id"]
    if "full_name" in name_candidates:
        select_parts.append("full_name")
    elif first_name or last_name:
        fn = first_name if first_name else "NULL"
        ln = last_name if last_name else "NULL"
        select_parts.append(f"CONCAT_WS(' ', {fn}, {ln}) AS full_name")
    else:
        select_parts.append("NULL::text AS full_name")

    select_parts.append(f"{email_col} AS email" if email_col else "NULL::text AS email")
    select_parts.append(f"{country_col} AS country" if country_col else "NULL::text AS country")

    df = pd.read_sql("SELECT " + ", ".join(select_parts) + " FROM customers", src)

    upsert_sql = text("""
        INSERT INTO dw.dim_customer(customer_id, full_name, email, country)
        VALUES (:customer_id, :full_name, :email, :country)
        ON CONFLICT (customer_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            email     = EXCLUDED.email,
            country   = EXCLUDED.country;
    """)
    with tgt.begin() as conn:
        conn.execute(text("SET search_path = dw, public"))
        for _, row in df.iterrows():
            conn.execute(upsert_sql, dict(
                customer_id=int(row["customer_id"]),
                full_name=row.get("full_name"),
                email=row.get("email"),
                country=row.get("country"),
            ))

def upsert_dim_product() -> None:
    src = create_engine(PRODUCTS_URL, pool_pre_ping=True)
    tgt = create_engine(DW_URL, pool_pre_ping=True)

    pid_col = _pick_col(src, "product_descriptions", ["product_id", "id"])
    name_col = _pick_col(src, "product_descriptions", ["name", "product_name"])
    category_col = _pick_col(src, "product_descriptions", ["category", "product_category"])
    try:
        desc_col = _pick_col(src, "product_descriptions", ["description", "details"])
        has_desc = True
    except Exception:
        desc_col = None
        has_desc = False

    sql = f"""
        SELECT
          {pid_col} AS product_id,
          {name_col} AS name,
          {category_col} AS category,
          {desc_col} AS description
        FROM product_descriptions
    """ if has_desc else f"""
        SELECT
          {pid_col} AS product_id,
          {name_col} AS name,
          {category_col} AS category,
          NULL::text AS description
        FROM product_descriptions
    """

    df = pd.read_sql(sql, src)

    upsert_sql = text("""
        INSERT INTO dw.dim_product(product_id, name, category, description)
        VALUES (:product_id, :name, :category, :description)
        ON CONFLICT (product_id) DO UPDATE SET
            name        = EXCLUDED.name,
            category    = EXCLUDED.category,
            description = EXCLUDED.description;
    """)
    with tgt.begin() as conn:
        conn.execute(text("SET search_path = dw, public"))
        for _, row in df.iterrows():
            conn.execute(upsert_sql, dict(
                product_id=int(row["product_id"]),
                name=row.get("name"),
                category=row.get("category"),
                description=row.get("description"),
            ))

# --------------------------------------------------------------------
# FX + Fact load
# --------------------------------------------------------------------
def _fetch_base_rates() -> tuple[dict, str]:
    """
    Call v6.exchangerate-api with FX_BASE_CURRENCY and return ('conversion_rates', base).
    conversion_rates: 1 <base> -> X <code>
    """
    if not FX_API_KEY:
        raise ValueError("FX_API_KEY is missing; add it to your .env and ensure compose uses env_file.")
    base = FX_BASE_CURRENCY or "USD"
    url = FX_API_URL_TPL.format(key=FX_API_KEY, base=base)
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("result") != "success":
        raise RuntimeError(f"FX API error: {payload}")
    return payload.get("conversion_rates", {}), base

def fetch_daily_fx_rates(execution_date_str: Optional[str] = None) -> None:
    """
    Populate dw.dim_fx_rate with one row per (date, currency).
    - If execution_date_str is provided, only that date is written.
    - Otherwise, min..max order dates are backfilled.
    - Rates are normalized to USD even if FX_BASE_CURRENCY != USD.
    """
    orders_eng = create_engine(ORDERS_URL, pool_pre_ping=True)
    dw_eng     = create_engine(DW_URL, pool_pre_ping=True)

    ts_col = _pick_col(orders_eng, "orders", ["order_timestamp", "order_datetime", "created_at", "ordered_at", "order_date"])
    cur_col = _pick_col(orders_eng, "orders", ["currency", "currency_code"])

    if execution_date_str:
        start_d = end_d = pd.to_datetime(execution_date_str).date()
    else:
        mm = pd.read_sql(f"SELECT MIN({ts_col}) AS min_ts, MAX({ts_col}) AS max_ts FROM orders", orders_eng)
        if mm.empty or pd.isna(mm.loc[0, "min_ts"]) or pd.isna(mm.loc[0, "max_ts"]):
            print("No orders found — skipping FX.")
            return
        start_d = pd.to_datetime(mm.loc[0, "min_ts"]).date()
        end_d   = pd.to_datetime(mm.loc[0, "max_ts"]).date()

    cur_df = pd.read_sql(f"SELECT DISTINCT {cur_col} AS currency FROM orders", orders_eng)
    currencies = {str(c).upper()[:3] for c in cur_df["currency"].dropna().tolist()}
    currencies.update({"USD", "EUR", "GBP"})
    currencies = sorted(currencies)

    base_rates, base = _fetch_base_rates()

    if base == "USD":
        # 1 USD -> x code  ⇒  1 code -> (1/x) USD
        def code_to_usd(code: str) -> float:
            if code == "USD": return 1.0
            x = base_rates.get(code)
            return 1.0 / float(x) if x else 1.0
    else:
        # 1 base -> x USD and 1 base -> y code ⇒ 1 code -> (x/y) USD
        usd_per_base = float(base_rates.get("USD", 0)) or 0.0
        def code_to_usd(code: str) -> float:
            if code == "USD": return 1.0
            y = base_rates.get(code)
            if not usd_per_base or not y: return 1.0
            return usd_per_base / float(y)

    with dw_eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.dim_fx_rate (
              fx_date      DATE                      NOT NULL,
              currency     CHAR(3)                   NOT NULL,
              rate_to_usd  NUMERIC(18,8)             NOT NULL,
              source       TEXT,
              fetched_at   TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT NOW(),
              PRIMARY KEY (fx_date, currency)
            );
        """))
        d = start_d
        src = f"{FX_PROVIDER_NAME} (latest base={base})"
        rows = []
        while d <= end_d:
            for code in currencies:
                rows.append({"d": d, "c": code, "r": code_to_usd(code), "s": src})
            conn.execute(text("""
                INSERT INTO dw.dim_fx_rate(fx_date, currency, rate_to_usd, source, fetched_at)
                VALUES (:d, :c, :r, :s, NOW())
                ON CONFLICT (fx_date, currency) DO UPDATE SET
                  rate_to_usd = EXCLUDED.rate_to_usd,
                  source      = EXCLUDED.source,
                  fetched_at  = NOW();
            """), rows)
            rows.clear()
            d += timedelta(days=1)

    print(f"FX populated for {start_d}..{end_d} (base={base}); ccy: {', '.join(currencies)}")

def load_fact_sales_item() -> None:
    orders_eng   = create_engine(ORDERS_URL, pool_pre_ping=True)
    products_eng = create_engine(PRODUCTS_URL, pool_pre_ping=True)
    dw_eng       = create_engine(DW_URL, pool_pre_ping=True)

    order_pk = _pick_col(orders_eng, "orders", ["order_id", "id"])
    cust_fk  = _pick_col(orders_eng, "orders", ["customer_id", "customer", "cust_id"])
    ts_col   = _pick_col(orders_eng, "orders", ["order_timestamp", "order_datetime", "created_at", "ordered_at", "order_date"])
    cur_col  = _pick_col(orders_eng, "orders", ["currency", "currency_code"])

    items_pk       = _pick_col(orders_eng, "order_items", ["order_item_id", "id"])
    items_order_fk = _pick_col(orders_eng, "order_items", ["order_id", "order"])
    items_prod_fk  = _pick_col(orders_eng, "order_items", ["product_id", "product"])
    qty_col        = _pick_col(orders_eng, "order_items", ["quantity", "qty"])
    price_col      = _pick_col(orders_eng, "order_items", ["unit_price", "price", "unitprice"])
    prod_pk        = _pick_col(products_eng, "product_descriptions", ["product_id", "id"])

    orders_df = pd.read_sql(f"""
        SELECT
          {order_pk} AS order_id,
          {cust_fk}  AS customer_id,
          {ts_col}   AS order_timestamp,
          {cur_col}  AS currency
        FROM orders
    """, orders_eng)

    items_df = pd.read_sql(f"""
        SELECT
          {items_pk}       AS order_item_id,
          {items_order_fk} AS order_id,
          {items_prod_fk}  AS product_id,
          {qty_col}        AS quantity,
          {price_col}      AS unit_price
        FROM order_items
    """, orders_eng)

    prod_df = pd.read_sql(f"SELECT {prod_pk} AS product_id FROM product_descriptions", products_eng)

    df = items_df.merge(orders_df, on="order_id", how="inner").merge(prod_df, on="product_id", how="left")
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])
    df["date_key"] = df["order_timestamp"].dt.strftime("%Y%m%d").astype(int)
    df["time_key"] = (df["order_timestamp"].dt.hour * 60 + df["order_timestamp"].dt.minute).astype(int)

    df["currency"]   = df["currency"].fillna("USD").astype(str).str.upper()
    df["quantity"]   = df["quantity"].astype(float)
    df["unit_price"] = df["unit_price"].astype(float)

    df["fx_date"] = df["order_timestamp"].dt.date
    fx_df = pd.read_sql("SELECT fx_date, currency, rate_to_usd FROM dw.dim_fx_rate", dw_eng)
    fx_df["currency"] = fx_df["currency"].astype(str).str.upper()

    df = df.merge(
        fx_df, left_on=["fx_date", "currency"], right_on=["fx_date", "currency"], how="left"
    ).rename(columns={"unit_price": "unit_price_orig"})

    # Fill missing FX: USD=>1.0, others->1.0 fallback (flagged in views)
    df["rate_to_usd"] = df.apply(
        lambda r: 1.0 if (pd.isna(r["rate_to_usd"]) and r["currency"] == "USD")
        else (r["rate_to_usd"] if not pd.isna(r["rate_to_usd"]) else 1.0),
        axis=1
    )

    # NOTE: unit_price_usd & line_amount_usd are GENERATED in Postgres—don't send them
    load_cols = [
        "order_item_id", "order_id", "product_id", "customer_id",
        "date_key", "time_key", "quantity",
        "unit_price_orig", "currency", "rate_to_usd"
    ]
    df_load = df[load_cols].drop_duplicates(subset=["order_item_id"]).copy()

    upsert_sql = text("""
        INSERT INTO dw.fact_sales_item (
            order_item_id, order_id, product_id, customer_id,
            date_key, time_key, quantity,
            unit_price_orig, currency, fx_rate_to_usd
        )
        VALUES (
            :order_item_id, :order_id, :product_id, :customer_id,
            :date_key, :time_key, :quantity,
            :unit_price_orig, :currency, :rate_to_usd
        )
        ON CONFLICT (order_item_id) DO UPDATE SET
            order_id        = EXCLUDED.order_id,
            product_id      = EXCLUDED.product_id,
            customer_id     = EXCLUDED.customer_id,
            date_key        = EXCLUDED.date_key,
            time_key        = EXCLUDED.time_key,
            quantity        = EXCLUDED.quantity,
            unit_price_orig = EXCLUDED.unit_price_orig,
            currency        = EXCLUDED.currency,
            fx_rate_to_usd  = EXCLUDED.fx_rate_to_usd;
    """)

    with dw_eng.begin() as conn:
        conn.execute(text("SET search_path = dw, public"))
        conn.execute(upsert_sql, df_load.to_dict(orient="records"))
    print(f"Upserted rows into dw.fact_sales_item: {len(df_load)}")

    try:
        with dw_eng.begin() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW dw.mv_hourly_sales;"))
    except Exception as e:
        print(f"Skipping MV refresh: {e}")

# --------------------------------------------------------------------
# Great Expectations
# --------------------------------------------------------------------
def ge_basic_checks():
    import subprocess
    env = os.environ.copy()
    env["DW_DB_URL"] = os.getenv("DW_DB_URL", DW_URL)
    subprocess.check_call(["python", "/opt/airflow/analytics/run_dq_checks.py"], env=env)

# --------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_dw_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "dw", "postgres", "etl"],
) as dag:

    t_create_dw = PythonOperator(
        task_id="create_dw_schema",
        python_callable=create_dw_schema,
    )

    t_dim_customer = PythonOperator(
        task_id="upsert_dim_customer",
        python_callable=upsert_dim_customer,
    )

    t_dim_product = PythonOperator(
        task_id="upsert_dim_product",
        python_callable=upsert_dim_product,
    )

    t_fetch_fx = PythonOperator(
        task_id="fetch_daily_fx_rates",
        python_callable=fetch_daily_fx_rates,
        op_kwargs={"execution_date_str": "{{ ds }}"},
    )

    t_load_fact = PythonOperator(
        task_id="load_fact_sales_item",
        python_callable=load_fact_sales_item,
    )

    t_ge = PythonOperator(
        task_id="ge_basic_checks",
        python_callable=ge_basic_checks,
    )

    t_create_dw >> [t_dim_customer, t_dim_product] >> t_fetch_fx >> t_load_fact >> t_ge
