import pandas as pd
from types import SimpleNamespace
import importlib

try:
    mod = importlib.import_module("ecommerce_dw_etl")
except ModuleNotFoundError:
    mod = importlib.import_module("airflow.dags.ecommerce_dw_etl")

# function import for the _pick_col unit test
try:
    from ecommerce_dw_etl import _pick_col
except ModuleNotFoundError:
    from airflow.dags.ecommerce_dw_etl import _pick_col


def test_dag_imports():
    assert hasattr(mod, "dag")

def test_required_functions_exist():
    for fn in ("create_dw_schema", "upsert_dim_customer", "upsert_dim_product", "fetch_daily_fx_rates", "load_fact_sales_item"):
        assert hasattr(mod, fn)


def test_date_time_keys():
    # mimic minimal transform
    df = pd.DataFrame({
        "order_timestamp": pd.to_datetime(["2024-08-09 14:35:00", "2024-08-09 00:01:00"])
    })
    df["date_key"] = df["order_timestamp"].dt.strftime("%Y%m%d").astype(int)
    df["time_key"] = (df["order_timestamp"].dt.hour * 60 + df["order_timestamp"].dt.minute).astype(int)
    assert df["date_key"].tolist() == [20240809, 20240809]
    assert df["time_key"].tolist() == [14*60+35, 1]

def test_fx_fallback_flag():
    f = pd.DataFrame({
        "currency": ["USD","EUR","ABC"],
        "fx_rate_to_usd": [1.0, 1.0, 1.0]
    })
    f["is_non_iso_currency"] = ~f["currency"].isin(["USD","EUR","GBP"])
    f["is_fx_fallback"] = (f["fx_rate_to_usd"] == 1.0) & (f["currency"] != "USD")
    assert f["is_non_iso_currency"].tolist() == [False, False, True]
    assert f["is_fx_fallback"].tolist() == [False, True, True]

def test_pick_col_prefers_first_match(monkeypatch):
    class FakeConn:
        def execute(self, _, params):
            # Return an object that has .fetchall()
            return SimpleNamespace(fetchall=lambda: [("id",), ("order_date",)])
        def __enter__(self): return self
        def __exit__(self, *a): pass
    class FakeEngine:
        def begin(self): return FakeConn()

    assert _pick_col(FakeEngine(), "orders", ["order_id","id"]) == "id"
    assert _pick_col(FakeEngine(), "orders", ["created_at","order_date"]) == "order_date"

