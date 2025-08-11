# analytics/run_ge_checks.py
import great_expectations as ge
from sqlalchemy import create_engine
import pandas as pd
import os

DW_URL = os.environ.get(
    "DW_DB_URL",
    "postgresql+psycopg2://postgres:postgres@data_warehouse:5432/data_warehouse"
)

def run_checks():
    eng = create_engine(DW_URL, pool_pre_ping=True)

    # 1) fact table basic constraints
    f = pd.read_sql("SELECT * FROM dw.fact_sales_item", eng)
    gdf = ge.from_pandas(f)

    # Uniqueness & nulls
    gdf.expect_column_values_to_not_be_null("order_item_id")
    gdf.expect_column_values_to_be_unique("order_item_id")

    # Keys in valid ranges
    gdf.expect_column_values_to_match_regex("date_key", r"^\d{8}$")
    gdf.expect_column_values_to_be_between("time_key", min_value=0, max_value=1439)

    # Currency whitelist
    gdf.expect_column_values_to_be_in_set("currency", ["USD","EUR","GBP"])

    # FX sanity
    gdf.expect_column_values_to_be_greater_than("fx_rate_to_usd", 0)

    # No non-USD rows with fallback rate (soft fail: mostly we want 0)
    non_usd = f[(f["currency"]!="USD") & (f["fx_rate_to_usd"]==1.0)]
    soft_pass = len(non_usd) == 0

    res = gdf.validate(result_format="SUMMARY")
    ok = res["success"] and soft_pass

    # 2) referential integrity (dim existence) — sample check
    prod_ids = set(pd.read_sql("SELECT product_id FROM dw.dim_product", eng)["product_id"])
    missing = f.loc[~f["product_id"].isin(prod_ids), "product_id"].unique().tolist()
    ri_ok = (len(missing) == 0)

    status = ok and ri_ok
    print("GE basic suite:", "PASSED" if status else "FAILED")
    if not ri_ok:
        print(f"Missing product_ids in dim_product: {missing[:10]}")
    if not soft_pass:
        print(f"Non-USD rows with fallback FX: {len(non_usd)}")

    return 0 if status else 1

if __name__ == "__main__":
    raise SystemExit(run_checks())
