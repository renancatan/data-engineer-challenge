# analytics/run_dq_checks.py
import os, sys
from sqlalchemy import create_engine, text

DW_URL = os.getenv("DW_DB_URL","postgresql+psycopg2://postgres:postgres@data_warehouse:5432/data_warehouse")

NON_ISO_MAX_PCT = float(os.getenv("DQ_NON_ISO_MAX_PCT", "0.50"))  # warn if over 50% of the fact

def fail(msg): print(f"[DQ][FAIL] {msg}"); sys.exit(1)

def main():
    eng = create_engine(DW_URL, pool_pre_ping=True)
    issues = []; warnings = []

    with eng.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM dw.fact_sales_item")).scalar() or 0
        print(f"[DQ] fact_sales_item rows: {total}")

        # PK uniqueness
        dup = conn.execute(text("""
          SELECT COUNT(*) FROM (
            SELECT order_item_id FROM dw.fact_sales_item GROUP BY order_item_id HAVING COUNT(*)>1
          ) t
        """)).scalar()
        if dup: issues.append(f"Duplicate order_item_id rows: {dup}")

        # FKs
        bad_date = conn.execute(text("""
          SELECT COUNT(*) FROM dw.fact_sales_item f
          LEFT JOIN dw.dim_date d ON d.date_key=f.date_key
          WHERE d.date_key IS NULL
        """)).scalar()
        if bad_date: issues.append(f"Missing dim_date keys: {bad_date}")

        bad_time = conn.execute(text("""
          SELECT COUNT(*) FROM dw.fact_sales_item f
          LEFT JOIN dw.dim_time t ON t.time_key=f.time_key
          WHERE t.time_key IS NULL
        """)).scalar()
        if bad_time: issues.append(f"Missing dim_time keys: {bad_time}")

        # Non-ISO in FACT: warn (expected pre-clean), fail only if proportion > threshold
        non_iso_fact = conn.execute(text("""
          SELECT COUNT(*) FROM dw.fact_sales_item
          WHERE currency NOT IN ('USD','EUR','GBP')
        """)).scalar() or 0
        pct = (non_iso_fact/total) if total else 0
        if non_iso_fact:
            msg = f"Non-ISO currency rows in fact: {non_iso_fact} ({pct:.1%})"
            if pct > NON_ISO_MAX_PCT:
                issues.append(msg + f" > {NON_ISO_MAX_PCT:.0%} threshold")
            else:
                warnings.append(msg)

        # FX > 0
        bad_fx = conn.execute(text("""
          SELECT COUNT(*) FROM dw.fact_sales_item
          WHERE fx_rate_to_usd IS NULL OR fx_rate_to_usd <= 0
        """)).scalar()
        if bad_fx: issues.append(f"Invalid fx_rate_to_usd rows: {bad_fx}")

        # quantity/price sane
        bad_qty = conn.execute(text("SELECT COUNT(*) FROM dw.fact_sales_item WHERE quantity IS NULL OR quantity < 0")).scalar()
        if bad_qty: issues.append(f"Invalid quantity rows: {bad_qty}")
        bad_price = conn.execute(text("SELECT COUNT(*) FROM dw.fact_sales_item WHERE unit_price_orig IS NULL OR unit_price_orig < 0")).scalar()
        if bad_price: issues.append(f"Invalid unit_price_orig rows: {bad_price}")

        # HARD FAIL: clean view must have ZERO non-ISO
        non_iso_clean = conn.execute(text("""
          SELECT COUNT(*) FROM dw.vw_sales_clean WHERE is_non_iso_currency = TRUE
        """)).scalar()
        if non_iso_clean:
            issues.append(f"vw_sales_clean contains non-ISO rows: {non_iso_clean}")

    for w in warnings: print("[DQ][WARN]", w)
    if issues:
        for p in issues: print("[DQ][ISSUE]", p)
        fail("Data quality checks failed.")
    print("[DQ] All checks passed ✅")

if __name__ == "__main__":
    main()
