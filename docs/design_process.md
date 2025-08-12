Modeling: dim_product, dim_customer (if used), fact_sales_item; generated USD fields; date_key/time_key.

FX logic: try API per day+currency; fallback to 1.0; flagged; not mutating source; “clean” view excludes non-ISO + FX fallback for analytics.

Performance: indexes on date_key/time_key/product_id; optional MV later.

Orchestration: Airflow DAG task order: schema → dims → fx → fact; column auto-detection for portability.

Data quality: flags + views; why we used views.

Known quirks: sample data contains ABC/XYZ/QWE; we filter in clean view.