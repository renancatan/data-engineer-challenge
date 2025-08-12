# Data Engineer Challenge Setup

This directory contains the complete Docker Compose setup for the Data Engineer coding challenge. The environment provides realistic eCommerce data sources and infrastructure for candidates to build their ETL pipelines and data warehouse solutions.

## Architecture Overview

The setup includes:
- **3 PostgreSQL databases** (source systems + data warehouse)
- **1 Apache Airflow instance** (orchestration)
- **Sample eCommerce data** (orders, customers, products)
- **Currency conversion API documentation** (external dependency)

## Quick Start

1. **Prerequisites**
   - Docker and Docker Compose installed
   - At least 4GB RAM available for containers
   - Ports 5432, 5433, 5434, and 8080 available

2. **Start the environment**
   ```bash
   cd data_engineer_setup
   docker-compose up -d
   ```

3. **Verify services are running**
   ```bash
   docker-compose ps
   ```

4. **Access Airflow Web UI**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

## Database Connections

### Database 1: Orders & Customers
- **Host**: localhost
- **Port**: 5432
- **Database**: ecommerce_orders
- **Username**: postgres
- **Password**: postgres

**Tables:**
- `customers` - Customer information (50 customers from various countries)
- `orders` - Order transactions (83 orders spanning 6 months)
- `order_items` - Individual items within orders (83+ line items)

### Database 2: Product Catalog
- **Host**: localhost
- **Port**: 5433
- **Database**: ecommerce_products
- **Username**: postgres
- **Password**: postgres

**Tables:**
- `product_descriptions` - Product catalog (100 products across 10 categories)

### Database 3: Data Warehouse (Target)
- **Host**: localhost
- **Port**: 5434
- **Database**: data_warehouse
- **Username**: postgres
- **Password**: postgres

**Note**: This database is intentionally empty for you to design your own schema.

## Sample Data Overview

### Business Context
The sample data represents a global eCommerce platform with:
- **Customers** from multiple countries
- **Products** across various categories (Electronics, Clothing, Books, etc.)
- **Orders** with realistic time patterns (more activity during business hours)
- **Multiple currencies** (USD, EUR, GBP) requiring conversion
- **Transaction history** spanning several months

### Key Business Questions to Answer
1. **Product Performance**: Which products are top performers in sales volume and revenue?
2. **Optimal Timing**: What's the best time of day to run sales promotions?

## Currency Conversion API

For currency conversion, you'll need to integrate with an external API. Here's the expected interface:

**Endpoint**: `https://api.exchangerate-api.com/v4/latest/{base_currency}`
**Parameters**:
- `date`: YYYY-MM-DD format
- `currency_from`: 3-letter currency code (USD, EUR, GBP)
- `currency_to`: 3-letter currency code

**Example Response**:
```json
{
  "base": "USD",
  "date": "2024-07-25",
  "rates": {
    "EUR": 0.92,
    "GBP": 0.78
  }
}
```

## Development Workflow

1. **Explore the Data**
   ```sql
   -- Connect to Database 1
   SELECT COUNT(*) FROM customers;
   SELECT COUNT(*) FROM orders;
   SELECT COUNT(*) FROM order_items;
   
   -- Connect to Database 2
   SELECT COUNT(*) FROM product_descriptions;
   SELECT DISTINCT category FROM product_descriptions;
   ```

2. **Design Your Data Warehouse Schema**
   - Consider dimensional modeling (facts and dimensions)
   - Plan for the business questions you need to answer
   - Design for scalability and query performance

3. **Implement ETL Pipeline**
   - Create your DAG in `airflow/dags/`
   - Use the example DAG to test database connections
   - Implement extraction, transformation, and loading logic

4. **Test Your Pipeline**
   - Use Airflow UI to trigger and monitor DAGs
   - Validate data quality and completeness
   - Test your analytics queries


## Troubleshooting

### Container Issues
```bash
# Check container status
docker-compose ps

# View container logs
docker-compose logs [service_name]

# Restart services
docker-compose restart

# Clean restart
docker-compose down && docker-compose up -d
```

### Database Connection Issues
```bash
# Test database connections
docker exec -it ecommerce_db1 psql -U postgres -d ecommerce_orders -c "SELECT COUNT(*) FROM customers;"
docker exec -it ecommerce_db2 psql -U postgres -d ecommerce_products -c "SELECT COUNT(*) FROM product_descriptions;"
docker exec -it data_warehouse psql -U postgres -d data_warehouse -c "SELECT * FROM connection_test;"
```

### Airflow Issues
- Ensure all containers are running before accessing the web UI
- Check Airflow logs: `docker-compose logs airflow-webserver`
- Restart Airflow: `docker-compose restart airflow-webserver airflow-scheduler`

## File Structure
```
data_engineer_setup/
├── docker-compose.yml          # Main orchestration file
├── database/
│   ├── init_db1.sql           # Orders & customers data
│   ├── init_db2.sql           # Product catalog data
│   └── init_warehouse.sql     # Empty warehouse schema
├── airflow/
│   ├── dags/
│   │   └── example_dag.py     # Example ETL pipeline
│   ├── logs/                  # Airflow logs (auto-generated)
│   └── plugins/               # Custom Airflow plugins
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## Deliverable Assets

- A SQL file that creates the structure of the DataWarehouse tables needed for solving the challenge at the Warehouse.
- A working Python script integrated with Airflow having the transformation pipeline running as expected given the codebase.
- A report or mockup about outlining the answers of the key business questions.
- Brief explanation about the thought process into a file named design_process.md .

## Success Criteria

Your solution should demonstrate:
- ✅ **Data Modeling**: Well-designed warehouse schema
- ✅ **ETL Pipeline**: Functional Airflow DAG with proper task dependencies
- ✅ **Data Quality**: Handling of data inconsistencies and validation
- ✅ **Business Logic**: Transformations that support the required analytics
- ✅ **Documentation**: Clear code comments and README updates
- ✅ **Dashboard Mockup**: Visual representation of insights (optional)

## Time Management

This is a 5-hour challenge. Suggested time allocation:
- **1 hour**: Environment setup and data exploration
- **2 hours**: Data warehouse schema design and implementation
- **1.5 hours**: ETL pipeline development and testing
- **30 minutes**: Documentation and dashboard mockup

Good luck with your data engineering challenge! 🚀


# How to Run

This section explains how to boot the stack, test the ETL, generate analytics, run tests, and execute data‑quality checks. It matches the formatting style used in the top of this repository (headings, bullets, and fenced code blocks).

## Prerequisites
- Docker and Docker Compose installed
- Ports **5432**, **5433**, **5434**, and **8080** available
- Airflow connections configured: `postgres_db1`, `postgres_db2`, `postgres_dw`

**Optional (Linux, to avoid permission issues):**
```bash
export AIRFLOW_UID=$(id -u)
export AIRFLOW_GID=$(id -g)
```

---


## 1) Create the `.env` file (before starting containers)

In the project root, create a file named `.env` with the following content, replacing `YOUR_API_KEY` with your real FX API key:
FX_API_KEY=YOUR_API_KEY
FX_BASE_CURRENCY=USD

## 1.b) Start the environment
```bash
docker compose up -d
```
**(reuse a logical date across commands):**
```bash
export EXEC_DATE=2025-08-09
```


## 2) (Re)create DW schema, views, and indexes
```bash
docker exec -i data_warehouse psql -U postgres -d data_warehouse < airflow/dags/dw_schema.sql
```

## 3) Dry‑run ETL tasks (Airflow `tasks test`)

**Trigger the whole DAG**
```bash
docker exec -it airflow_scheduler airflow dags trigger ecommerce_dw_etl
```

**Optional: Individual runs**
```bash
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl create_dw_schema      ${EXEC_DATE:-2025-08-09}
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl upsert_dim_product   ${EXEC_DATE:-2025-08-09}
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl fetch_daily_fx_rates ${EXEC_DATE:-2025-08-09}
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl load_fact_sales_item ${EXEC_DATE:-2025-08-09}
```

## 4) Run analytics SQL (views / summaries) - **RUN TWICE** to actually see the results
```bash
docker exec -i data_warehouse psql -U postgres -d data_warehouse < analytics/queries.sql
```

## 5) Generate charts (artifacts in `analytics/figures/`)

**Generate:**
```bash
docker exec -it airflow_scheduler bash -lc 'python /opt/airflow/analytics/make_charts.py'
ls -1 analytics/figures
```

**Expected artifacts**
- `daily_revenue_7dma.png` / `.csv`
- `revenue_by_category.png` / `.csv`
- `revenue_by_hour.png` / `.csv`
- `heatmap_weekday_hour.png`
- `top_customers.png` / `.csv`
- `mom_by_category_top5_bars.png` / `.csv`
- *(Optional)* `fx_coverage.png`
- A few other added, such as avg order.. etc


## 6) Run tests (pytest inside scheduler)
```bash
docker exec -it airflow_scheduler bash -lc 'python -m pip install --user -q pytest pytest-cov && export PATH=$HOME/.local/bin:$PATH && PYTHONPATH=/opt/airflow/dags pytest -q /opt/airflow/tests'
```

## 7) Data Quality checks (Great Expectations)
```bash
docker cp analytics/run_dq_checks.py airflow_scheduler:/opt/airflow/analytics/run_dq_checks.py
docker exec -it airflow_scheduler airflow tasks test   --subdir /opt/airflow/dags/ecommerce_dw_etl.py   ecommerce_dw_etl ge_basic_checks ${EXEC_DATE:-2025-08-09}
```

## 8) Optional — verify FX is applied

> Assumes you’ve already run:
> ```
> docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl fetch_daily_fx_rates 2025-08-09
> docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl load_fact_sales_item 2025-08-09
> ```

### 8.a) Spot-check 10 fact rows (showing FX columns)
```bash
docker exec -i data_warehouse psql -U postgres -d data_warehouse -c "
SELECT order_item_id, currency, fx_rate_to_usd, unit_price_orig, unit_price_usd, line_amount_usd
FROM dw.fact_sales_item
ORDER BY order_item_id
LIMIT 10;"
```

### 8.b) FX summary by currency (proves API rates applied) rates for EUR and GBP, since the others arent real currency, so they will remain the same rate.
```bash
docker exec -i data_warehouse psql -U postgres -d data_warehouse -c "
SELECT currency, COUNT(*) AS rows, ROUND(AVG(fx_rate_to_usd), 6) AS avg_fx
FROM dw.fact_sales_item
GROUP BY currency
ORDER BY currency;"
```

### 8.c) Show only non-USD converted rows (sanity check)
```bash
  docker exec -i data_warehouse psql -U postgres -d data_warehouse -c "
SELECT order_item_id, currency, fx_rate_to_usd,
       unit_price_orig,
       unit_price_usd,
       (unit_price_orig * fx_rate_to_usd)::numeric(18,4) AS expected_usd
FROM dw.fact_sales_item
WHERE currency IN ('EUR','GBP')
ORDER BY order_item_id
LIMIT 10;"
```

### 8.d) (Optional) See what’s in the FX dimension for today’s load date
```bash
docker exec -i data_warehouse psql -U postgres -d data_warehouse -c "
SELECT fx_date, currency, rate_to_usd, source, fetched_at
FROM dw.dim_fx_rate
ORDER BY fx_date DESC, currency
LIMIT 20;"
```

---

## For Scalability:

- Containerize services (already done), deploy on Kubernetes (e.g., AKS, EKS, GKE) for scaling Airflow workers and database pods.

- Use a managed Postgres or cloud data warehouse (BigQuery, Snowflake, Redshift) for horizontal scaling and performance.

- Switch to Airflow’s Celery/KubernetesExecutor to scale ETL in parallel.

- Introduce partitioned tables and clustering for big fact tables.

- Add CI/CD pipelines to deploy DAGs and schema changes


---

## Notes for improvement:

- Add structured logging to ETL scripts (Python logging with JSON formatter)

- Send logs to a central store (ELK, Loki+Grafana, CloudWatch)

- Include log context (dag_id, task_id, execution_date, record counts, durations)

- Add alerting on failed tasks or unexpected row counts

- Modularize the code better (It`s using only one script with all funcs instead of break them down into folders, specific jobs, etc)

- Note: 4 GB RAM limit from the challenge specs is not enforced here, as this environment uses your host’s available resources.

---

## Troubleshooting
- **Airflow imports fail in tests** → run pytest with `PYTHONPATH=/opt/airflow/dags`.
- **`tests/` not found in container** → mount it via compose or `docker cp tests/. airflow_scheduler:/opt/airflow/tests`.
- **DQ task exits non‑zero** → expected when validations fail; decide if the DAG should *fail* (prod) or *warn* (dev).
- **Charts not updating locally** → clear `/opt/airflow/analytics/figures` in the container, rerun generator, then check `./analytics/figures`.