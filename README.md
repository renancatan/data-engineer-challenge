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


# How to run

- docker compose up -d

- airflow tasks test … create_dw_schema then dims/fx/fact (or trigger DAG from UI).

- psql examples to run queries.

- “Troubleshooting” bullets you already used: env checks, getent hosts, and psql from scheduler.


flowchart LR
  subgraph Sources
    A[(Postgres: ecommerce_db1\ncustomers/orders/order_items)]
    B[(Postgres: ecommerce_db2\nproduct_descriptions)]
  end
  A -->|extract| D{{Airflow DAG}}
  B -->|extract| D
  D -->|transform + FX| C[(Postgres: data_warehouse\nDW schema)]
  C -->|views| E[BI / Queries\n(vw_sales_base / enriched / clean)]


Prereqs
Docker + Docker Compose installed

Ports used by Airflow UI and Postgres are free

Airflow Connections exist:

postgres_db1 → db1

postgres_db2 → db2

postgres_dw → dw

Set an execution date (Airflow “logical date”) you’ll reuse in tests:


export EXEC_DATE=2025-08-09

1) Start containers
docker compose up -d

2) (Re)create DW schema, views, and indexes
docker exec -i data_warehouse psql -U postgres -d data_warehouse < airflow/dags/dw_schema.sql

3) Dry-run each ETL task (Airflow tasks test)

(Optional, run some of them separated for individual control/testing) 
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl create_dw_schema 2025-08-09
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl upsert_dim_product 2025-08-09
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl fetch_daily_fx_rates 2025-08-09
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl load_fact_sales_item 2025-08-09

trigger the whole DAG:
docker exec -it airflow_scheduler airflow dags trigger ecommerce_dw_etl

4) Run analytics SQL (materialized views, summaries, etc.)
docker exec -i data_warehouse psql -U postgres -d data_warehouse < analytics/queries.sql

5) Generate charts and export locally

# give ownership to airflow user (uid 50000) inside the container
docker exec -u root -it airflow_scheduler bash -lc '
  mkdir -p /opt/airflow/analytics/figures &&
  chown -R 50000:0 /opt/airflow/analytics
'

docker exec -it airflow_scheduler bash -lc 'python /opt/airflow/analytics/make_charts.py'
# if analytics is mounted, results appear on your host:
ls -1 analytics/figures


Generated artifacts (PNG + CSV) will be under analytics/figures/, e.g.:

daily_revenue_7dma.png / .csv

revenue_by_category.png / .csv

revenue_by_hour.png / .csv

heatmap_weekday_hour.png

top_customers.png / .csv

mom_by_category_top5.png / .csv (bar chart)

(Optional) fx_coverage.png

others new imgs.. also added

6) Run tests (pytest inside scheduler)
Note: tests import DAG modules, so we set PYTHONPATH=/opt/airflow/dags.

docker exec -it airflow_scheduler bash -lc '
  python -m pip install --user -q pytest pytest-cov &&
  export PATH=$HOME/.local/bin:$PATH &&
  PYTHONPATH=/opt/airflow/dags pytest -q /opt/airflow/tests
'

just if necessary:
docker exec -it airflow_scheduler bash -lc 'mkdir -p /opt/airflow/tests'
docker cp tests/. airflow_scheduler:/opt/airflow/tests

and then again:
docker exec -it airflow_scheduler bash -lc '
  python -m pip install --user -q pytest pytest-cov &&
  export PATH=$HOME/.local/bin:$PATH &&
  PYTHONPATH=/opt/airflow/dags pytest -q /opt/airflow/tests
'

7) Run Data Quality checks (Great Expectations)

docker cp analytics/run_dq_checks.py airflow_scheduler:/opt/airflow/analytics/run_dq_checks.py
docker exec -it airflow_scheduler airflow tasks test \
  --subdir /opt/airflow/dags/ecommerce_dw_etl.py \
  ecommerce_dw_etl ge_basic_checks 2025-08-09

If invalid ISO currency codes are present (e.g., 55 rows), this task will exit non-zero (by design).


flowchart LR
  subgraph Sources
    A[(Postgres: ecommerce_db1<br/>customers/orders/order_items)]
    B[(Postgres: ecommerce_db2<br/>product_descriptions)]
  end
  subgraph Airflow
    D[ecommerce_dw_etl DAG<br/>(create_dw_schema → dims → fx → fact)]
  end
  subgraph DW[Postgres: data_warehouse]
    E[(dw.dim_customer)] F[(dw.dim_product)] G[(dw.dim_date)] H[(dw.dim_time)]
    I[(dw.dim_fx_rate)] J[(dw.fact_sales_item)] K[(dw.mv_hourly_sales)]
    L[[dw.vw_sales_base]] M[[dw.vw_sales_enriched]] N[[dw.vw_sales_clean]]
  end
  A -->|orders & items| D
  B -->|product catalog| D
  D -->|upserts| E & F & I & J
  D -->|seeds| G & H
  J --> K
  J --> L --> M --> N
  N -->|SQL queries| O[[Analytics/Reports]]



Troubleshooting
- Airflow imports fail in tests
Ensure: PYTHONPATH=/opt/airflow/dags when running pytest.

- tests/ not found inside container
Confirm the docker cp step above, or mount it via docker-compose volumes.

- DQ task fails with non-zero exit
This is expected when validations fail. Decide whether the DAG should fail (prod) or warn (dev); change the DQ task behavior accordingly.

- Charts not updating locally
Make sure you clear /opt/airflow/analytics/figures in the container before rerunning, then docker cp the folder back to ./analytics/figures.
