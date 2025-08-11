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



# Quick check-list:

# 1) Up containers
docker compose up -d

# 2) (Re)create schema + views + indexes
docker exec -i data_warehouse psql -U postgres -d data_warehouse < database/dw_schema.sql

# 3) Run DAG tasks once
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl create_dw_schema 2025-08-09
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl upsert_dim_product 2025-08-09
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl fetch_daily_fx_rates 2025-08-09
docker exec -it airflow_scheduler airflow tasks test --subdir /opt/airflow/dags/ecommerce_dw_etl.py ecommerce_dw_etl load_fact_sales_item 2025-08-09

# 4) Run analytics
docker exec -i data_warehouse psql -U postgres -d data_warehouse < analytics/queries.sql

# 5) Run tests
docker exec -it airflow_scheduler bash -lc 'PYTHONPATH=/opt/airflow/dags ~/.local/bin/pytest -q /opt/airflow/tests'


# 6) Generate visuals:
docker cp airflow_scheduler:/opt/airflow/analytics/figures ./analytics/figures


flowchart LR
  subgraph Sources
    A[(Postgres: ecommerce_db1\ncustomers/orders/order_items)]
    B[(Postgres: ecommerce_db2\nproduct_descriptions)]
  end

  subgraph Airflow
    D[ecommerce_dw_etl DAG\n(create_dw_schema → dims → fx → fact)]
  end

  subgraph DW[Postgres: data_warehouse]
    E[(dw.dim_customer)]
    F[(dw.dim_product)]
    G[(dw.dim_date)]
    H[(dw.dim_time)]
    I[(dw.dim_fx_rate)]
    J[(dw.fact_sales_item)]
    K[(dw.mv_hourly_sales)]
    L[[dw.vw_sales_base]]
    M[[dw.vw_sales_enriched]]
    N[[dw.vw_sales_clean]]
  end

  A -->|orders & items| D
  B -->|product catalog| D
  D -->|upserts| E & F & I & J
  D -->|seeds| G & H
  J --> K
  J --> L --> M --> N

  N -->|SQL queries| O[[Analytics/Reports]]


  ---
# 1) Clean any old local copies
rm -rf analytics/figures
mkdir -p analytics

# 2) Copy the whole figures folder out of the container into ./analytics
docker cp airflow_scheduler:/opt/airflow/analytics/figures ./analytics

# 3) Sanity check
ls -1 analytics/figures
