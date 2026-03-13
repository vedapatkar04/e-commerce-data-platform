Always check for port conflicts before debugging credentials.
If a service is already running on the same port, Docker's 
container will never receive the connection — even if everything 
else is perfectly configured.

Rule: One service per port on your machine.

# E-Commerce Data Platform

An end-to-end data engineering portfolio project simulating a real production pipeline — from raw transactional data to analytical warehouse, real-time streaming, SQL transformations, and data quality monitoring.

> Built to demonstrate core data engineering skills for a junior data engineer role.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│   OLTP PostgreSQL (orders, users, products, clickstream)        │
└────────────────────────┬────────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
   Batch (Airflow)              Streaming (Kafka)
   runs daily                   real-time events
          │                             │
          └──────────────┬──────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                   Warehouse Schema                               │
│   fact_orders + dim_users, dim_products, dim_date, dim_location  │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                   dbt Transformation Layer                       │
│   staging → intermediate → marts (revenue, funnel, products)    │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                   Data Quality & Monitoring                      │
│   Great Expectations checks + Pipeline health monitoring        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Source DB | PostgreSQL | 15 |
| Orchestration | Apache Airflow | 2.8.1 |
| Streaming | Apache Kafka | 7.5.0 |
| Transformation | dbt Core | 1.7.0 |
| Data Quality | Great Expectations | latest |
| Language | Python | 3.14 |
| Containers | Docker Compose | - |

---

## Project Phases

### ✅ Phase 1 — Foundation & Data Modeling
- Designed normalized OLTP schema (6 tables)
- Designed star schema warehouse (fact_orders + 4 dimensions)
- Generated 140,000+ rows of synthetic data using Python Faker
- PostgreSQL running in Docker with automatic schema init

### ✅ Phase 2 — Batch ETL Pipeline
- Built extract, transform, load layers in Python
- Orchestrated via Apache Airflow DAG with 9 tasks
- Full refresh strategy with data quality validation
- CeleryExecutor with Redis message broker

### ✅ Phase 3 — Kafka Streaming
- Producer simulating 2 clickstream events/sec
- Consumer micro-batching 50 events per DB write
- 100,000+ events processed with realistic funnel distribution
- Monitored via Kafka UI

### ✅ Phase 4 — dbt Transformations
- 4 staging models (clean raw sources)
- 1 intermediate model (enriched joins)
- 3 mart models (revenue, funnel, top products)
- Automated data quality tests (not_null, unique, accepted_values)

### ✅ Phase 5 — Orchestration & Deployment
- Great Expectations data quality checks across all tables
- Pipeline health monitoring across all 7 services
- One-command startup and pipeline runner script

---

## Folder Structure

```
ecommerce-data-platform/
├── docker-compose.yml
├── requirements.txt
├── README.md
│
├── phase-1-modeling/
│   ├── init.sql                    # OLTP + warehouse schema
│   └── seed_data.py                # Synthetic data generator
│
├── phase-2-batch-pipeline/
│   ├── dags/etl_dag.py             # Airflow DAG (9 tasks)
│   └── etl/
│       ├── extract.py
│       ├── transform.py
│       └── load.py
│
├── phase-3-streaming/
│   ├── producer.py                 # Kafka producer
│   └── consumer.py                 # Kafka consumer
│
├── phase-4-dbt-transforms/
│   ├── profiles.yml
│   └── ecommerce_dbt/
│       ├── dbt_project.yml
│       └── models/
│           ├── staging/            # stg_users, stg_orders, stg_products, stg_clickstream
│           ├── intermediate/       # int_order_items_enriched
│           └── marts/              # mart_revenue, mart_funnel, mart_top_products
│
└── phase-5-orchestration/
    ├── great_expectations/
    │   └── expectations.py         # Data quality checks
    ├── monitoring/
    │   └── pipeline_health.py      # Service health monitor
    └── scripts/
        └── run_pipeline.sh         # One-command runner
```

---

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- Python 3.11+
- Git

### 1. Clone the repo
```bash
git clone https://github.com/vedapatkar04/ecommerce-data-platform.git
cd ecommerce-data-platform
```

### 2. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3. Start all services
```bash
docker-compose up -d
```

### 4. Seed the database
```bash
python phase-1-modeling/seed_data.py
```

### 5. Run the full pipeline
```bash
# Trigger Airflow ETL DAG
# Open http://localhost:8080 → ecommerce_etl → Trigger

# Run dbt transformations
docker exec -it ecommerce_dbt dbt run

# Run data quality checks
python phase-5-orchestration/great_expectations/expectations.py

# Check platform health
python phase-5-orchestration/monitoring/pipeline_health.py
```

### Or use the one-command runner
```bash
chmod +x phase-5-orchestration/scripts/run_pipeline.sh
./phase-5-orchestration/scripts/run_pipeline.sh
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin123 |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin123 |
| Kafka UI | http://localhost:8090 | — |
| PostgreSQL | localhost:5433 | deuser / depass |

---

## Data Model

### OLTP Schema (Source)
Normalized transactional database powering the e-commerce app:
`users` → `orders` → `order_items` → `products` → `categories`
`users` → `clickstream_events` → `products`

### Warehouse Schema (Star Schema)
Denormalized analytical layer optimized for queries:
```
           dim_users
               │
dim_date ── fact_orders ── dim_products
               │
          dim_location
```

### dbt Mart Layer
Business-ready aggregations:
- `mart_revenue` — daily revenue by country and category
- `mart_funnel` — clickstream conversion rates by stage
- `mart_top_products` — product performance ranked by revenue

---

## Key Concepts Demonstrated

- **OLTP vs OLAP** — normalized write-optimized vs denormalized read-optimized schemas
- **Star Schema** — fact and dimension table design
- **ETL Pipeline** — extract, transform, load with pandas and SQLAlchemy
- **Workflow Orchestration** — Airflow DAGs, task dependencies, XCom, retries
- **Event Streaming** — Kafka producer/consumer, micro-batching, offset management
- **SQL Transformations** — dbt staging/intermediate/mart layers, ref() dependencies
- **Data Quality** — Great Expectations assertions, dbt schema tests
- **Containerization** — Docker Compose with 10 services

---

## Data Volume

| Table | Rows |
|---|---|
| oltp.users | 1,000 |
| oltp.products | 500 |
| oltp.orders | 10,000 |
| oltp.order_items | ~30,000 |
| oltp.clickstream_events | 100,000+ |
| warehouse.fact_orders | ~30,000 |
