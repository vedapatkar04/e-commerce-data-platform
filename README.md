Always check for port conflicts before debugging credentials.
If a service is already running on the same port, Docker's 
container will never receive the connection — even if everything 
else is perfectly configured.

Rule: One service per port on your machine.

Project Overview
A complete data engineering platform built on e-commerce data. Simulates a real production pipeline from raw transactional data to an analytical warehouse, including real-time streaming, transformation layers, and orchestration.
Built as a portfolio project to demonstrate core data engineering concepts for a junior data engineer role.

Architecture
Layer	Tool	Purpose
Source DB	PostgreSQL 15	OLTP transactional data
Orchestration	Apache Airflow 2.8	Pipeline scheduling & monitoring
Streaming	Apache Kafka	Real-time clickstream events
Transformation	dbt Core	SQL-based data modeling
Warehouse	PostgreSQL (warehouse schema)	Star schema analytical layer
Language	Python 3.14	ETL scripting
Containers	Docker + Compose	Local infrastructure


Project Phases
Phase 1 — Foundation & Data Modeling ✅
•	Designed normalized OLTP schema (6 tables: users, products, categories, orders, order_items, clickstream_events)
•	Designed star schema warehouse (fact_orders + 4 dimensions: users, products, date, location)
•	Set up PostgreSQL in Docker with automatic schema initialization
•	Generated 140,000+ rows of realistic synthetic data using Python Faker

Phase 2 — Batch ETL Pipeline ✅
•	Built Extract layer: 4 functions reading from OLTP schema into DataFrames
•	Built Transform layer: derives price_band, user segment, date_key integer, calculates revenue
•	Built Load layer: full refresh strategy with data quality validation
•	Wired into Apache Airflow DAG with 9 tasks and correct dependency ordering
•	All tasks running green — warehouse star schema fully populated

Phase 3 — Kafka Streaming ⏳
•	Real-time clickstream event producer simulating user behaviour
•	Kafka consumer pipeline writing events to PostgreSQL
•	Stream processing with windowed aggregations

Phase 4 — dbt Transformations ⏳
•	Staging, intermediate, and mart layer models
•	Incremental loading strategy
•	Data lineage and documentation

Phase 5 — Orchestration & Deployment ⏳
•	Full Docker Compose setup for all services
•	Data quality checks with Great Expectations
•	Monitoring and alerting


Folder Structure
ecommerce-data-platform/
├── docker-compose.yml          # All infrastructure
├── requirements.txt            # Python dependencies
├── .gitignore
├── README.md
├── phase-1-modeling/
│   ├── init.sql                # OLTP + warehouse schema
│   └── seed_data.py            # Synthetic data generator
├── phase-2-batch-pipeline/
│   ├── dags/
│   │   └── etl_dag.py          # Airflow DAG
│   └── etl/
│       ├── extract.py
│       ├── transform.py
│       └── load.py
├── phase-3-streaming/
├── phase-4-dbt-transforms/
└── phase-5-orchestration/


How to Run
Prerequisites
•	Docker Desktop installed and running
•	Python 3.11+
•	Git

Quick Start
1.	Clone the repository
git clone https://github.com/YOUR_USERNAME/ecommerce-data-platform.git
2.	Install Python dependencies
pip install -r requirements.txt
3.	Start all Docker services
docker-compose up -d
4.	Seed the database
cd phase-1-modeling && python seed_data.py
5.	Open Airflow UI and trigger DAG
http://localhost:8080  (admin / admin123)

Service URLs
Service	URL	Credentials
Airflow UI	http://localhost:8080	admin / admin123
pgAdmin	http://localhost:5050	admin@admin.com / admin123
PostgreSQL	localhost:5433	deuser / depass


Data Model
OLTP Schema (Source)
6 normalized tables representing a real e-commerce backend database:
•	users — registered customers with country and activity status
•	categories — hierarchical product categories (self-referencing)
•	products — product catalog with price and stock
•	orders — customer orders with status lifecycle
•	order_items — line items per order (junction table)
•	clickstream_events — user behaviour events (page views, add to cart, purchases)

Warehouse Schema (Star Schema)
5 tables in star schema format optimized for analytical queries:
•	fact_orders — central fact table (one row per order line item)
•	dim_users — user dimension with derived segment (new/regular/vip)
•	dim_products — product dimension with derived price_band
•	dim_date — pre-populated date dimension (2022-2026) with YYYYMMDD integer key
•	dim_location — location dimension with region derived from country
