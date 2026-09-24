# 🛒 Ecommerce Analytics Platform

An end-to-end **e-commerce data engineering and analytics platform** built with Python, DuckDB, dbt, Docker, and Streamlit.

The project simulates a modern e-commerce data environment by generating **streaming clickstream events and batch order data**, ingesting them into a local data lake, transforming them through layered dbt models, and exposing business-facing analytics through an interactive Streamlit dashboard.

The platform is designed to demonstrate the complete analytics lifecycle:

**Data Generation → Ingestion → Data Lake → DuckDB Warehouse → dbt Transformations → Analytics Marts → Streamlit Dashboard**

Dashboard Link: https://mattyg3-ecommerce-analytics-platform.streamlit.app/

---

## 📊 What This Project Demonstrates

This project focuses on practical data engineering and analytics patterns commonly used in modern data platforms:

* Streaming-style clickstream ingestion
* Batch order ingestion
* Parquet-based data lake storage
* DuckDB analytical warehousing
* dbt transformation and dimensional modeling
* Incremental data processing
* Handling late-arriving events
* Versioned event schemas
* Automated pipeline orchestration
* Dockerized development environment
* Business-facing analytics dashboards
* Customer, product, funnel, and revenue analysis

The underlying data is **synthetically generated** so the entire pipeline can be reproduced locally without requiring access to proprietary customer or transaction data.

---

## 🏗️ Architecture

```text
                    ┌─────────────────────────┐
                    │   Synthetic Producers   │
                    │                         │
                    │  Clickstream + Orders   │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │       Ingestion         │
                    │                         │
                    │ Streaming Clickstream   │
                    │ Batch Orders             │
                    │ Schema Versioning        │
                    │ Late Event Handling      │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │       Data Lake         │
                    │                         │
                    │ Raw → Landing Parquet   │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │         DuckDB          │
                    │                         │
                    │      Bronze Layer       │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │           dbt           │
                    │                         │
                    │ Staging → Intermediate  │
                    │          → Marts        │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Analytics Marts     │
                    │                         │
                    │ fact_orders              │
                    │ fact_order_items        │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │       Streamlit         │
                    │                         │
                    │ Revenue • Funnel        │
                    │ Customers • Products   │
                    └─────────────────────────┘
```

---

## 🧱 Technology Stack

| Layer                | Technology                     |
| -------------------- | ------------------------------ |
| Programming          | Python                         |
| Data Generation      | Python                         |
| Streaming Simulation | Python                         |
| File Format          | Parquet                        |
| Data Lake            | Local filesystem               |
| Analytical Warehouse | DuckDB                         |
| Transformation       | dbt Core + dbt-duckdb          |
| Data Processing      | Pandas / PyArrow               |
| Dashboard            | Streamlit                      |
| Containerization     | Docker / Docker Compose        |
| Orchestration        | Bash / Python pipeline scripts |
| Version Control      | Git / GitHub                   |

---

# 🔄 Data Pipeline

## 1. Synthetic Data Generation

The platform generates two complementary datasets:

### Clickstream events

The clickstream generator simulates user behavior across an e-commerce website.

Supported event types include:

```text
page_view
view_product
add_to_cart
checkout_start
purchase
```

Events contain fields such as:

```text
event_id
event_type
user_id
session_id
product_id
event_time
ingest_time
version
device
country
user_agent
referrer
experiment_id
```

The generator also simulates realistic data-engineering conditions including:

* Multiple user sessions
* Returning users
* Variable session lengths
* Different devices and countries
* Late-arriving events
* Versioned event schemas
* Variable traffic volume

### Orders

The batch order generator produces transactional order data that can be analyzed alongside the behavioral clickstream data.

---

# 2. Data Lake

Generated data is written to the local data lake as Parquet files.

```text
/data-lake/
│
├── raw/
│   ├── clickstream/
│   └── orders/
│
├── landing/
│   ├── clickstream/
│   └── orders/
│
├── checkpoints/
│
└── warehouse.duckdb
```

This separation provides a simple representation of a modern lake/warehouse architecture while keeping the entire project runnable on a local machine.

---

# 3. Bronze Layer

The ingestion layer loads landing data into DuckDB.

The bronze layer preserves the event-level data needed for downstream analytics while providing a consistent analytical interface for dbt.

The clickstream pipeline also maintains a processing checkpoint so previously processed files do not need to be repeatedly ingested.

---

# 4. dbt Transformation Layer

dbt transforms the ingested data into analytics-ready models.

The project follows a layered modeling approach:

```text
Bronze
  │
  ▼
Staging
  │
  ▼
Intermediate
  │
  ▼
Marts
```

### Staging

Staging models provide:

* Type casting
* Column standardization
* Basic cleaning
* Source-level transformations

### Intermediate

Intermediate models contain reusable business logic such as:

* Event processing
* Session-level transformations
* Deduplication
* Order/item relationships
* Customer behavior logic

### Marts

The marts layer contains business-facing analytical models.

Key models include:

```text
marts.fact_orders
marts.fact_order_items
```

These models are consumed directly by the Streamlit dashboard.

---

# ⏱️ Incremental Processing & Late Events

A major focus of the project is handling data that does not arrive in perfectly chronological order.

Real event pipelines frequently encounter:

* Network delays
* Batch processing delays
* Retries
* Out-of-order events
* Late-arriving files

The ingestion and transformation design therefore separates:

**event time**

from

**ingestion time**

and uses incremental processing patterns that allow delayed events to be incorporated without rebuilding the entire historical dataset.

This provides a more realistic demonstration of production analytics engineering than a simple CSV → database workflow.

---

# 📈 Streamlit Analytics Dashboard

The Streamlit dashboard provides a business-facing interface over the DuckDB analytics warehouse.

All dashboard views support a selectable date range.

## Overview

The Overview tab provides executive-level performance metrics including:

* Revenue
* Orders
* Customers
* Average Order Value
* Revenue trends
* Order trends
* Customer revenue mix
* Period-over-period comparisons
* Key business insights

The dashboard dynamically adjusts time-series granularity based on the selected date range:

```text
Short period   → Hour
Medium period  → Day
Longer period  → Week
Very long      → Month
```

This keeps charts readable across different analysis windows.

---

## Funnel

The Funnel tab analyzes customer progression through the e-commerce journey:

```text
Sessions
   ↓
Product Views
   ↓
Add to Cart
   ↓
Checkout
   ↓
Purchase
```

The analysis is based on session-level clickstream behavior and helps identify where users drop out of the purchasing journey.

---

## Customers

The Customers tab focuses on customer behavior and value.

Analysis includes:

* Customer value
* Customer revenue distribution
* New vs. repeat customers
* Purchase frequency
* Revenue concentration
* Top customers
* Customer lifecycle analysis
* Cohort retention

### Cohort Retention

Customers are assigned to a cohort based on their **first-ever purchase month**.

Retention is then measured by whether those customers make subsequent purchases in later months.

Example:

```text
Cohort       Month 0   Month 1   Month 2   Month 3
---------------------------------------------------
Jan 2026      100%      42%       28%       21%
Feb 2026      100%      39%       25%        -
Mar 2026      100%      44%        -         -
```

Future periods with no available data are treated as unavailable rather than as zero retention.

---

## Products

The Products tab analyzes product-level sales performance using:

```text
marts.fact_order_items
```

Key metrics include:

* Product revenue
* Units sold
* Orders
* Average unit price
* Realized selling price
* Product revenue concentration
* Product performance over time
* Product-level conversion behavior

Product revenue is calculated from item-level:

```text
line_amount
```

rather than order-level revenue, preventing the same order total from being incorrectly attributed to multiple products.

---

# 🗄️ Analytics Data Model

The primary analytical fact tables are:

## `fact_orders`

```text
order_id
user_id
session_id
order_ts
order_date
order_status
item_count
order_total_amount
```

Used for:

* Revenue
* Orders
* Customers
* AOV
* Customer analytics
* Order trends

---

## `fact_order_items`

```text
order_id
product_id
user_id
session_id
quantity
price
line_amount
order_ts
order_date
```

Used for:

* Product revenue
* Units sold
* Product rankings
* Product trends
* Product-level analysis

---

## `bronze.clickstream`

The event-level behavioral dataset contains:

```text
event_id
event_type
user_id
session_id
product_id
event_time
ingest_time
version
device
country
user_agent
referrer
experiment_id
```

This table powers behavioral and funnel analytics.

---

# 🐳 Dockerized Environment

The project runs inside Docker so the complete pipeline can be reproduced without installing the entire analytics stack directly on the host machine.

The container includes the project's:

* Python environment
* DuckDB
* dbt
* Streamlit
* Pipeline dependencies
* Analytics code

The DuckDB warehouse is stored at:

```text
/data-lake/warehouse.duckdb
```

and the data lake is mounted separately from the application code.

---

# 🚀 Quickstart

## Prerequisites

Install:

* Docker
* Docker Compose
* Make
* Git

---

## Clone the Repository

```bash
git clone https://github.com/mattyg3/ecommerce_analytics_platform.git

cd ecommerce_analytics_platform
```

---

## Configure the Environment

```bash
cp .env.example .env
```

If required by your Docker environment, configure your host UID/GID in `.env`.

---

## Initialize the Environment

```bash
make setup
```

---

## Build and Start Docker

```bash
make up
```

---

## Run the Pipeline

Enter the running container:

```bash
docker exec -it analytics_platform bash
```

Then run the pipeline:

```bash
bash orchestration/run_pipeline.sh [HOURS_TO_SIMULATE] [START_DATE: 'YYYY-MM-DD']
```

The pipeline performs the core workflow:

```text
Generate Data
     ↓
Ingest
     ↓
Bronze
     ↓
dbt
     ↓
Analytics Marts
```

---

## Launch the Dashboard

From inside the container:

```bash
streamlit run streamlit_app.py --server.address=0.0.0.0
```

Then open:

```text
http://localhost:8501
```

---

# 🛠️ Common Commands

| Command             | Purpose                                |
| ------------------- | -------------------------------------- |
| `make setup`        | Initialize the environment             |
| `make up`           | Build and start Docker                 |
| `make down`         | Stop the application                   |
| `make full-refresh` | Reset and rebuild the data environment |
| `make reset-all`    | Perform a complete reset and rebuild   |
| `make rebuild`      | Rebuild/restart the container          |
| `make clean`        | Remove generated environment artifacts |

---

# 🔍 Querying DuckDB

The warehouse can be queried directly from inside the container.

For example:

```bash
duckdb /data-lake/warehouse.duckdb
```

Then:

```sql
SHOW TABLES;
```

Check the analytics marts:

```sql
SELECT *
FROM marts.fact_orders
LIMIT 10;
```

Revenue by day:

```sql
SELECT
    order_date,
    SUM(order_total_amount) AS revenue,
    COUNT(DISTINCT order_id) AS orders
FROM marts.fact_orders
GROUP BY order_date
ORDER BY order_date;
```

Product performance:

```sql
SELECT
    product_id,
    SUM(quantity) AS units_sold,
    SUM(line_amount) AS revenue
FROM marts.fact_order_items
GROUP BY product_id
ORDER BY revenue DESC;
```

---

# 🧪 Reproducibility

The project is intentionally designed so the entire analytical environment can be created locally.

A typical workflow is:

```text
Clone repository
      ↓
Start Docker environment
      ↓
Generate synthetic data
      ↓
Run ingestion
      ↓
Build dbt models
      ↓
Query DuckDB
      ↓
Explore Streamlit dashboard
```

No external cloud warehouse is required.

---

# 🎯 Engineering Design Decisions

### DuckDB instead of a traditional database server

DuckDB provides a powerful analytical SQL engine while keeping the project lightweight and portable.

This makes it possible to demonstrate warehouse-style analytics without requiring PostgreSQL, Snowflake, BigQuery, or another external service.

### Parquet for the data lake

Parquet provides columnar storage and efficient analytical reads while maintaining a simple local-file architecture.

### dbt for transformations

dbt separates analytical transformation logic from ingestion code and provides a clear modeling structure for:

```text
staging → intermediate → marts
```

### Synthetic data generation

Synthetic data makes the project reproducible and allows controlled simulation of real-world data conditions such as:

* Late events
* Repeated users
* Multiple sessions
* Different event types
* Variable traffic
* Batch vs. streaming workloads

### Streamlit for analytics

Streamlit provides a lightweight way to expose the resulting warehouse models to business users without introducing a separate frontend application.

---

# 📌 Portfolio Focus

This project was built to demonstrate practical experience across both **data engineering and analytics**.

It brings together:

```text
Python
  +
Data Ingestion
  +
Parquet
  +
DuckDB
  +
dbt
  +
Incremental Processing
  +
Docker
  +
SQL
  +
Streamlit
```

The emphasis is not simply on producing charts, but on building the underlying data platform that makes those analytics possible.

---

# 🔮 Potential Future Enhancements

Possible extensions include:

* Automated data-quality tests
* dbt documentation and lineage visualization
* CI/CD with GitHub Actions
* Additional customer segmentation
* Product-level conversion analysis
* Experiment / A/B test analytics
* Automated dashboard exports
* Pipeline monitoring and data-quality reporting
* Airflow or another workflow orchestrator
* Cloud deployment using object storage and a cloud warehouse
* Natural-language analytics using a local LLM

---

# 📄 License

This project is licensed under the MIT License.

