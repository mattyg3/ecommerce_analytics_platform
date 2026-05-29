# 🛒 Ecommerce Analytics Platform

An end-to-end analytics platform that ingests versioned streaming clickstream events and batch order data, transforms them through late-event–safe incremental dbt models, and surfaces insights in an interactive Streamlit dashboard — all orchestrated within a fully Dockerized environment.

---

## Architecture Overview

```
Producers (synthetic data generators)
        │
        ▼
Ingestion layer (streaming clickstream + batch orders)
        │
        ▼
Data Lake (Parquet / DuckDB warehouse)
        │
        ▼
dbt transformations (staging → intermediate → marts)
        │
        ▼
Spark jobs (automated batch transformations)
        │
        ▼
Streamlit dashboard (port 8501)
```

### Data Flow

- **Producers** generate synthetic clickstream events (streaming) and order records (batch), written to the local data lake.
- **Ingestion** handles schema versioning and late-event safety, ensuring records that arrive out-of-order are correctly incorporated without reprocessing clean data.
- **dbt** models transform raw ingested data through a medallion-style layer (staging → intermediate → marts), with incremental models to support efficient, production-grade refreshes.
- **Spark jobs** provide automated batch transformations on top of the data lake.
- **Orchestration** scripts coordinate the pipeline end-to-end.
- **Streamlit** reads directly from the DuckDB warehouse and serves an interactive analytics dashboard.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data transformation | dbt-core 1.8, dbt-duckdb 1.8 |
| Analytical engine | DuckDB 0.10 |
| Batch processing | Apache Spark |
| Orchestration | Airflow / pipeline scripts |
| Dashboard | Streamlit 1.33 |
| Serialization | Pandas 2.2, PyArrow 15 |
| Containerization | Docker / Docker Compose |

---

## Project Structure

```
ecommerce_analytics_platform/
├── dbt_project/         # dbt models (staging, intermediate, marts)
├── ingestion/           # Clickstream & order ingestion logic
├── orchestration/       # Pipeline orchestration scripts
├── producers/           # Synthetic data generators
├── scripts/             # Setup and environment scripts
├── spark_jobs/          # Spark batch transformation jobs
├── streamlit_app.py     # Analytics dashboard
├── Dockerfile
├── docker-compose.yml
├── Makefile             # Convenience commands
└── requirements.txt
```

---

## Dashboard

The Streamlit app (`streamlit_app.py`) reads from `marts.fact_orders` in the DuckDB warehouse and provides four tabs:

- **Overview** — Revenue, orders, customers, AOV, and time-series trend charts with cumulative views
- **Funnel** — Conversion funnel (in progress)
- **Customers** — LTV distribution and repeat vs. one-time customer breakdown
- **Products** — Top product insights (in progress)

All tabs support date-range filtering via the sidebar.

---

## Quickstart

### Prerequisites

- Docker & Docker Compose
- `make`

### Setup

```bash
# Clone the repo
git clone https://github.com/mattyg3/ecommerce_analytics_platform.git
cd ecommerce_analytics_platform

# Configure environment
cp .env.example .env
# Edit .env to set your UID/GID if needed

# Initialize data lake and run setup
make setup

# Build and start the container
make up
```

### Inside the container

```bash
# Run the full pipeline (ingest → dbt → spark)
python orchestration/run_pipeline.py

# Launch the Streamlit dashboard
streamlit run streamlit_app.py
# Access at http://localhost:8501
```

### Common Make commands

| Command | Description |
|---|---|
| `make setup` | Initialize environment and data lake |
| `make up` | Build image and start container |
| `make down` | Stop container |
| `make full-refresh` | Reset data lake and re-run setup |
| `make reset-all` | Full refresh + rebuild |
| `make clean` | Tear down volumes and clear logs |
| `make rebuild` | Stop and restart the container |

---

## dbt Models

Models follow a standard layered approach:

- **Staging** — Raw source data with light cleaning and type casting
- **Intermediate** — Business logic, joins, and event deduplication
- **Marts** — Final analytical tables (`fact_orders`, etc.) consumed by the dashboard

Incremental models use late-event–safe logic to avoid reprocessing historical partitions while still capturing delayed arrivals.

---

## Configuration

The only required environment variables are:

```env
UID=<your user id>
GID=<your group id>
```

Copy `.env.example` to `.env` and update values as needed. The DuckDB warehouse is written to `/data-lake/warehouse.duckdb` inside the container (mounted from `./data-lake` on the host).

---

## License

MIT
