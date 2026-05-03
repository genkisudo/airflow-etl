# Adzuna Job ETL Pipeline

A production-ready data pipeline that extracts job listings from the Adzuna API, transforms them with PySpark, and loads them into Databricks Delta tables—fully orchestrated with Apache Airflow and containerized with Docker.

## What It Does

- **Extracts** job listings from Adzuna API (paginated, error-tolerant)
- **Transforms** raw JSON into structured, deduplicated Spark DataFrames
- **Loads** data into Databricks Delta using idempotent MERGE logic (no duplicates on re-runs)
- **Schedules** hourly runs with Apache Airflow and monitors via logs

**Tech Stack:** Apache Airflow 3.0.3 · PySpark · Databricks Connect · PostgreSQL · Redis · Docker

## Architecture

```
Adzuna API → Spark DataFrame → Databricks Delta Table
                ↑
        Orchestrated by Airflow
        (PostgreSQL metadata, Redis queue)
```

**Key Features:**
- **Idempotent MERGE:** Null-safe comparisons ensure updates only on actual changes
- **Serverless Compute:** Databricks serverless SQL warehouses (no cluster cost when idle)
- **Error Resilience:** API timeouts and failures log warnings but don't fail the DAG
- **No Backfill:** `catchup=False` prevents replaying old intervals

## Quick Start

### Prerequisites
- [Adzuna API account](https://developer.adzuna.com/) (free, get Application ID & Key)
- [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
- Docker Desktop (macOS/Windows/Linux)

### Setup (5 min)

1. **Clone & navigate:**
   ```bash
   git clone https://github.com/genkisudo/airflow-etl.git
   cd airflow-etl
   docker compose up -d
   ```

2. **Configure Airflow** (`http://localhost:8080` — login: airflow/airflow):
   - Add Variables: `ADZUNA_APP_ID` and `ADZUNA_APP_KEY`
   - Add Connection: ID=`databricks_default`, Type=Databricks, Host=your Databricks URL, Password=PAT token

3. **Create Databricks catalog/schema/table:**
   ```bash
   docker compose exec -T airflow-scheduler python /opt/python/scripts/databricks_ddl.py
   ```

4. **Run the DAG:**
   - Unpause `load_adzuna_jobs` in Airflow UI
   - Triggers hourly or click play to test immediately

5. **Verify:** Check `adzuna.prj.jobs` table in Databricks for job listing data

## Project Structure

```
airflow_databricks_etl/
├── airflow/dags/etl.py           # DAG definition
├── airflow/config/etl_config.py  # Centralized config (schedule, API params)
├── scripts/src.py                # ETL logic (fetch, transform, merge)
├── scripts/databricks_ddl.py     # Catalog/table bootstrap
├── docker-compose.yaml           # Full stack (Airflow, Postgres, Redis)
└── pyproject.toml                # Dependencies
```

## Learnings & Implementation Highlights

- **Pagination:** Robust error handling for flaky APIs (individual page failures don't break the pipeline)
- **Data Quality:** Deduplication on ID + null-safe comparisons prevent unwanted updates
- **Idempotency:** Re-runs are safe; MERGE logic only updates changed fields
- **Efficiency:** Temp views avoid unnecessary data transfer between Spark and Delta

---

**Learning project from [Surfalytics](https://surfalytics.com/) Data Engineering Community.**