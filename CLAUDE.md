# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow-based ETL pipeline that extracts job listing data from the Adzuna API, transforms it using PySpark, and loads it into Databricks Delta tables. The entire system runs in Docker with PostgreSQL (metadata) and Redis (task queue).

**Key Technologies:**
- Apache Airflow 3.0.3 (orchestration)
- PySpark + Databricks Connect (transformation & compute)
- PostgreSQL + Redis (Airflow infrastructure)
- Docker Compose (local development)
- Python 3.12

## Architecture

### Directory Structure
```
airflow_databricks_etl/
├── airflow/                      # Airflow home directory
│   ├── dags/etl.py              # DAG definition (single task that runs the full ETL)
│   ├── config/etl_config.py     # Configuration (schedule, credentials, Databricks target)
│   └── config/airflow.cfg       # Airflow configuration
├── scripts/src.py                # ETL business logic (fetch, transform, merge)
├── scripts/databricks_ddl.py     # Setup script for catalog/schema/table creation
├── docker-compose.yaml           # Multi-container setup (Airflow, Postgres, Redis)
├── Dockerfile                    # Custom Airflow image with databricks-connect + dependencies
└── pyproject.toml               # Python dependencies (munch, databricks-connect)
```

### Data Pipeline Flow

1. **Fetch** (`fetch_adzuna_to_spark_df`): Paginate through Adzuna API, flatten nested JSON into a Spark DataFrame with fields like `id`, `title`, `location`, `company`, `category`, `description`, `url`, `created`.

2. **Transform** (in `fetch_adzuna_to_spark_df`):
   - Drop duplicates on `id`
   - Cast `id` to bigint, `created` to timestamp
   - Add `etl_updated` timestamp column

3. **Merge** (`merge_to_delta_table`): MERGE INTO Databricks Delta table with:
   - MATCHED: Update if any field changed (using null-safe comparison `<=>`)
   - NOT MATCHED: Insert new records
   - Updates set `etl_at` timestamp on changes

### Configuration

- **Schedule**: `@hourly` (configurable in `etl_config.py`)
- **Adzuna params**: Job search keyword (default: "data engineer"), country (default: "ca"), max days old (default: 3)
- **Target Databricks**: Catalog `adzuna`, schema `prj`, table `jobs`
- **Credentials**: ADZUNA_APP_ID and ADZUNA_APP_KEY stored as Airflow variables; Databricks PAT via connection named `databricks_default`

## Commands

### Local Development

**Start Airflow (with all dependencies):**
```bash
docker compose up -d
```

**Stop services:**
```bash
docker compose down
```

**View Airflow UI:**
- URL: `http://localhost:8080/`
- Login: `airflow` / `airflow`

**View logs for a specific service:**
```bash
docker compose logs airflow-scheduler
docker compose logs airflow-worker
```

**Run the DDL setup script (creates catalog/schema/table in Databricks):**
```bash
docker compose exec -T airflow-scheduler python /opt/python/scripts/databricks_ddl.py
```

**Execute Python scripts inside the container:**
```bash
docker compose exec -T airflow-scheduler python /opt/python/scripts/src.py
```

**Rebuild the custom Docker image (after Dockerfile changes):**
```bash
docker compose build --no-cache
```

### Debugging

**Shell into scheduler container:**
```bash
docker compose exec -T airflow-scheduler bash
```

**View Airflow logs:**
```bash
docker compose logs -f airflow-scheduler
```

**Check database connection:**
```bash
docker compose exec -T airflow-scheduler python -c "from scripts.src import get_spark; s=get_spark(); print(s.sql('select 1').collect())"
```

## Key Implementation Details

### DAG (`airflow/dags/etl.py`)
- DAG ID: `adzuna_etl_dag`
- Single `@task` decorated function `load_adzuna_jobs()` that orchestrates the full ETL
- Retrieves API credentials from Airflow Variables (ADZUNA_APP_ID, ADZUNA_APP_KEY)
- Creates Spark session using `DatabricksHook`, uses serverless compute
- Calls fetch → merge → count → log → close spark
- No task dependencies; single task per DAG run

### ETL Logic (`scripts/src.py`)
- `get_spark()`: Creates remote Spark session via Databricks Connect with serverless=True
- `fetch_adzuna_to_spark_df()`: Handles pagination (reads total count from page 1, fetches remaining pages, error tolerant)
- Returns None if no results; returns deduped Spark DataFrame with transformed columns otherwise
- `merge_to_delta_table()`: Uses temp view to avoid data transfer; null-safe comparisons prevent unnecessary updates
- `get_table_records_cnt()`: Quick count for logging/validation

### Configuration (`airflow/config/etl_config.py`)
- Uses `munch.Munch` for dot-notation access (e.g., `config.target_catalog`)
- Airflow Variables fetched at module load time (not runtime), so they must exist before DAG runs
- All config is centralized; avoid hardcoding in DAG or scripts

## Setup & First Run

1. Set environment variables / Airflow Variables (ADZUNA_APP_ID, ADZUNA_APP_KEY)
2. Configure Databricks connection: Connection ID = `databricks_default`, Host = Databricks URL, Password = PAT token
3. Run `docker compose exec -T airflow-scheduler python /opt/python/scripts/databricks_ddl.py` to create the target table
4. Unpause the DAG in the UI; it will run on the next schedule interval

## Notes

- **Error handling**: API fetch has timeout and status code checks; individual page failures log warnings but don't fail the DAG
- **Idempotency**: The MERGE logic ensures re-runs don't create duplicates; updates only on actual changes
- **No backfill by default**: `catchup = False` in config to avoid replaying old schedule intervals
- **Serverless compute**: Databricks Connect uses serverless SQL warehouses (no cluster cost when idle)
