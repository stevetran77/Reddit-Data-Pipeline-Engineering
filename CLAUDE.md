# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an OpenAQ Air Quality Data Pipeline using Apache Airflow 3.1.5 with Python 3.11. The project implements an end-to-end ETL pipeline that extracts air quality data from OpenAQ API, loads to S3 (Parquet), catalogs with AWS Glue, transforms and loads to Redshift, and visualizes with Looker. The architecture uses Airflow's CeleryExecutor with PostgreSQL for metadata storage and Redis as the message broker.

## Architecture

### Data Flow

```
OpenAQ API → Airflow (extract) → S3 (Parquet) → Glue Crawler → Glue ETL Job → Redshift → Looker
```

The pipeline orchestrates these steps through a single Airflow DAG (`openaq_to_redshift_pipeline`):

1. **Extract Phase**: Parallel extraction tasks for multiple cities (Hanoi, HCMC) call OpenAQ API and upload partitioned Parquet files to S3
2. **Catalog Phase**: Glue Crawler scans S3 and updates Data Catalog schema
3. **Transform & Load Phase**: Glue ETL Job reads from catalog, deduplicates, applies quality checks, and loads to Redshift
4. **Validation Phase**: Verifies data loaded successfully to Redshift

### Core Components

- **Airflow Orchestration**: Manages DAG workflows with CeleryExecutor for parallel task execution
- **OpenAQ Integration**: `etls/openaq_etl.py` handles API connection, location extraction, and measurement retrieval
- **AWS Services**: S3 for data lake, Glue for cataloging and ETL, Redshift for data warehouse
- **Data Partitioning**: S3 files organized by `airquality/{city}/year={Y}/month={M}/day={D}/data.parquet`
- **PostgreSQL Database**: Stores Airflow metadata (database name: `airflow_reddit` - legacy naming)
- **Redis**: Message broker for Celery task queue

### Directory Structure

- `dags/` - Airflow DAG definitions (`openaq_dag.py` defines the 7-task pipeline)
- `pipelines/` - High-level pipeline orchestration (`openaq_pipeline.py`, `glue_pipeline.py`)
- `etls/` - ETL modules (`openaq_etl.py` for API interactions)
- `utils/` - Utility modules (`aws_utils.py`, `glue_utils.py`, `redshift_utils.py`, `constants.py`)
- `config/` - Configuration files (`config.conf` contains all credentials - gitignored, `config.conf.example` is template)
- `sql/` - SQL scripts (`redshift_schema.sql` for warehouse schema)
- `glue_jobs/` - Glue ETL scripts to upload to S3 (`openaq_to_redshift.py`)
- `tests/` - Unit tests with pytest (`test_glue_utils.py`, `test_redshift_utils.py`)
- `doc/` - Documentation (`OPENAQ_PIPELINE_PLAN.md` contains detailed implementation plan)

### Configuration System

All configuration is centralized in `config/config.conf` (not in version control). The `utils/constants.py` module loads configuration using ConfigParser and exports constants:

- `[database]` - PostgreSQL connection for Airflow metadata
- `[aws]` - AWS credentials and S3 bucket
- `[aws_glue]` - Glue database, crawler, ETL job names
- `[aws_redshift]` - Redshift cluster connection details
- `[api_keys]` - OpenAQ API key
- `[openaq_settings]` - Default city, country, lookback hours
- `[etl_settings]` - Batch size, error handling, log level

**Critical**: Always create `config/config.conf` from `config/config.conf.example` before running the pipeline.

### Status Indicators

The codebase uses text-based status indicators (no emojis):
- `[OK]` - Operation succeeded
- `[FAIL]` - Operation failed
- `[SUCCESS]` - Task/pipeline completed successfully
- `[WARNING]` - Non-critical issue
- `[INFO]` - Informational message
- `[START]` - Task/pipeline starting

## Development Setup

### Initial Setup

```bash
# Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Create configuration from template
copy config\config.conf.example config\config.conf  # Windows
cp config/config.conf.example config/config.conf  # Linux/Mac
# Edit config/config.conf with your credentials

# Initialize Airflow database
airflow db init
airflow db upgrade

# Create admin user
airflow users create --username admin --firstname admin --lastname admin --role Admin --email airflow@airflow.com --password admin
```

### Running with Docker

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-worker

# Stop services
docker-compose down

# Rebuild after requirements.txt changes
docker-compose build
docker-compose up -d
```

Services:
- Airflow Webserver: http://localhost:8080 (admin/admin)
- PostgreSQL: localhost:5432 (postgres/postgres)
- Redis: localhost:6379

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_glue_utils.py

# Run with verbose output
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=utils --cov=pipelines --cov=etls
```

## Key Implementation Patterns

### Pipeline Execution Flow

All pipelines follow this structure:

1. **Extraction** (`etls/openaq_etl.py`):
   - `connect_openaq()` - Create authenticated API client
   - `extract_locations()` - Get monitoring location IDs by city/country
   - `extract_measurements()` - Fetch hourly air quality data with lookback period
   - `transform_measurements()` - Convert to DataFrame with pivot table structure

2. **Loading** (`pipelines/openaq_pipeline.py`):
   - `openaq_pipeline()` - Orchestrates extract → transform → S3 upload
   - Uses `upload_to_s3_partitioned()` from `utils/aws_utils.py`
   - Partitioning: `year=YYYY/month=MM/day=DD/data.parquet`

3. **Cataloging** (`pipelines/glue_pipeline.py`):
   - `trigger_crawler_task()` - Start Glue Crawler
   - `check_crawler_status()` - Poll crawler until READY state
   - Crawler creates/updates Glue Data Catalog tables

4. **Transformation** (`glue_jobs/openaq_to_redshift.py`):
   - Runs in AWS Glue environment (PySpark)
   - Deduplication by location_id + measurement_datetime
   - Data quality checks (null values, coordinate validation)
   - Writes to Redshift via JDBC connection

5. **Validation** (`pipelines/glue_pipeline.py`):
   - `validate_redshift_load()` - Check row count in fact_measurements table

### DAG Task Dependencies

The main DAG (`dags/openaq_dag.py`) implements this task graph:

```
[extract_hanoi, extract_hcmc] (parallel)
    ↓
trigger_glue_crawler
    ↓
wait_for_crawler (PythonSensor, 60s poke interval, 30min timeout)
    ↓
trigger_glue_etl_job
    ↓
wait_for_glue_job (PythonSensor, 120s poke interval, 60min timeout)
    ↓
validate_redshift_data
```

### Error Handling

- DAG-level retries: 2 retries with 5-minute delay, exponential backoff enabled
- Task-specific retries vary (extractors: 2, triggers: 3, sensors: 0)
- All functions print status indicators before raising exceptions
- XCom used to pass crawler/job identifiers between tasks

## AWS Infrastructure Requirements

The pipeline requires these AWS resources (created manually via Console):

1. **S3 Bucket**: `openaq-data-pipeline`
   - Folders: `airquality/`, `scripts/`, `temp/`

2. **Glue Database**: `openaq_database`

3. **Glue Crawler**: `openaq_s3_crawler`
   - Data source: `s3://openaq-data-pipeline/airquality/`
   - Creates tables with prefix: `aq_`

4. **Glue ETL Job**: `openaq_to_redshift`
   - Script location: Upload `glue_jobs/openaq_to_redshift.py` to `s3://openaq-data-pipeline/scripts/`
   - Glue version: 4.0
   - Worker type: G.1X (2 workers)
   - Requires Glue Connection to Redshift

5. **Glue Connection**: `RedshiftConnection`
   - Type: JDBC
   - URL: `jdbc:redshift://<cluster>:5439/openaq_warehouse`

6. **Redshift Cluster**: `openaq-warehouse`
   - Database: `openaq_warehouse`
   - Schema: Created by running `sql/redshift_schema.sql`
   - Tables: `openaq.fact_measurements`, `openaq.dim_locations`

7. **IAM Roles**:
   - `GlueServiceRole` - Glue access to S3 and Redshift
   - `RedshiftS3Role` - Redshift COPY from S3

## Configuration Notes

### Environment Variables (airflow.env)

- `AIRFLOW__CORE__EXECUTOR`: CeleryExecutor
- `AIRFLOW__CELERY__BROKER_URL`: Redis connection
- `AIRFLOW__CELERY__RESULT_BACKEND`: PostgreSQL for task results
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: PostgreSQL metadata DB
- `AIRFLOW__CORE__FERNET_KEY`: Encryption key (rotate for production)
- `AIRFLOW__CORE__LOAD_EXAMPLES`: False

### Docker Volume Mounts

All code directories are mounted as volumes for live editing without rebuilds:
- `./config` → `/opt/airflow/config`
- `./dags` → `/opt/airflow/dags`
- `./etls` → `/opt/airflow/etls`
- `./pipelines` → `/opt/airflow/pipelines`
- `./utils` → `/opt/airflow/utils`
- `./tests` → `/opt/airflow/tests`

Changes to Python files are immediately available (Airflow scheduler rescans DAGs every 30 seconds).

## Debugging

- **Airflow Task Logs**: Airflow UI → DAG → Task Instance → Logs tab
- **Container Logs**: `docker-compose logs -f <service-name>`
- **PostgreSQL Metadata**: Connect to localhost:5432, database `airflow_reddit`
- **Glue Job Logs**: AWS Console → Glue → Jobs → Run details → CloudWatch logs
- **Redshift Query Monitoring**: AWS Console → Redshift → Query monitoring

### Common Issues

1. **"config.conf not found"**: Create `config/config.conf` from template
2. **"No module named 'openaq'"**: Run `pip install -r requirements.txt` or rebuild Docker
3. **Crawler fails**: Check S3 path and IAM role permissions
4. **Glue job fails**: Verify Glue Connection to Redshift is configured correctly
5. **Sensor timeout**: Check crawler/job is actually running in AWS Console

## Key Dependencies

- **apache-airflow**: 3.1.5 (scheduler and webserver)
- **openaq**: 0.2.0 (OpenAQ API client)
- **boto3**: 1.35.0 (AWS SDK)
- **awswrangler**: 3.9.1 (AWS data wrangling for Glue)
- **redshift-connector**: 2.1.4 (Redshift database connector)
- **pandas**: 2.3.3 (data manipulation)
- **pyarrow**: 18.1.0 (Parquet support)
- **sqlalchemy**: 2.0.45 (database ORM)
