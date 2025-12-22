# OpenAQ Data Pipeline - AI Agent Instructions

## Project Overview

Airflow 2.7.1 ETL pipeline extracting air quality data from OpenAQ API v3 for Vietnam, processing with AWS Glue/Spark, storing in S3 (Parquet), and querying via Athena → OWOX → Looker Studio.

**Tech Stack**: Airflow LocalExecutor, PostgreSQL metadata DB, Docker, AWS (S3/Glue/Athena), PySpark 3.4.1, Python 3.11

## Architecture Pattern

### Data Flow
```
OpenAQ API → Airflow (extract) → S3 Raw (JSON.gz) → Glue Spark (transform) → S3 Marts (Parquet) → Athena → OWOX → Looker
```

### Three-Zone S3 Structure
- `s3://openaq-data-pipeline/aq_raw/` - Immutable raw JSON.gz from API
- `s3://openaq-data-pipeline/aq_dev/` - Dev zone for Parquet (testing ETL changes)
- `s3://openaq-data-pipeline/aq_prod/` - Prod zone for Parquet (production dashboards)

Environment selection: `PIPELINE_ENV` env var (`dev`|`prod`), defaults to `dev`. See [utils/constants.py](utils/constants.py) for `ENV_FOLDER_MAP`.

## Critical Setup Requirements

### Before Running ANYTHING
1. **Create config file**: `cp config/config.conf.example config/config.conf` (Windows: `copy`)
2. **Fill credentials**: Edit `config/config.conf` with AWS keys, OpenAQ API key, database passwords
3. **Never commit**: `config/config.conf` is gitignored - credentials stay local only

### Configuration System
- **Single source of truth**: [utils/constants.py](utils/constants.py) loads all config via `ConfigParser`
- **Environment-aware naming**: Glue database/crawler names auto-append `_dev` or `_prod` based on `ENV`
- **Legacy naming**: PostgreSQL DB is `airflow_reddit` (historical, ignore the name)

## Development Workflows

### Docker (Primary Method)
```bash
# First time setup
docker-compose up -d            # Starts postgres, airflow-init, webserver, scheduler
# Login: http://localhost:8080 (admin/admin)

# After code changes (Python files hot-reload automatically)
# Only rebuild if requirements.txt changes:
docker-compose build && docker-compose up -d

# Debugging
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
docker-compose down  # Stop all
```

**Key insight**: All code directories (`dags/`, `etls/`, `pipelines/`, `utils/`) are volume-mounted. Editing files on host immediately reflects in containers (Airflow rescans DAGs every 30s).

### Testing Pattern
```bash
# Run from project root or Docker container
pytest tests/                    # All tests
pytest tests/test_extract_data.py -v  # Specific test with verbose output
pytest tests/ --cov=utils --cov=pipelines  # With coverage
```

Test files follow naming: `test_<module>.py`. Example: [tests/test_extract_data.py](tests/test_extract_data.py) shows full pipeline validation.

## Code Patterns & Conventions

### Status Indicators (Text-Based, No Emojis)
Always use these prefixes in print statements:
- `[INFO]` - Informational messages
- `[OK]` - Operation succeeded
- `[SUCCESS]` - Task/pipeline completed
- `[FAIL]` - Operation failed (before raising exception)
- `[WARNING]` - Non-critical issues
- `[START]` - Task/pipeline starting

**Examples**: See [etls/openaq_etl.py](etls/openaq_etl.py#L30), [glue_jobs/process_openaq_raw.py](glue_jobs/process_openaq_raw.py#L35-L55)

### Pipeline Architecture (Separation of Concerns)

1. **Extraction Layer** (`etls/openaq_etl.py`):
   - Pure API interaction - fetch raw data, minimal transformation
   - REST API pattern: `connect_openaq()` → `fetch_all_vietnam_locations()` → `extract_measurements()`
   - Returns Python dictionaries/lists, NOT DataFrames

2. **Orchestration Layer** (`pipelines/openaq_pipeline.py`):
   - Calls ETL functions, uploads raw JSON to `aq_raw/` zone
   - **Does NOT transform data** - transformation moved to Spark for scalability
   - Triggered by Airflow DAG tasks

3. **Transform Layer** (`glue_jobs/process_openaq_raw.py`):
   - PySpark job reading `aq_raw/`, writing partitioned Parquet to `aq_dev/` or `aq_prod/`
   - Handles: datetime parsing, deduplication (location_id + timestamp), pivot from long→wide format, enrichment
   - Glue Job parameters: `--input_path`, `--output_path`, `--env`, `--partition_cols`

4. **Task Layer** (`dags/tasks/*.py`):
   - Factory functions creating Airflow operators
   - Example: `create_catalog_tasks()` returns `(trigger_task, wait_task)` tuple
   - Separates task creation from DAG definition

### DAG Structure Pattern
See [dags/openaq_dag.py](dags/openaq_dag.py):
```python
# 1. Extract raw data (nationwide Vietnam, not city-by-city)
extract_task = PythonOperator(..., python_callable=openaq_pipeline, op_kwargs={'vietnam_wide': True})

# 2. Transform with Spark (trigger + wait for Glue job)
trigger_glue, wait_glue = create_glue_transform_tasks(dag)

# 3. Catalog with Glue Crawler (trigger + wait for completion)
trigger_crawler, wait_crawler = create_catalog_tasks(dag)

# 4. Validate Athena accessibility
validate_task = create_validate_athena_task(dag)

# Dependencies: extract >> trigger_glue >> wait_glue >> trigger_crawler >> wait_crawler >> validate
```

**Retry strategy**: DAG default is 2 retries, 5min delay, exponential backoff. Task-specific retries vary (extractors: 2, triggers: 3, sensors: 0).

### S3 Upload Pattern
[utils/aws_utils.py](utils/aws_utils.py#L30-L70): `upload_to_s3()` supports `format='json'|'parquet'`
- JSON: Uses `lines=True` for NDJSON format (Spark/Glue compatible)
- Parquet: `engine='pyarrow'`, `compression='snappy'`, `index=False`
- Always prints `[SUCCESS] Uploaded {count} records to s3://...` on completion

### AWS Client Creation
Use `get_s3_client()`, `get_glue_client()`, `get_athena_client()` from `utils/aws_utils.py` and `utils/glue_utils.py`. They handle:
- Credentials from `config/config.conf` via constants
- Session token support (for temporary credentials)
- Region configuration

### Sensor Pattern for Async Jobs
Example: [dags/tasks/catalog_tasks.py](dags/tasks/catalog_tasks.py#L38-L57)
```python
PythonSensor(
    task_id='wait_for_crawler',
    python_callable=check_crawler_status,  # Returns True when READY
    poke_interval=60,  # Check every 60s
    timeout=1800,      # Fail after 30min
    mode='poke'
)
```
Sensors poll AWS services (Glue Crawler, Glue Job) until completion. `mode='poke'` blocks the worker.

## Common Pitfalls

1. **Missing config file**: If you see `FileNotFoundError: config/config.conf not found`, you forgot step 1 in setup
2. **Coordinate order bug**: OpenAQ API expects `lat,lng` (not `lng,lat`). Fixed in [etls/openaq_etl.py#L34](etls/openaq_etl.py#L34)
3. **JSON format for Spark**: Always use `lines=True` in `to_json()` for NDJSON (newline-delimited). Standard JSON arrays break Glue jobs.
4. **NumPy version**: Must use `numpy<2.0.0` (see requirements.txt) - NumPy 2.x breaks PySpark 3.4.1 compatibility
5. **Java for Spark**: Dockerfile installs OpenJDK 17 and sets `JAVA_HOME` - required for local PySpark testing
6. **Environment variables in Glue**: Glue jobs use `--arguments` (not env vars). Pass `--env dev|prod` explicitly.

## File Navigation Quick Reference

- **DAG definition**: [dags/openaq_dag.py](dags/openaq_dag.py)
- **API extraction**: [etls/openaq_etl.py](etls/openaq_etl.py)
- **Pipeline orchestration**: [pipelines/openaq_pipeline.py](pipelines/openaq_pipeline.py)
- **Spark transformation**: [glue_jobs/process_openaq_raw.py](glue_jobs/process_openaq_raw.py)
- **Configuration loader**: [utils/constants.py](utils/constants.py)
- **AWS utilities**: [utils/aws_utils.py](utils/aws_utils.py), [utils/glue_utils.py](utils/glue_utils.py), [utils/athena_utils.py](utils/athena_utils.py)
- **Task factories**: [dags/tasks/catalog_tasks.py](dags/tasks/catalog_tasks.py), [dags/tasks/glue_transform_tasks.py](dags/tasks/glue_transform_tasks.py), [dags/tasks/validation_tasks.py](dags/tasks/validation_tasks.py)
- **Architecture docs**: [doc/architecture.md](doc/architecture.md) (in Vietnamese)
- **Existing AI guide**: [CLAUDE.md](CLAUDE.md) (comprehensive reference)

## When Editing Code

- **Always check [CLAUDE.md](CLAUDE.md) first** - it contains detailed implementation notes and common issues
- **Preserve status indicators** - all operations should print `[INFO]`/`[OK]`/`[FAIL]` messages
- **Follow three-zone pattern** - raw data → `aq_raw/`, transformed data → `aq_dev/` or `aq_prod/`
- **Test imports resolution** - Airflow containers use `sys.path.insert(0, '/opt/airflow/')` to resolve modules
- **Update environment-aware resources** - if adding new Glue/Athena resources, use `f"{resource_name}_{ENV}"` pattern
