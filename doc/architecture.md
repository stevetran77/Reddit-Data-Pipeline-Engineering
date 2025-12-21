### **File: `architecture.md**`

```markdown
# Architecture: OpenAQ Local Lakehouse

## Executive Summary
The OpenAQ Data Pipeline is a **Local Docker Lakehouse** that leverages the power of AWS Serverless services. Orchestration runs locally on **Docker Desktop** (Airflow LocalExecutor) to minimize costs and improve development velocity. Heavy data processing is offloaded to **AWS Glue ETL Jobs**, ensuring the local machine acts only as a trigger, not a compute node. Data is stored in **S3** and queried via **Amazon Athena**, creating a robust, zero-maintenance data platform.

## Project Initialization
To set up the environment:
```bash
# 1. Configure AWS CLI with IAM User credentials
aws configure

# 2. Initialize local project structure
mkdir -p dags/{raw,dev,prod} etls glue_jobs pipelines utils config sql/athena_views
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

```

## Decision Summary

| Category | Decision | Version | Rationale |
| --- | --- | --- | --- |
| **Orchestration** | **Airflow (LocalExecutor)** | 2.7+ | Runs efficiently on local Docker. No need for Redis/Celery complexity. |
| **Ingestion** | **Airflow PythonOperator** | - | Lightweight API extraction logic runs locally to fetch JSON. |
| **Transformation** | **AWS Glue ETL (Spark/Python)** | 4.0 | **Offloads compute to AWS.** Handles massive datasets/backfills without crashing the local Docker container. |
| **Catalog** | **AWS Glue Crawler** | - | Automates schema discovery for the Parquet files in S3. |
| **Storage** | **AWS S3** | Standard | Cloud-native storage accessible by Glue, Athena, and OWOX. |
| **Warehouse** | **Amazon Athena** | V3 | Serverless SQL engine for querying S3 data. Pay-per-query. |
| **Data Layers** | **raw -> dev -> prod** | - | Strict lineage: Raw (JSON) -> Dev (Parquet) -> Prod (Views). |

## Data Architecture (S3 Zones)

| Zone | Path | Format | Engine | Description |
| --- | --- | --- | --- | --- |
| **Raw** | `s3://{bucket}/raw/` | JSON | Airflow | **Immutable Landing Zone.** Exact API response payload. Partitioned by date. |
| **Dev** | `s3://{bucket}/dev/` | Parquet | **AWS Glue** | **Processed Zone.** Cleaned, deduplicated, and typed data. Compressed (Snappy). |
| **Prod** | `s3://{bucket}/prod/` | SQL View | Athena | **Serving Zone.** Aggregated views (e.g., Daily Averages) for visualization. |

## Data Flow Pipeline

1. **Extract (Local):** Airflow triggers `extract_raw_data`. Fetches API data and uploads `.json` to `s3://.../raw/`.
2. **Transform (Cloud):** Airflow triggers **Glue Job** `openaq_raw_to_dev`.
* Reads JSON from `raw`.
* Flattens coordinates and converts types.
* Writes Parquet to `s3://.../dev/`.


3. **Catalog (Cloud):** Airflow triggers **Glue Crawler**. Updates Athena table definitions.
4. **Validate (Cloud):** Airflow triggers Athena query to verify data quality in `dev`.
5. **Serve (Cloud):** OWOX/Looker queries `openaq_prod` views which read from `dev` tables.

## Technology Stack Details

### Core Technologies

* **Orchestrator:** Apache Airflow (Docker Compose)
* **ETL Engine:** AWS Glue (Serverless Spark/Python Shell)
* **Query Engine:** Amazon Athena
* **Language:** Python 3.10+ (Local), PySpark (Glue)

### Integration Points

* **Airflow → AWS Glue:** Uses `boto3` to `start_job_run` and polls for completion.
* **Airflow → S3:** Uploads raw JSON files.
* **Glue → S3:** Reads JSON, writes Parquet.

## Implementation Patterns

### 1. Naming & Consistency

* **S3 Partitions:** Hive-style strictly enforced (`/year=YYYY/month=MM/day=DD/`).
* **Glue Jobs:** Naming convention `job_{source}_{target}` (e.g., `job_raw_to_dev`).
* **Regions:** Fixed to `ap-southeast-1`.

### 2. Security (Local Context)

* **Credentials:** Injected via `.env` file to Docker.
* **IAM User:** `openaq-local-dev` requires `glue:StartJobRun`, `glue:GetJobRun`, `s3:PutObject`, `s3:GetObject`.

## Architecture Decision Records (ADRs)

### ADR-001: Migration to Local Docker

**Context:** EC2 Free Tier has RAM limits.
**Decision:** Run Airflow locally.
**Consequences:** Zero compute cost for orchestration.

### ADR-003: Offloading Transformation to Glue

**Context:** Local Docker container may run out of memory when processing large historical backfills (e.g., 1 year of data).
**Decision:** Use AWS Glue ETL Jobs for the Transformation step.
**Consequences:**

* (+) Local machine stays responsive.
* (+) Can scale to process terabytes of data if needed.
* (-) Small cost associated with Glue DPU-hours (though within Free Tier limits for small runs).
* (-) Slightly more complex deployment (need to sync scripts to S3).

```

```