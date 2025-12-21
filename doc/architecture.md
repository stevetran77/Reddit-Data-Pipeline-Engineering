```markdown
# Architecture: OpenAQ Lean Lakehouse

## Executive Summary
The OpenAQ Data Pipeline is a **Serverless Lakehouse** architecture designed for the AWS Free Tier. It leverages **EC2 (t3.small)** for lightweight orchestration via **Airflow (LocalExecutor)**, while offloading storage and compute to **S3** and **Amazon Athena**. The system follows a strict **Raw -> Dev -> Prod** data layering strategy to ensure data quality and lineage, culminating in an **OWOX + Looker Studio** visualization layer with distinct environments.

## Project Initialization
First implementation story should execute:
```bash
# Initialize project structure with environment separation
mkdir -p dags/dev dags/prod etls pipelines utils config sql/athena_views
# Set up Python environment
python -m venv .venv
source .venv/bin/activate
pip install apache-airflow pandas boto3 awswrangler

```

## Decision Summary

| Category | Decision | Version | Rationale |
| --- | --- | --- | --- |
| **Orchestration** | **Airflow (LocalExecutor)** | 2.7+ | Fits within EC2 Free Tier (1GB RAM) by removing Redis/Celery overhead. |
| **Compute** | **EC2 (Docker)** | t3.small | Single node hosting Airflow and Metadata DB. Cost-effective for low volume. |
| **Storage** | **AWS S3** | Standard | Primary Data Lake storage. Partitioned by Hive style (`/year=YYYY/...`). |
| **Warehouse** | **Amazon Athena** | V3 | Serverless SQL engine. Zero fixed costs, pay-per-query. Replaces Redshift. |
| **Data Layers** | **Raw -> Dev -> Prod** | - | **Raw**: Ingested JSON. **Dev**: Processed Parquet. **Prod**: Aggregated Views. |
| **Visualization** | **OWOX + Looker Studio** | - | OWOX connects Athena results to Looker. Two data marts created: Dev and Prod. |
| **Environments** | **Logical Separation** | - | `dev` and `prod` prefixes within S3 and separate Athena Databases (`openaq_dev`, `openaq_prod`). |

## Data Architecture

### Layering Strategy (`raw`, `dev`, `prod`)

| Layer | S3 Path Pattern | Format | Purpose |
| --- | --- | --- | --- |
| **Raw** | `s3://{bucket}/{env}/raw/year={Y}/month={M}/day={D}/` | JSON | **Immutable Landing Zone.** Exact copy of API response. Allows replay if logic changes. |
| **Dev** | `s3://{bucket}/{env}/dev/year={Y}/month={M}/day={D}/` | Parquet | **Processed Zone.** Deduplicated, type-cast, compressed (Snappy). Optimized for Athena. |
| **Prod** | *(Virtual via Athena)* | SQL View | **Serving Zone.** Filtered, aggregated views exposed to OWOX/Looker. |

### Data Models

**1. Measurements (Dev Layer - Parquet)**

* `location_id` (int)
* `parameter` (string)
* `value` (float)
* `unit` (string)
* `datetime` (timestamp)
* `coordinates` (struct<lat, lon>)

**2. Prod Views (Serving Layer - Athena SQL)**

* `view_latest_aqi`: Filters out negative values, joins with location metadata.
* `view_daily_average`: Pre-aggregates data for faster OWOX querying.

## Technology Stack Details

### Core Technologies

* **Language:** Python 3.10+
* **Orchestrator:** Apache Airflow 2.7+ (LocalExecutor mode)
* **Infrastructure:** AWS (S3, Athena, Glue, EC2, IAM)
* **IaC:** None (Manual/Scripted setup for personal project)

### Integration Points

* **Airflow → S3:** `boto3` for file uploads (supports `env` parameter).
* **Airflow → Athena:** `awswrangler` or `boto3` to execute DDL for Views.
* **OWOX → Athena:** JDBC/ODBC connection via IAM User. Two connections configured: `OpenAQ Dev` and `OpenAQ Prod`.

## Implementation Patterns

### Category: Naming Patterns

* **S3 Paths:** Lowercase, Hive-partitioned. Example: `raw/year=2024/month=01/`
* **Athena Databases:** `openaq_{env}` (e.g., `openaq_dev`, `openaq_prod`).
* **Athena Views:** Prefix `view_`. Example: `view_prod_daily_summary`.

### Category: Structure Patterns

* **ETL Idempotency:** All tasks must be idempotent. Re-running a DAG for a past date should overwrite/replace data in S3 for that partition, not duplicate it.
* **Config Management:** `config.conf` accepts `ENV` variable to switch paths.

## Security Architecture

* **IAM User `owox-integrator`:**
* Policy: `AWSQuicksightAthenaAccess` (managed) + Inline Policy for `s3:GetObject`/`ListBucket` on specific bucket.


* **EC2 Instance Profile:**
* Role attached to EC2 allows `s3:PutObject`, `glue:StartCrawler`, `athena:StartQueryExecution`.
* **No AWS keys stored on EC2.**



## Development Environment

### Prerequisites

* Docker & Docker Compose
* Python 3.10+
* AWS CLI configured (for local testing)

### Setup Commands

```bash
# 1. Clone repo
git clone ...
# 2. Configure env
cp config/config.conf.example config/config.conf
# 3. Start Lean Airflow
docker-compose up -d

```

## Architecture Decision Records (ADRs)

### ADR-001: LocalExecutor for Orchestration

**Context:** AWS Free Tier limits RAM to 1GB. Standard Airflow Celery architecture requires Redis and multiple containers, exceeding this limit.
**Decision:** Use `LocalExecutor`.
**Consequences:** Loss of horizontal scaling (acceptable for personal project). Significant RAM savings.

### ADR-002: Athena over Redshift

**Context:** Redshift has high fixed costs ($0.25/hr+).
**Decision:** Use Amazon Athena.
**Consequences:** Pay-per-query model fits "low volume" requirement perfectly. Slightly higher latency for dashboards, but acceptable.

### ADR-003: Raw/Dev/Prod Layering

**Context:** Need clear separation between ingested data and reporting data, and ability to test changes safely.
**Decision:** Use `raw` (JSON), `dev` (Parquet), `prod` (Views) naming convention with logical separation (prefixes).
**Consequences:** Clear lineage. OWOX connects to specific environment views, protecting dashboards from breaking changes.

```

```