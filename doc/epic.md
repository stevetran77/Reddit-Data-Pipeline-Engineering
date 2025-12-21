# OpenAQ Local Lakehouse - Epic Breakdown

**Author:** John (Product Manager)
**Date:** 2025-12-21
**Project Level:** 2 (Brownfield/Restructuring)
**Target Scale:** AWS Free Tier / Personal Project (Local Execution)

---

## Overview
This document outlines the implementation plan for the **OpenAQ Local Lakehouse**. It is designed for a single developer running orchestration locally while leveraging AWS Serverless (Glue/Athena) for heavy lifting.

**Key Architecture Decisions Reflected:**
* **Orchestration:** Airflow LocalExecutor (Docker Desktop).
* **Compute Strategy:** Offload transformation to AWS Glue; run light tasks locally.
* **Data Flow:** `Raw` (JSON) → `Dev` (Parquet via Glue) → `Prod` (Athena Views).
* **Visualization:** OWOX Data Mart connecting to Athena Views.

---

## Functional Requirements Inventory
* **FR1:** Infrastructure capable of running Airflow locally (Docker).
* **FR2:** Logical separation of Dev and Prod environments.
* **FR3:** Extraction of OpenAQ API data to immutable Raw storage.
* **FR4:** Transformation of raw data to optimized Parquet format (via Glue).
* **FR5:** Automated cataloging of data for SQL access.
* **FR6:** Production data layer that filters invalid measurements.
* **FR7:** Visualization of current and historical air quality trends.
* **FR8:** Ability to switch dashboards between Dev and Prod data safely.

---

## Epic 1: Infrastructure Foundation

**Goal:** Establish the local runtime environment and cloud storage structure.
**Value:** A working local Airflow setup that can talk to AWS services.

### Story 1.1: Setup Local Docker Airflow
**As a** Data Engineer,
**I want** to deploy Airflow locally using Docker Compose,
**So that** I can orchestrate pipelines using my own machine's resources without cloud costs.

**Acceptance Criteria:**
* **Given** Docker Desktop is installed.
* **When** I run `docker-compose up -d`.
* **Then** Airflow Webserver and Scheduler are running.
* **And** `LocalExecutor` is configured.
* **And** the project directory is mounted as a volume for live code editing.

### Story 1.2: Configure AWS Authentication for Local Docker
**As a** Developer,
**I want** to securely inject my AWS credentials into the Docker containers,
**So that** Airflow tasks can access S3 and Glue without hardcoding keys in the code.

**Acceptance Criteria:**
* **Given** an IAM User `openaq-local-dev` with programmatic access.
* **When** I configure `.env` or mount `~/.aws/credentials`.
* **Then** I can run `aws s3 ls` successfully from *inside* the Airflow container.
* **And** credentials are NOT committed to git.

### Story 1.3: Configure S3 Data Lake Structure
**As a** System,
**I want** a structured S3 bucket with environment prefixes,
**So that** data is cleanly organized by layer.

**Acceptance Criteria:**
* **Given** the bucket `openaq-data-pipeline`.
* **When** I inspect the bucket.
* **Then** prefixes `/raw/`, `/dev/`, and `/prod/` exist.
* **And** public access is blocked.

---

## Epic 2: Data Ingestion & Processing

**Goal:** Build the hybrid Local/Cloud ELT pipeline.
**Value:** Data flowing from API to S3 and being transformed into queryable Parquet.

### Story 2.1: Implement API Extraction to Raw Layer (Local)
**As a** Data Engineer,
**I want** to extract data from OpenAQ API to JSON using local compute,
**So that** I have an immutable backup in the Raw layer without paying for cloud compute.

**Acceptance Criteria:**
* **Given** the OpenAQ API v3 client.
* **When** the `extract_task` runs locally.
* **Then** it fetches measurements and saves exact JSON responses.
* **And** uploads to `s3://.../raw/year=YYYY/month=MM/day=DD/`.

### Story 2.2: Develop Glue ETL Job for Transformation (Cloud)
**As a** Data Engineer,
**I want** a Glue Job to process JSON to Parquet,
**So that** heavy compute is offloaded to AWS (Serverless) and doesn't freeze my local machine.

**Acceptance Criteria:**
* **Given** JSON files in `raw`.
* **When** the Glue Job `openaq_raw_to_dev` runs.
* **Then** it reads JSON, flattens structures, and converts types.
* **And** writes Snappy-compressed Parquet to `s3://.../dev/`.
* **And** runs successfully on AWS Glue infrastructure.

### Story 2.3: Orchestrate Glue Trigger from Airflow
**As a** Data Engineer,
**I want** an Airflow task that starts the Glue Job and waits for completion,
**So that** my local orchestrator controls the cloud transformation.

**Acceptance Criteria:**
* **Given** the `GlueJobOperator` or custom Boto3 hook.
* **When** the task runs.
* **Then** it triggers the specific Glue Job.
* **And** it polls for "SUCCEEDED" status before proceeding.

### Story 2.4: Configure Glue Crawler
**As a** Data Engineer,
**I want** a crawler to catalog the `dev` layer,
**So that** Athena can query the transformed data.

**Acceptance Criteria:**
* **When** the crawler runs.
* **Then** the `openaq_dev` database contains tables with correct partitions.

---

## Epic 3: The Serving Layer (Athena & Prod)

**Goal:** Create the "Production" environment using Athena Views.
**Value:** Stable, clean data interface for visualization.

### Story 3.1: Configure Athena Views
**As a** Data Analyst,
**I want** SQL Views in the `openaq_prod` database,
**So that** I query clean, aggregated data (filtering out sensor errors).

**Technical Notes:**
* `view_latest_aqi`: Latest snapshot per location.
* `view_daily_average`: Daily aggregation for trends.

### Story 3.2: Implement Validation Task
**As a** Test Engineer,
**I want** to run a "Health Check" query on the views,
**So that** I know the data is valid before checking the dashboard.

**Acceptance Criteria:**
* **When** the validation task runs.
* **Then** it queries `view_latest_aqi` count.
* **And** fails the DAG if the count is 0.

---

## Epic 4: Visualization & Lifecycle

**Goal:** Connect the data to Looker via OWOX.
**Value:** Actionable insights.

### Story 4.1: Connect OWOX to Looker
**As a** User,
**I want** to see Dev/Prod dashboards in Looker,
**So that** I can monitor air quality in Vietnam.

**Acceptance Criteria:**
* **Given** the IAM User credentials.
* **When** I configure OWOX.
* **Then** I can create Data Sources for `OpenAQ Dev` and `OpenAQ Prod`.
* **And** charts load correctly in Looker Studio.