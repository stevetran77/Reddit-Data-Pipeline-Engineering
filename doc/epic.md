# OpenAQ Lean Lakehouse - Epic Breakdown

**Author:** John (Product Manager)
**Date:** 2025-12-21
**Project Level:** 2 (Brownfield/Restructuring)
**Target Scale:** AWS Free Tier / Personal Project

---

## Overview

This document provides the complete epic and story breakdown for the **OpenAQ Data Pipeline**. It decomposes the requirements from the Architecture Document into implementable stories suitable for a single developer or AI agent.

**Key Architecture Decisions Reflected:**
* **Orchestration:** Airflow LocalExecutor on EC2 (No Redis/Celery).
* **Data Flow:** `Raw` (JSON) → `Dev` (Parquet via Glue) → `Prod` (Athena Views).
* **Visualization:** OWOX Data Mart connecting to Athena Views.

---

## Functional Requirements Inventory

* **FR1:** Infrastructure capable of running Airflow on 1GB RAM (Free Tier).
* **FR2:** Logical separation of Dev and Prod environments without duplicating resources.
* **FR3:** Extraction of OpenAQ API data to immutable Raw storage.
* **FR4:** Transformation of raw data to optimized Parquet format.
* **FR5:** Automated cataloging of data for SQL access.
* **FR6:** Production data layer that filters invalid measurements.
* **FR7:** Visualization of current and historical air quality trends.
* **FR8:** Ability to switch dashboards between Dev and Prod data safely.

---

## Epic 1: Lean Infrastructure Foundation

**Goal:** Establish the cost-effective "Serverless Lakehouse" runtime environment on AWS Free Tier.
**Value:** Enables the team to deploy pipelines immediately without accruing fixed costs.

### Story 1.1: Provision Lean Airflow on EC2
**As a** Data Engineer,
**I want** to deploy a lightweight Airflow instance on EC2,
**So that** I can orchestrate pipelines without crashing the 1GB RAM limit.

**Acceptance Criteria:**
* **Given** an AWS Free Tier account and a `t3.small` (or `micro` with swap) EC2 instance.
* **When** I deploy the project `docker-compose.yml`.
* **Then** Airflow Webserver and Scheduler are running.
* **And** the `LocalExecutor` is configured (NO Redis, NO Celery containers).
* **And** the instance has IAM role access to S3 and Glue.

**Technical Notes:**
* Modify existing `docker-compose.yml` to remove Redis/Worker services.
* Setup swap space (2GB) on EC2.

### Story 1.2: Configure S3 Data Lake Structure
**As a** System,
**I want** a structured S3 bucket with logical environment separation,
**So that** raw and processed data are isolated for Dev and Prod.

**Acceptance Criteria:**
* **Given** the bucket `openaq-data-pipeline`.
* **When** I inspect the bucket structure.
* **Then** the following prefixes exist:
    * `/dev/raw/`, `/dev/processed/`
    * `/prod/raw/`, `/prod/processed/`
    * `/athena-results/`
* **And** the bucket blocks all public access.

### Story 1.3: Setup IAM User for OWOX Integration
**As a** Product Manager,
**I want** a dedicated IAM user for the visualization connector,
**So that** OWOX can query our data securely without root access.

**Acceptance Criteria:**
* **Given** the AWS IAM console.
* **When** I create the user `owox-integrator`.
* **Then** it has programmatic access only (Access Key/Secret).
* **And** it has the `AWSQuicksightAthenaAccess` managed policy.
* **And** it has an inline policy allowing `s3:GetObject` on the data bucket.

---

## Epic 2: Data Ingestion & Processing

**Goal:** Implement the ELT pipeline using Airflow for extraction and AWS Glue for transformation.
**Value:** Establishes the core data flow. By the end of this epic, data will flow into S3 and be cataloged.

### Story 2.1: Implement API Extraction to Raw Layer
**As a** Data Engineer,
**I want** to extract air quality data from the OpenAQ API and save it as JSON,
**So that** I have an immutable copy of the source data (Raw layer) in S3.

**Acceptance Criteria:**
* **Given** the OpenAQ API v3 client.
* **When** the extraction task runs (manually triggered).
* **Then** it fetches measurements for the target locations.
* **And** it saves the *exact* API response payload as `.json` files.
* **And** the upload path follows: `s3://.../dev/raw/year=YYYY/month=MM/day=DD/`.
* **And** the process is idempotent.

**Technical Notes:**
* Use `PythonOperator`. Do not transform data here.

### Story 2.2: Develop Glue ETL Job for Raw->Dev Transformation
**As a** Data Engineer,
**I want** a Glue ETL Job to process Raw JSON into Parquet,
**So that** transformation processing is offloaded to serverless infrastructure.

**Acceptance Criteria:**
* **Given** JSON files in the `dev/raw` S3 bucket.
* **When** the Glue Job `openaq_raw_to_dev` runs.
* **Then** it reads the JSON data and flattens nested structures.
* **And** it converts types (timestamps to UTC, values to float).
* **And** it writes Snappy-compressed Parquet files to `s3://.../dev/processed/`.
* **And** it maintains Hive partitions.

### Story 2.3: Configure Glue Crawler for Dev Layer
**As a** Data Engineer,
**I want** a Glue Crawler to scan the `dev` S3 bucket,
**So that** the transformed data is discoverable in the Athena `openaq_dev` database.

**Acceptance Criteria:**
* **Given** the `dev/processed/` S3 path.
* **When** the crawler runs.
* **Then** it updates the `openaq_dev` Data Catalog tables.
* **And** it adds new partitions automatically.

### Story 2.4: Orchestrate Manual Ingestion DAG
**As a** Data Engineer,
**I want** an Airflow DAG to coordinate the Extraction, Transformation, and Cataloging,
**So that** I can trigger the full pipeline on demand.

**Acceptance Criteria:**
* **Given** the Airflow environment.
* **When** I manually trigger the DAG `openaq_ingestion`.
* **Then** it executes: `Extract` -> `Glue Transform` -> `Glue Crawler`.

---

## Epic 3: The Serving Layer (Athena & Prod)

**Goal:** Create the "Production" environment using Athena Views.
**Value:** Provides a stable, clean data interface for Looker, shielding it from raw data issues.

### Story 3.1: Configure Prod Database Infrastructure
**As a** Data Engineer,
**I want** to establish the `openaq_prod` logical database in Athena,
**So that** I have a dedicated namespace for production views.

**Acceptance Criteria:**
* **When** the setup task runs.
* **Then** the database `openaq_prod` exists.
* **And** the `openaq_dev` database is accessible as the source.

### Story 3.2: Implement `view_latest_aqi` (Real-Time View)
**As a** Data Analyst,
**I want** a view that shows only the most recent valid measurement,
**So that** my dashboard doesn't show outdated or negative values.

**Acceptance Criteria:**
* **When** I query `openaq_prod.view_latest_aqi`.
* **Then** it returns only the latest timestamp per location.
* **And** it filters out values < 0.
* **And** it performs efficiently (< 5s query time).

### Story 3.3: Implement `view_daily_average` (Aggregated View)
**As a** Business User,
**I want** a pre-aggregated daily summary view,
**So that** looking at 30-day trends is fast and cheap.

**Acceptance Criteria:**
* **When** I query `openaq_prod.view_daily_average`.
* **Then** I see one row per location per day with `avg_pm25`.
* **And** incomplete days (<50% coverage) are flagged.

### Story 3.4: Implement View Validation Task
**As a** Test Engineer,
**I want** an automated check that runs after ETL,
**So that** I receive an alert if the Prod views are broken before OWOX reads them.

**Acceptance Criteria:**
* **When** the `validate_prod_views` task runs.
* **Then** it executes a count query on the Prod view.
* **And** fails the pipeline if the view is empty.

---

## Epic 4: Visualization & Lifecycle

**Goal:** Establish the Visualization Layer using OWOX and Looker Studio.
**Value:** Dashboards that drive decisions, with a safe Dev/Prod workflow.

### Story 4.1: Configure OWOX Data Marts
**As a** Data Analyst,
**I want** to connect OWOX to my Athena databases,
**So that** Looker Studio can access the curated views.

**Acceptance Criteria:**
* **When** I configure OWOX BI.
* **Then** I create two Data Marts: `OpenAQ Dev` and `OpenAQ Prod`.
* **And** both connect successfully using the IAM User from Story 1.3.

### Story 4.2: Create Looker Studio Data Sources
**As a** Data Analyst,
**I want** to create Looker Data Sources for both environments,
**So that** I can toggle reports between testing and production.

**Acceptance Criteria:**
* **Then** I have `DS_OpenAQ_Dev` and `DS_OpenAQ_Prod` data sources in Looker.
* **And** field types (Geo, Date) are correct.

### Story 4.3: Build 'Air Quality Overview' Dashboard
**As a** User,
**I want** a dashboard showing trends and current status,
**So that** I can monitor air quality in Vietnam.

**Acceptance Criteria:**
* **Then** the dashboard includes a Map (Latest AQI) and Trend Line (Daily Avg).
* **And** I can filter by City.

### Story 4.4: Validate Environment Switching
**As a** Release Manager,
**I want** to verify I can point the dashboard to Prod,
**So that** I can "promote" my changes after testing.

**Acceptance Criteria:**
* **When** I swap the dashboard Data Source from Dev to Prod.
* **Then** the charts load successfully without breaking.

---

**Status:** Ready for Implementation
**Prioritization:** Sequential (Epic 1 -> 2 -> 3 -> 4)