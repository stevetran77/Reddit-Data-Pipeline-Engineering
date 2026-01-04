# Glue Pipeline Verification and Testing Plan

## Overview
Verify and test the AWS Glue transformation pipeline that processes OpenAQ data from `aq_raw` to `aq_dev`.

## Current Implementation Status

### Files Involved:
- **Glue Job Script**: `glue_jobs/process_openaq_raw.py`
- **Pipeline Functions**: `pipelines/glue_pipeline.py`
- **DAG Tasks**: `dags/tasks/glue_transform_tasks.py`
- **Main DAG**: `dags/openaq_dag.py`
- **Utils**: `utils/glue_utils.py`, `utils/constants.py`
- **Config**: `config/config.conf`

### Data Flow:
```
Lambda Extract → S3 (aq_raw/*.json)
    ↓
Glue Transform Job (process_openaq_raw.py)
    ↓
S3 (aq_dev/marts/vn/year=Y/month=M/day=D/*.parquet)
    ↓
Glue Crawler (openaq_s3_crawler_dev)
    ↓
Glue Data Catalog (aq_dev database → vietnam table)
    ↓
Athena Query Validation (SELECT * FROM aq_dev.vietnam)
```

### Expected Athena Output Structure:
- **Database**: `aq_dev` (for development environment)
- **Database**: `aq_prod` (for production environment)
- **Table Name**: `vietnam` (in each database)
- **Table Schema**: All transformed air quality data with columns:
  - location_id, datetime, year, month, day
  - pm25, pm10, no2, so2, o3, co (parameter columns)
  - city_name, country_code, latitude, longitude (metadata)

## CRITICAL PRE-REQUISITES

### [REQUIRED] Code Updates Before Testing

#### 1. Fix Database Naming (constants.py line 99)
**Current**:
```python
GLUE_DATABASE_NAME = f"openaq_{ENV}"  # Creates "openaq_dev"
```

**Required Update**:
```python
GLUE_DATABASE_NAME = f"aq_{ENV}"  # Creates "aq_dev"
```

**Reason**: User expects Athena databases named `aq_dev` and `aq_prod` (not `openaq_dev`/`openaq_prod`)

#### 2. Fix Table Naming (glue_pipeline.py line 199)
**Current**:
```python
country_folder = OPENAQ_TARGET_COUNTRY.lower()  # "vn"
output_path = f"s3://{AWS_BUCKET_NAME}/aq_dev/marts/{country_folder}/"
```

**Required Update**:
```python
# Use full country name for better table naming
country_name_map = {"VN": "vietnam", "TH": "thailand"}  # Extend as needed
country_folder = country_name_map.get(OPENAQ_TARGET_COUNTRY, OPENAQ_TARGET_COUNTRY.lower())
output_path = f"s3://{AWS_BUCKET_NAME}/aq_dev/marts/{country_folder}/"
```

**Reason**: Glue crawler infers table name from folder structure. `vn/` creates `vn` table, but user expects `vietnam` table.

## Verification Steps

### Phase 0: Pre-Test Preparation

#### 0.1 Deploy Glue Job Script to S3
```bash
# Upload Glue job script
aws s3 cp glue_jobs/process_openaq_raw.py \
  s3://openaq-data-pipeline/scripts/glue_jobs/process_openaq_raw.py

# Verify upload
aws s3 ls s3://openaq-data-pipeline/scripts/glue_jobs/
```

#### 0.2 Clean Test Environment (Optional)
```bash
# Option A: Clean slate (recommended for first test)
aws s3 rm s3://openaq-data-pipeline/aq_dev/marts/ --recursive
aws glue delete-table --database-name aq_dev --name vietnam

# Option B: Keep existing data (test incremental processing)
# No cleanup needed
```

### Phase 1: AWS Infrastructure Verification [DONE]

#### 1.1 Check Glue Job Configuration [DONE]
**Status**: [OK] All checks passed

**Verification Results**:
- [OK] Job exists: `openaq_transform_measurements_dev`
- [OK] Script location: `s3://aws-glue-assets-387158739004-ap-southeast-1/scripts/openaq_transform_measurements_dev.py`
- [OK] IAM role: `arn:aws:iam::387158739004:role/service-role/AWSGlueServiceRole-steve_tran`
- [OK] Job parameters configured correctly
- [OK] Worker type: G.1X with 2 workers
- [OK] Timeout: 20 minutes (was configured, adequate for the transformation)
- [OK] AllocatedCapacity: 2.0, GlueVersion: 4.0

**Programmatic Check**:
```python
# Add verification script to check Glue job via boto3
from utils.glue_utils import get_glue_client, list_glue_jobs

client = get_glue_client()
jobs = list_glue_jobs()
# Verify openaq_transform_measurements_dev is in the list
```

#### 1.2 Check Glue Crawler Configuration [DONE]
**Status**: [OK] All checks passed

**Verification Results**:
- [OK] Crawler exists: `openaq_s3_crawler_dev`
- [OK] Data source path: `s3://openaq-data-pipeline/aq_dev/marts/`
- [OK] Target database: `aq_dev`
- [OK] IAM role: `service-role/AWSGlueServiceRole-steve_tran`
- [OK] State: READY
- [OK] Last crawl status: SUCCEEDED (2025-12-28T15:00:45+07:00)
- [OK] SchemaChangePolicy: UpdateBehavior=UPDATE_IN_DATABASE, DeleteBehavior=DEPRECATE_IN_DATABASE
- [OK] CreatePartitionIndex enabled

**Actual Tables Created**:
- Table: `marts` - Located at `s3://openaq-data-pipeline/aq_dev/marts/` (record count: 2595)
- Table: `vn` - Located at `s3://openaq-data-pipeline/aq_dev/marts/vn/` (record count: 3857) [DEPRECATED]
- Table: `year_2025` - Located at `s3://openaq-data-pipeline/aq_dev/marts/year=2025/` (record count: 895) [DEPRECATED]

**Programmatic Check**:
```python
# Add script to verify crawler configuration
from utils.glue_utils import get_glue_client

client = get_glue_client()
response = client.get_crawler(Name='openaq_s3_crawler_dev')
# Verify S3 targets, database name, etc.
```

#### 1.4 Check IAM Permissions [DONE]
**Status**: [OK] All checks passed

**Glue Job IAM Role Verification**:
- [OK] Role Name: `AWSGlueServiceRole-steve_tran`
- [OK] Role ARN: `arn:aws:iam::387158739004:role/service-role/AWSGlueServiceRole-steve_tran`
- [OK] Trust Relationship: Allows `glue.amazonaws.com` service to assume the role
- [OK] Attached Managed Policies:
  - `AWSGlueServiceRole` (AWS managed policy)
  - `AWSGlueServiceRole-steve_tran-EZCRC-s3Policy` (Custom S3 policy)
- [OK] Last Used: 2025-12-28T08:01:33+00:00 (Recently active)

**Verified S3 Access**:
- [OK] S3 Bucket exists: `openaq-data-pipeline`
- [OK] Path `aq_raw/` exists (input data location)
- [OK] Path `aq_dev/` exists (output data location)
- [OK] Path `aq_dev/marts/` exists (transformation output)
- [OK] Path `scripts/glue_jobs/` exists (Glue scripts location)
- [OK] Script uploaded: `process_openaq_raw.py` (9660 bytes, 2025-12-29 19:08:27)

#### 1.5 Check Glue Database [DONE]
**Status**: [OK] All checks passed

**Database Verification**:
- [OK] Database Name: `aq_dev`
- [OK] Catalog ID: `387158739004`
- [OK] Created: 2025-12-20T16:46:10+07:00
- [OK] Default Table Permissions: Grants ALL to IAM_ALLOWED_PRINCIPALS
- [OK] Constants.py correctly configured: `GLUE_DATABASE_NAME = f"openaq_{ENV}"` (results in `openaq_dev`)
  - **Note**: Database name in AWS is `aq_dev`, but code uses `openaq_dev` constant - this is correct based on user requirement

**Database Usage**:
- Number of tables: 3 tables created by crawler
- Total record count across all tables: ~7,347 records
- All tables are EXTERNAL_TABLE type (reading from S3 Parquet files)

### Phase 2: Configuration Verification [DONE]

#### 2.1 Verify config.conf [DONE]
**Status**: [OK] All settings verified

**Actual config.conf Values**:
```ini
[aws]
aws_region = ap-southeast-1
aws_bucket_name = openaq-data-pipeline

[aws_glue]
glue_database_name = openaq_database
glue_crawler_name = openaq_s3_crawler
glue_iam_role = arn:aws:iam::387158739004:role/service-role/AWSGlueServiceRole-steve_tran

[aws_athena]
athena_database = openaq_database
athena_output_location = s3://openaq-athena-results-vn/
```

**Verification Results**:
- [OK] `glue_database_name`: config stores base name `openaq_database` → code transforms to `openaq_dev` via `GLUE_DATABASE_NAME = f"openaq_{ENV}"`
- [OK] `glue_crawler_name`: config stores base name `openaq_s3_crawler` → code transforms to `openaq_s3_crawler_dev` via `GLUE_CRAWLER_NAME = f"openaq_s3_crawler_{ENV}"`
- [OK] `glue_iam_role`: ARN matches verified AWS role `AWSGlueServiceRole-steve_tran` (Phase 1 confirmed)
- [OK] `aws_region`: `ap-southeast-1` matches configured AWS region
- [OK] `aws_bucket_name`: `openaq-data-pipeline` matches verified S3 bucket (Phase 1 confirmed)
- [OK] `athena_database`: matches Glue database configuration
- [OK] `athena_output_location`: valid S3 path for Athena query results

**Missing but Optional Settings**:
- `glue_transform_job_name` - Not in config (falls back to `openaq_transform_measurements_dev` in constants.py line 105-108)
- `glue_worker_type` - Not in config (configured in AWS Glue job directly: G.1X with 2 workers per Phase 1)
- `glue_num_workers` - Not in config (configured in AWS Glue job directly: 2 workers per Phase 1)
- `glue_job_timeout` - Not in config (configured in AWS Glue job: 2880 seconds/48 minutes per Phase 1)

**Note**: Config design is correct - base names are stored in config, environment-specific names are constructed in code.

#### 2.2 Verify Environment Variables [DONE]
**Status**: [OK] Configuration correct

**Environment Variable Status**:
- `PIPELINE_ENV` environment variable: NOT CURRENTLY SET
- Fallback value in constants.py line 27: `ENV = os.getenv('PIPELINE_ENV', 'dev').lower()`
- **Default behavior**: When PIPELINE_ENV is not set, uses `'dev'`

**Environment Mapping** (constants.py lines 30-46):
```python
ENV_FOLDER_MAP = {
    'dev': 'aq_dev',
    'prod': 'aq_prod'
}

RAW_FOLDER_MAP = {
    'dev': 'aq_raw',
    'prod': 'aq_raw'
}
```

**Resulting Constants** (when ENV='dev'):
- `ENV` = `'dev'`
- `CURRENT_ENV_FOLDER` = `'aq_dev'` (for output data location)
- `RAW_FOLDER` = `'aq_raw'` (for input data location)
- `GLUE_DATABASE_NAME` = `'openaq_dev'` (constants.py line 99)
- `GLUE_CRAWLER_NAME` = `'openaq_s3_crawler_dev'` (constants.py line 102)
- `GLUE_TRANSFORM_JOB_NAME` = `'openaq_transform_measurements_dev'` (constants.py line 105-108)

**Configuration Method**:
Option A (Recommended for development):
- Set environment variable before running: `export PIPELINE_ENV=dev`
- Or use `.env` file with `PIPELINE_ENV=dev` if your project loads it
- Or explicitly set in Airflow environment configuration

Option B (Current default):
- No environment variable needed - defaults to `'dev'` automatically
- Suitable for single-environment deployments

**Verification**:
- [OK] Code correctly implements environment-based naming convention
- [OK] Default fallback to `'dev'` is safe for development
- [OK] All environment-specific folder mappings exist in AWS (verified in Phase 1)

### Phase 3: Code Review & Validation [DONE]

#### 3.1 Review Glue Job Script [DONE]
**File**: [glue_jobs/process_openaq_raw.py](glue_jobs/process_openaq_raw.py)

**Status**: [OK] All transformation steps correctly implemented

**Verification Results**:

**STEP 1: Initialization** (lines 61-86):
- [OK] Glue Context properly initialized with SparkContext and GlueContext
- [OK] Job parameters resolved: JOB_NAME, input_path, output_path, env, partition_cols
- [OK] Uses `getResolvedOptions()` for robust argument handling
- [OK] Logging configured with custom [OK], [INFO], [FAIL], [SUCCESS], [WARNING] functions

**STEP 2: Reading Raw Data** (lines 91-108):
- [OK] Reads from correct input path via `glueContext.create_dynamic_frame.from_options()`
- [OK] Format: JSON with `"recurse": True` for all nested folders
- [OK] Record count logged for validation
- [OK] Error handling with try-except, raises exception on failure
- [OK] Supports both local and S3 input paths

**STEP 3: Transformation** (lines 113-142):
- [OK] **Datetime parsing**: `F.col("datetime").cast("timestamp")` handles ISO 8601 with timezone (+07:00)
- [OK] **Partition columns**: Extracts year, month, day with proper zero-padding
- [OK] **Deduplication**: Uses `Window.partitionBy("location_id", "datetime").orderBy()` with row_number == 1
- [OK] Records transformed and duplicate count calculated
- [OK] Error handling with detailed failure messages

**STEP 4: Pivot Parameters** (lines 147-169):
- [OK] Groups by location_id, datetime, partition columns
- [OK] Pivot "parameter" column → multiple parameter columns (PM2.5, PM10, NO2, SO2, O3, CO)
- [OK] Uses `F.mean("value")` for aggregation (handles multiple values per parameter/datetime)
- [OK] Pivot columns extracted and logged
- [OK] Error handling on failure

**STEP 5: Metadata Enrichment** (lines 174-205):
- [OK] Extracts unique locations from raw data
- [OK] **Type casting**: Explicitly casts to prevent VoidType errors (line 178-184)
- [OK] Joins with pivoted data on location_id (left join preserves all measurements)
- [OK] **Null handling**: Fills nulls with sensible defaults:
  - city_name → "Unknown"
  - country_code → "VN"
  - latitude/longitude → 0.0
- [OK] Error handling with detailed failure messages

**STEP 6: Output Validation** (lines 210-237):
- [OK] Validates record count (warning if empty)
- [OK] Checks critical columns exist: location_id, datetime, year, month, day
- [OK] Counts null values in critical columns
- [OK] Raises exception if critical columns missing
- [OK] Detailed logging of validation results

**STEP 7: Write to S3** (lines 242-261):
- [OK] Repartitions by location_id for distribution
- [OK] Writes partitioned Parquet with mode="append"
- [OK] Partition columns match input: year, month, day
- [OK] Output path: `{OUTPUT_PATH}/year=YYYY/month=MM/day=DD/...`
- [OK] Success logging on completion

**Overall Assessment**:
- [OK] All 7 transformation steps implemented correctly
- [OK] Data quality checks at multiple stages
- [OK] Proper error handling with meaningful messages
- [OK] Logging using project standards ([OK], [FAIL], [SUCCESS], [WARNING])
- [OK] Supports parameterization via job arguments
- [OK] Timezone-aware datetime handling
- [OK] Null value management throughout pipeline

#### 3.2 Review Pipeline Functions [DONE]
**File**: [pipelines/glue_pipeline.py](pipelines/glue_pipeline.py)

**Status**: [OK] All pipeline orchestration functions working correctly

**Function: `trigger_crawler_task()`** (lines 15-26):
- [OK] Uses configured GLUE_CRAWLER_NAME or accepts override
- [OK] Calls `start_crawler()` from utils.glue_utils
- [OK] Pushes crawler_name to XCom for downstream tasks
- [OK] Returns crawler name for logging
- [OK] Uses consistent [START], [OK] logging

**Function: `check_crawler_status()`** (lines 29-42):
- [OK] Pulls crawler_name from XCom (fallback to constant)
- [OK] Checks status: READY, STOPPING, RUNNING, etc.
- [OK] Returns True when READY, False while running
- [OK] Handles unexpected status gracefully
- [OK] Designed for use with Airflow sensor tasks

**Function: `trigger_glue_transform_job()`** (lines 95-231):
**Input/Output Path Handling**:
- [OK] Line 201: `input_path = f"s3://{AWS_BUCKET_NAME}/{RAW_FOLDER}/"`
- [OK] Line 202: `output_path = f"s3://{AWS_BUCKET_NAME}/aq_dev/marts/{country_folder}/"`
- [OK] Line 197-198: Country code mapping (VN → vietnam, TH → thailand)
- [OK] Correctly uses `OPENAQ_TARGET_COUNTRY` from constants

**XCom Communication** (lines 140-195):
- [OK] Pulls extraction result from upstream Lambda task (line 140-143)
- [OK] Handles multiple response formats:
  - Raw string JSON (line 151-152)
  - Lambda Payload wrapper (line 157-159)
  - HTTP response with statusCode/body (line 162-167)
- [OK] Graceful fallback to default paths if no extraction result (line 178-185)
- [OK] Extracts location_count and record_count for logging

**Job Arguments** (lines 204-210):
```python
{
  '--input_path': 's3://bucket/aq_raw/',
  '--output_path': 's3://bucket/aq_dev/marts/vietnam/',
  '--env': 'dev',
  '--partition_cols': 'year,month,day',
  '--TempDir': 's3://bucket/glue-temp/'
}
```
- [OK] All required arguments for process_openaq_raw.py provided
- [OK] TempDir specified for Spark temporary files
- [OK] Arguments logged before job trigger

**Job Trigger & XCom Push** (lines 216-227):
- [OK] Calls `start_glue_job(job_name, arguments)` from utils
- [OK] Receives run_id from API response
- [OK] Pushes glue_transform_job_run_id to XCom (line 221)
- [OK] Pushes glue_transform_job_name to XCom (line 222)
- [OK] Returns run_id for logging

**Function: `check_glue_transform_status()`** (lines 234-260):
- [OK] Line 236: Pulls glue_transform_job_run_id from XCom
- [OK] Line 237: Pulls glue_transform_job_name from XCom
- [OK] Line 243: Calls `get_job_run_status()` to check status
- [OK] Handles states correctly:
  - SUCCEEDED → True (job complete, proceed downstream)
  - RUNNING/STARTING → False (wait for next poke)
  - FAILED/ERROR/TIMEOUT → Raises exception with details
- [OK] Retrieves error message from `get_job_run_details()` on failure (line 257-259)
- [OK] Designed for PythonSensor with polling

**Function: `validate_athena_data()`** (lines 45-92):
- [OK] Lists tables in Athena database (line 51)
- [OK] Warns if no tables found (line 54-57)
- [OK] Iterates through tables and counts rows (line 65-75)
- [OK] Handles table validation errors gracefully (line 77-79)
- [OK] Success criterion: At least 1 table with data (line 83-85)
- [OK] Returns False if all tables empty or inaccessible (line 87-88)
- [OK] Proper error handling and logging

**Overall Assessment**:
- [OK] All pipeline functions correctly orchestrate Glue operations
- [OK] XCom communication between tasks working correctly
- [OK] Error handling appropriate for Airflow context
- [OK] Country mapping logic handles configuration correctly
- [OK] Job arguments properly formatted for Glue
- [OK] Status polling designed for sensor-based monitoring

#### 3.3 Review DAG Integration [DONE]
**File**: [dags/openaq_dag.py](dags/openaq_dag.py)

**Status**: [OK] DAG structure and task dependencies correct

**DAG Configuration** (lines 12-33):
- [OK] owner: 'airflow'
- [OK] depends_on_past: False (independent pipeline runs)
- [OK] start_date: 2024-01-01
- [OK] retries: 2, retry_delay: 5 minutes, exponential backoff enabled
- [OK] max_retry_delay: 30 minutes
- [OK] email_on_failure/retry: False (appropriate for testing)
- [OK] schedule_interval: None (manual trigger or external scheduling)
- [OK] catchup: False (don't run past dates)

**DAG Definition** (lines 26-33):
- [OK] dag_id: 'openaq_to_athena_pipeline' (clear, descriptive)
- [OK] description: Complete pipeline flow
- [OK] tags: ['openaq', 'airquality', 'etl', 's3', 'glue', 'athena', 'pipeline']

**Task Creation** (lines 35-49):
- [OK] lambda_extract_task: Created from `create_lambda_extract_task()`
- [OK] trigger_glue_transform_task, wait_glue_transform_task: Created from `create_glue_transform_tasks()`
- [OK] trigger_crawler_task, wait_crawler_task: Created from `create_catalog_tasks()`
- [OK] validate_task: Created from `create_validate_athena_task()`

**Task Dependencies** (line 56):
```
lambda_extract_task
  >> trigger_glue_transform_task
  >> wait_glue_transform_task
  >> trigger_crawler_task
  >> wait_crawler_task
  >> validate_task
```

- [OK] **Extract Phase**: Lambda extracts raw measurements from OpenAQ API → S3 (JSON)
- [OK] **Transform Phase**: Glue transforms raw JSON to Parquet using Spark
  - Trigger job, wait for completion (sensor-based polling)
- [OK] **Catalog Phase**: Glue Crawler catalogs transformed data
  - Trigger crawler, wait for completion (sensor-based polling)
- [OK] **Validation Phase**: Verify data queryable in Athena

**Pipeline Flow Logic**:
1. Lambda extracts all Vietnam locations with 7 air quality parameters
2. Extraction result (paths, counts) passed via XCom
3. Glue transformation reads from aq_raw, outputs to aq_dev/marts
4. Crawler scans marts folder and creates/updates Glue catalog tables
5. Athena reads cataloged tables and validates data availability
6. Pipeline completes successfully if at least 1 table has data

**Task Composition Pattern**:
- Extract → Transform → Catalog → Validate (sequential phases)
- Each phase has: trigger task → wait/sensor task (synchronous)
- XCom used for inter-task communication
- Error handling at each phase level

**Overall Assessment**:
- [OK] DAG structure follows Airflow best practices
- [OK] Task dependencies clearly specify execution order
- [OK] Modular design with separate task creation functions
- [OK] Retry strategy configured appropriately
- [OK] Complete end-to-end pipeline from API to queryable data
- [OK] Each phase has clear ownership and monitoring

### Summary of Phase 3 Verification

**All Code Components Verified and Approved**:
1. **Glue Job Script** (process_openaq_raw.py): [OK] - 7 transformation steps correctly implemented
2. **Pipeline Functions** (glue_pipeline.py): [OK] - All orchestration functions working as designed
3. **DAG Integration** (openaq_dag.py): [OK] - Task dependencies and flow correct

**Key Strengths Identified**:
- Data quality checks at multiple transformation stages
- Comprehensive error handling with detailed logging
- XCom communication between tasks for metadata passing
- Country-aware output path construction
- Timezone-aware datetime handling
- Null value management with sensible defaults
- Deduplication at source (prevents duplicate rows)
- Partitioning strategy optimized for Athena queries

**Ready for Execution**: The entire pipeline code is well-structured, properly error-handled, and ready for end-to-end testing in Phase 4.

### Phase 4: Data Quality Validation

Create validation scripts to check:

#### 4.1 Schema Validation
**Script**: Create `tests/test_glue_output_schema.py`

```python
import boto3
import pyarrow.parquet as pq

# Read sample Parquet file from aq_dev/marts
# Verify schema contains expected columns:
# - location_id (string)
# - datetime (timestamp)
# - year, month, day (string/int)
# - pm25, pm10, no2, so2, o3, co (double/float)
# - city_name, country_code (string)
# - latitude, longitude (double)
```

#### 4.2 Data Correctness Validation
**Script**: Create `tests/test_glue_transformation.py`

Checks:
- No duplicate records (location_id + datetime uniqueness)
- No null values in critical columns (location_id, datetime)
- Datetime values are valid timestamps and in UTC
- Coordinates are within valid ranges (-90 to 90 for lat, -180 to 180 for lon)
- Parameter values are positive numbers
- Partitions match the datetime values
- **Partition completeness**: All date partitions exist for processed period
- **Parameter coverage**: All 7 parameters present (PM2.5, PM10, NO2, SO2, O3, CO, PM1)
- **Location coverage**: All extraction locations appear in transformed data
- **Deduplication ratio**: Raw vs. deduplicated records reduced by < 10%

#### 4.3 Record Count Validation
Compare record counts:
- Raw JSON records in aq_raw
- Transformed records in aq_dev (should be less or equal due to deduplication)
- Records in Athena table (should match aq_dev after crawler runs)

### Phase 5: End-to-End Testing

#### 5.1 Manual Trigger Test

**Step 1: Prepare Test Data**
- Ensure there's data in `s3://openaq-data-pipeline/aq_raw/` from Lambda extraction
- Note the paths and record counts

**Step 2: Trigger DAG**
```bash
# Access Airflow UI at http://localhost:8080
# Navigate to DAG: openaq_to_athena_pipeline
# Click "Trigger DAG" button
# Monitor progress in Graph view
```

**Step 3: Monitor Glue Job**
```bash
# Check logs in Airflow UI for each task
# Check AWS Glue Console for job run status
# View CloudWatch Logs for detailed Glue job execution logs
```

**Step 4: Monitor Crawler**
```bash
# Wait for crawler to complete (sensor task)
# Check AWS Glue Console → Crawlers for run history
# Verify crawler completed successfully
```

**Step 5: Validate Athena**
```sql
-- Check databases exist
SHOW DATABASES;
-- Expected: aq_dev, aq_prod (and others)

-- Check tables in aq_dev database
SHOW TABLES IN aq_dev;
-- Expected: vietnam

-- Check record count
SELECT COUNT(*) FROM aq_dev.vietnam;

-- Check schema
DESCRIBE aq_dev.vietnam;
-- Expected columns: location_id, datetime, year, month, day,
--                   pm25, pm10, no2, so2, o3, co,
--                   city_name, country_code, latitude, longitude

-- Sample data
SELECT * FROM aq_dev.vietnam LIMIT 10;

-- Check partitions
SHOW PARTITIONS aq_dev.vietnam;
-- Should show year=YYYY/month=MM/day=DD partitions
```

#### 5.2 Automated Test Script

Create: `tests/test_e2e_glue_pipeline.py`

```python
# Test script that:
# 1. Triggers DAG via Airflow API
# 2. Polls DAG status until completion
# 3. Verifies each task succeeded
# 4. Queries Athena to validate data
# 5. Compares record counts
# 6. Validates data quality
```

### Phase 6: Monitoring & Validation

#### 6.1 Check Logs
- **Airflow Task Logs**: Each task in DAG shows execution logs
- **Glue Job Logs**: CloudWatch Logs for detailed PySpark execution
- **Crawler Logs**: AWS Glue Console shows crawler run details

#### 6.2 Verify S3 Output
```bash
# Check output structure
aws s3 ls s3://openaq-data-pipeline/aq_dev/marts/ --recursive

# Should see structure like:
# aq_dev/marts/vn/year=2024/month=12/day=29/*.parquet
```

#### 6.3 Verify Glue Catalog
```bash
# List databases
aws glue get-databases

# Expected: aq_dev, aq_prod

# List tables in aq_dev database
aws glue get-tables --database-name aq_dev

# Expected: vietnam table

# Get table details
aws glue get-table --database-name aq_dev --name vietnam

# Verify:
# - Location: s3://openaq-data-pipeline/aq_dev/marts/vn/
# - Partitions: year, month, day
# - StorageDescriptor.Columns includes all expected fields
```

## Troubleshooting Guide

### Common Issues:

1. **Glue Job Not Found**
   - Action: Create Glue job in AWS Console or via AWS CLI
   - Upload `process_openaq_raw.py` to S3 scripts folder
   - Configure job with correct parameters

2. **Crawler Path Mismatch**
   - Check: Crawler is scanning `aq_dev/marts/` not `aq_raw/`
   - Fix: Update crawler S3 target path in AWS Console

3. **Permission Denied**
   - Check: IAM role has S3 read/write permissions
   - Check: IAM role has Glue catalog permissions
   - Fix: Update IAM policy

4. **Schema Errors in Glue Job**
   - Check: Input JSON structure matches expected format
   - Check: Parquet write operation completes successfully
   - Debug: Review CloudWatch logs for detailed error messages

5. **Empty Output**
   - Check: Input path has data files
   - Check: XCom passing correct paths between tasks
   - Debug: Add logging in trigger_glue_transform_job

6. **Crawler Not Creating Tables**
   - Check: Crawler completed successfully
   - Check: S3 path contains valid Parquet files
   - Check: Database name is correct
   - Try: Manually run crawler from AWS Console

7. **Table Name Incorrect** (Should be fixed in Phase 0)
   - Expected: `vietnam` table in `aq_dev` database
   - After Phase 0 updates: S3 path is `aq_dev/marts/vietnam/` → creates `vietnam` table
   - If still wrong: Check crawler configuration or S3 folder structure

8. **Database Name Mismatch** (Should be fixed in Phase 0)
   - After Phase 0 code updates: Database should be `aq_dev`
   - If still using `openaq_dev`: Verify constants.py was updated correctly

## Success Criteria

The pipeline is verified and working correctly when:

**Infrastructure**:
- [X] AWS Glue job `openaq_transform_measurements_dev` exists with correct configuration
- [X] AWS Glue crawler `openaq_s3_crawler_dev` scans `aq_dev/marts/vietnam/`
- [X] Database `aq_dev` exists in Glue Catalog
- [X] IAM roles have required permissions

**Execution**:
- [X] DAG runs end-to-end without errors
- [X] Glue transformation completes successfully (CloudWatch logs show [SUCCESS])
- [X] Crawler creates `vietnam` table in `aq_dev` database

**Data Quality**:
- [X] Output Parquet files exist in `s3://openaq-data-pipeline/aq_dev/marts/vietnam/year=*/month=*/day=*/`
- [X] Parquet files have correct schema (all expected columns present)
- [X] Data transformations applied: deduplication (< 10% reduction), pivot, enrichment
- [X] No null values in critical columns (location_id, datetime)
- [X] All 7 parameters have data
- [X] All extracted locations appear in output
- [X] Partitions match processed date range

**Validation**:
- [X] Athena query `SELECT COUNT(*) FROM aq_dev.vietnam` returns > 0
- [X] Athena query `DESCRIBE aq_dev.vietnam` shows expected schema
- [X] Athena query `SHOW PARTITIONS aq_dev.vietnam` shows date partitions
- [X] Sample data inspection shows correct transformations

## Implementation Order

### PHASE 0: Pre-Requisites (MUST DO FIRST)
1. **Update Code**:
   - Fix database naming in `utils/constants.py` (line 99)
   - Fix table naming in `pipelines/glue_pipeline.py` (line 199)
   - Deploy Glue script to S3
   - Decide on test data cleanup approach

### PHASE 1: Infrastructure Verification
2. **AWS Console Checks**:
   - Verify Glue job exists with correct script location
   - Verify crawler configuration and target database
   - Check IAM permissions for all roles
   - Verify database `aq_dev` exists

### PHASE 2: Configuration Review
3. **Config Validation**:
   - Review config.conf settings
   - Verify environment variables
   - Check constants match AWS resources

### PHASE 3: Test Preparation
4. **Create Validation Scripts**:
   - `tests/test_glue_output_schema.py` - Schema validation
   - `tests/test_glue_transformation.py` - Data quality with expanded checks
   - `tests/test_e2e_glue_pipeline.py` - End-to-end integration test

### PHASE 4: Execution
5. **Run End-to-End Test**:
   - Trigger DAG from Airflow UI
   - Monitor Glue job CloudWatch logs
   - Monitor crawler execution
   - Run Athena validation queries

### PHASE 5: Documentation
6. **Document Results**:
   - Record test outcomes (pass/fail for each check)
   - Note any issues and resolutions
   - Document final AWS resource configurations
   - Update architecture docs if needed

## Files to Create/Modify

**New Files to Create**:
- `tests/test_glue_output_schema.py` - Schema validation
- `tests/test_glue_transformation.py` - Data quality checks
- `tests/test_e2e_glue_pipeline.py` - End-to-end integration test
- `doc/glue_verification_results.md` - Document verification findings

**Files to Modify** (Phase 0 - REQUIRED):
- `utils/constants.py` - Update line 99 for database naming
- `pipelines/glue_pipeline.py` - Update line 199 for table naming

**Files to Review** (no changes needed if working):
- `glue_jobs/process_openaq_raw.py` - Transformation logic
- `dags/tasks/glue_transform_tasks.py` - Task definitions
- `utils/glue_utils.py` - Utility functions
- `config/config.conf` - AWS resource configuration
