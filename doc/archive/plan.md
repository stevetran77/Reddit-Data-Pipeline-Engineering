# OpenAQ Data Pipeline Optimization & Production Deployment Plan

## Executive Summary

**Objective:** Ensure the OpenAQ air quality data pipeline is fully functional, tested, and ready for production deployment. The pipeline uses Python-based extraction (via Airflow PythonOperator) with AWS Glue for transformation and Athena for querying.

**Architecture Flow:**
```
OpenAQ API → Airflow (Python extraction) → S3 (Parquet) → Glue Crawler → Athena Queries
```

**Current Implementation Status:**
- ✅ Airflow DAG defined (`dags/openaq_dag.py`)
- ✅ Python extraction pipeline (`pipelines/openaq_pipeline.py`)
- ✅ Glue transformation (`glue_jobs/`, Glue tasks)
- ✅ Glue Crawler integration
- ✅ Athena validation

**Total Estimated Time:** 8 hours (5 phases)

**Key Focus:** Validate end-to-end pipeline functionality, optimize performance, and prepare for production with monitoring and alerting.

---

## Architecture Diagram

```
┌─────────────────┐
│    Airflow      │  Orchestrates entire pipeline
│  PythonOperator │  Schedules daily extractions
└────────┬────────┘
         │ Call openaq_pipeline()
         ▼
┌─────────────────┐
│  Python Code    │  Connects to OpenAQ API
│ openaq_etl.py   │  Extracts location data
│ openaq_pipeline │  Transforms to DataFrame
└────────┬────────┘
         │ Write Parquet
         ▼
┌─────────────────┐
│   S3 Data Lake  │  airquality/{city}/
│     Parquet     │  year={Y}/month={M}/day={D}/
└────────┬────────┘
         │ Trigger Glue Job
         ▼
┌─────────────────┐
│   Glue Job      │  PySpark transformation
│   (transform)   │  Dedupe, pivot, partition
└────────┬────────┘
         │ Write Parquet
         ▼
┌─────────────────┐
│  S3 Marts       │  aq_dev/marts/vietnam/
│    Parquet      │  Ready for analysis
└────────┬────────┘
         │ Trigger Crawler
         ▼
┌─────────────────┐
│ Glue Crawler    │  Updates Data Catalog
│   & Catalog     │  Makes data discoverable
└────────┬────────┘
         │ Query
         ▼
┌─────────────────┐
│   Athena        │  SQL queries on cataloged data
│   Database      │  Analytics and validation
└─────────────────┘
```

---

## Phase 1: Prerequisites & Environment Verification

**Duration:** 1.5 hours
**Objective:** Verify all dependencies are installed and AWS services are accessible

### Sub-task 1.1: Verify Python Dependencies (15 min) (Done)

**File:** `requirements.txt`

**Actions:**
1. Check current dependencies:
   ```bash
   cat requirements.txt
   ```

2. Install all dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Verify critical imports:
   ```bash
   python -c "from pipelines.openaq_pipeline import openaq_pipeline; print('[OK] openaq_pipeline imported')"
   python -c "from utils.aws_utils import upload_to_s3_partitioned; print('[OK] aws_utils imported')"
   python -c "import boto3; print('[OK] boto3 imported')"
   ```

**Success Criteria:**
- ✅ All dependencies installed without errors
- ✅ Core modules import successfully
- ✅ No missing packages

---

### Sub-task 1.2: Configure Airflow (30 min) (Done)

**Objective:** Set up Airflow configuration and database

**Actions:**

1. Create config file from template:
   ```bash
   cp config/config.conf.example config/config.conf
   # Edit with your AWS credentials and database settings
   ```

2. Initialize Airflow database:
   ```bash
   airflow db init
   airflow db upgrade
   ```

3. Create Airflow admin user:
   ```bash
   airflow users create \
     --username admin \
     --firstname admin \
     --lastname admin \
     --role Admin \
     --email admin@airflow.com \
     --password admin
   ```

4. Configure AWS Connection in Airflow:
   ```bash
   airflow connections add aws_default \
     --conn-type aws \
     --conn-login YOUR_AWS_ACCESS_KEY \
     --conn-password YOUR_AWS_SECRET_KEY \
     --conn-extra '{"region_name": "ap-southeast-1"}'
   ```

5. Test connection:
   ```bash
   airflow connections test aws_default
   ```

**Success Criteria:**
- ✅ Airflow database initialized
- ✅ Admin user created
- ✅ AWS connection configured
- ✅ Connection test passes

---

### Sub-task 1.3: Verify AWS Infrastructure (30 min) (Done)

**Objective:** Confirm all required AWS resources exist and are accessible

**Verify S3 Bucket:**
```bash
aws s3 ls s3://openaq-data-pipeline/ --region ap-southeast-1

# Expected folders: aq_prod/, aq_dev/, aq_raw/, aq_raw_test, athena-results/
```

**Verify Glue Databases (Dev & Prod):**
```bash
aws glue get-database --name openaq_dev --region ap-southeast-1
aws glue get-database --name openaq_prod --region ap-southeast-1
```

**Verify Glue Jobs (Dev & Prod):**
```bash
aws glue get-job --job-name openaq_transform_measurements_dev --region ap-southeast-1
aws glue get-job --job-name openaq_transform_measurements_prod --region ap-southeast-1
```

**Verify Glue Crawlers (Dev & Prod):**
```bash
aws glue get-crawler --name openaq_s3_crawler_dev --region ap-southeast-1
aws glue get-crawler --name openaq_s3_crawler_prod --region ap-southeast-1
```

**Verify Athena Databases (Dev & Prod):**
```bash
aws athena start-query-execution --query-string "SELECT 1;" --query-execution-context Database=openaq_dev --result-configuration OutputLocation=s3://openaq-data-pipeline/athena-results/ --region ap-southeast-1
aws athena start-query-execution --query-string "SELECT 1;" --query-execution-context Database=openaq_prod --result-configuration OutputLocation=s3://openaq-data-pipeline/athena-results/ --region ap-southeast-1
```

**Success Criteria:**
- ✅ S3 bucket accessible with folders: aq_raw_test/, aq_raw_prod/, aq_dev/, aq_prod/
- ✅ Glue databases exist: openaq_dev, openaq_prod
- ✅ Glue jobs deployed: openaq_transform_measurements_dev, openaq_transform_measurements_prod
- ✅ Glue crawlers configured: openaq_s3_crawler_dev, openaq_s3_crawler_prod
- ✅ Athena databases exist: openaq_dev, openaq_prod

---

## Phase 2: Test Data Extraction Pipeline

**Duration:** 2 hours
**Objective:** Verify Python extraction works correctly

### Sub-task 2.1: Test Extract Task in Isolation (30 min)

**Objective:** Run extraction task without full DAG

**Test extraction task:**
```bash
docker-compose exec airflow-webserver airflow tasks test openaq_to_athena_pipeline lambda_extract_vietnam 2024-12-28

```

**Expected output:**
```
[START] Extracting air quality data for Vietnam
[INFO] Connecting to OpenAQ API...
[OK] Connected to OpenAQ API
[INFO] Extracting locations...
[INFO] Found 53 locations in Vietnam
[INFO] Extracting measurements...
[OK] Extracted 312 records
[INFO] Uploading to S3...
[OK] Uploaded to s3://openaq-data-pipeline/aq_raw_test/2025/12/28/...
[SUCCESS] Extraction completed successfully
```

**Check S3 for files:**
```bash
aws s3 ls s3://openaq-data-pipeline/aq_raw_test/ --recursive | tail -20
```

**Expected structure (Dev Environment):**
```
aq_raw_test/2025/12/25/13/raw_vietnam_national_test.json
aq_raw_test/2025/12/27/12/raw_vietnam_national_test.json
aq_raw_test/2025/12/28/06/raw_vietnam_national_20251228T061747.json
```

**Note:** Data is stored in `aq_raw_test/` folder (dev environment) with structure: `year/month/day/hour/raw_vietnam_national_{timestamp}.json`

**Success Criteria:**
- ✅ Task completes without errors
- ✅ Output shows extraction metrics (locations + records count)
- ✅ JSON files created in S3 under aq_raw_test/
- ✅ Files are in correct partition structure (year/month/day/hour/)
- ✅ File format is JSON (not Parquet) with meta + results wrapper

---

### Sub-task 2.2: Validate Extracted Data (30 min)

**Objective:** Verify raw JSON data quality and format

**Download sample file:**
```bash
aws s3 cp s3://openaq-data-pipeline/aq_raw_test/2025/12/28/06/raw_vietnam_national_20251228T061747.json ./sample_raw.json
```

**Inspect JSON structure:**
```bash
python3 << 'EOF'
import json

with open('sample_raw.json', 'r') as f:
    data = json.load(f)

print("[INFO] JSON Structure:")
print(f"  - Root keys: {list(data.keys())}")

if 'meta' in data:
    meta = data['meta']
    print(f"\n[INFO] Metadata:")
    print(f"  - Name: {meta.get('name')}")
    print(f"  - Website: {meta.get('website')}")
    print(f"  - Records found: {meta.get('found')}")
    print(f"  - Extracted at: {meta.get('extracted_at')}")

if 'results' in data:
    results = data['results']
    print(f"\n[INFO] Results:")
    print(f"  - Total records: {len(results)}")
    if len(results) > 0:
        print(f"  - Sample record (first):")
        sample = results[0]
        for key, value in list(sample.items())[:5]:
            print(f"    - {key}: {value}")

print(f"\n[OK] Raw JSON structure is valid")
EOF
```

**Success Criteria:**
- ✅ JSON file is valid and readable
- ✅ Contains meta and results keys
- ✅ Meta has name, website, found count, and extracted_at timestamp
- ✅ Results array contains measurement records
- ✅ Sample record has location_id, parameter, value, datetime fields

---

### Sub-task 2.3: Check XCom Data (15 min)

**Objective:** Verify extraction result data is passed correctly

**List XCom values (via Airflow UI):**
1. Navigate to http://localhost:8080
2. Click **DAGs** → `openaq_to_athena_pipeline`
3. Click the DAG run (completed or running)
4. Click **extract_all_vietnam_locations** task
5. Go to **XCom** tab

**Or via CLI (if task was run via scheduler):**
```bash
docker exec airflow-webserver airflow tasks xcom-get openaq_to_athena_pipeline extract_all_vietnam_locations return_value 2024-12-28
```

**Expected XCom output structure:**
```json
{
  "status": "SUCCESS",
  "location_count": 53,
  "record_count": 312,
  "files_uploaded": [
    "s3://openaq-data-pipeline/aq_raw_test/2025/12/28/06/raw_vietnam_national_20251228T061747.json"
  ]
}
```

**Success Criteria:**
- ✅ XCom contains return value with status
- ✅ Status is SUCCESS
- ✅ location_count = 53 (Vietnam locations)
- ✅ record_count > 0 (measurement records)
- ✅ files_uploaded contains correct S3 path to aq_raw_test/

---

## Phase 3: Test Glue Transformation Pipeline

**Duration:** 2 hours
**Objective:** Verify Glue job transforms data correctly

### Sub-task 3.1: Trigger Glue Transform Task (30 min)

**Objective:** Run Glue transformation triggered from Airflow

**Test Glue trigger task:**
```bash
airflow tasks test openaq_to_athena_pipeline trigger_glue_transform_job 2024-12-28
```

**Expected output (Dev environment):**
```
[START] Triggering Glue Transform Job: openaq_transform_measurements_dev
[INFO] Extraction metadata: locations=53, records=1520
[INFO] Glue Job Arguments:
  --input_path: s3://openaq-data-pipeline/aq_raw_test/
  --output_path: s3://openaq-data-pipeline/aq_dev/marts/vietnam/
  --env: dev
[OK] Glue job triggered successfully
[INFO] Job run ID: jr_abc123xyz
```

**Note:** For production (PIPELINE_ENV=prod), uses openaq_transform_measurements_prod with aq_raw_prod/ input and aq_prod/ output

**Success Criteria:**
- ✅ Task completes without errors
- ✅ Glue job ID returned
- ✅ Job ID stored in XCom

---

### Sub-task 3.2: Monitor Glue Job Execution (30 min)

**Objective:** Track Glue job progress

**Test wait sensor task:**
```bash
airflow tasks test openaq_to_athena_pipeline wait_glue_transform_task 2024-12-28
```

**Monitor via AWS CLI:**
```bash
# Get job run  (Dev)
aws glue get-job-run --job-name openaq_transform_measurements_dev --run-id $JOB_RUN_ID --region ap-southeast-1 --query 'JobRun.[JobRunState,ExecutionTime,ErrorMessage]'

# For production, use: openaq_transform_measurements_prod
  --run-id $JOB_RUN_ID \
  --region ap-southeast-1 \
  --query 'JobRun.[JobRunState,ExecutionTime,ErrorMessage]'
```

**Expected progression:**
1. `RUNNING` - Job is executing
2. `SUCCEEDED` - Job completed successfully

**Check CloudWatch logs:**
```bash
aws logs tail /aws-glue/jobs/output --follow --filter-pattern "openaq_transform" | head -50
```

**Success Criteria:**
- ✅ Job reaches SUCCEEDED state
- ✅ No errors in CloudWatch logs
- ✅ Execution time < 15 minutes
- ✅ Sensor task completes

---

### Sub-task 3.3: Validate Transformed Data (30 min)

**Objective:** Verify transformed Parquet output

**Check S3 output:**
```bash
aws s3 ls s3://openaq-data-pipeline/aq_dev/marts/vietnam/ --recursive | tail -20

# Expected: year=2024/month=12/day=28/*.parquet
```

**Download and inspect:**
```bash
aws s3 cp s3://openaq-data-pipeline/aq_dev/marts/vietnam/year=2024/month=12/day=28/part-00000.parquet ./output.parquet
```

**Validate transformed data:**
```bash
python3 << 'EOF'
import pyarrow.parquet as pq
import pandas as pd

table = pq.read_table('output.parquet')
df = table.to_pandas()

print(f"[INFO] Total records: {len(df)}")
print(f"[INFO] Columns: {list(df.columns)}")
print(f"\n[INFO] Sample data:")
print(df.head(3))

# Check for duplicates
duplicates = df.groupby(['location_id', 'datetime']).size()
if (duplicates > 1).any():
    print(f"\n[WARNING] Found {(duplicates > 1).sum()} duplicate records")
else:
    print(f"\n[OK] No duplicate records")

# Check metadata completeness
null_cols = df[['city_name', 'country_code', 'latitude', 'longitude']].isnull().sum()
if null_cols.sum() == 0:
    print(f"[OK] All metadata fields populated")
else:
    print(f"[WARNING] Found {null_cols.sum()} null values in metadata")
EOF
```

**Success Criteria:**
- ✅ Output Parquet files exist
- ✅ Record count > 0
- ✅ No duplicate records
- ✅ Metadata fields populated
- ✅ Partitioning correct (year=/month=/day=)

---

## Phase 4: Test End-to-End Pipeline

**Duration:** 2 hours
**Objective:** Run complete pipeline and validate results

### Sub-task 4.1: Run Complete DAG (30 min)

**Objective:** Execute full pipeline from extraction to validation

**Trigger complete DAG:**
```bash
# Test mode (no database write)
airflow dags test openaq_to_athena_pipeline 2024-12-28

# Or actual run
airflow dags trigger openaq_to_athena_pipeline --exec-date 2024-12-28
```

**Monitor in Airflow UI:**
1. Navigate to http://localhost:8080
2. Go to DAGs → `openaq_to_athena_pipeline`
3. Click latest DAG run
4. Watch task progression:
   - ✅ `extract_all_vietnam_locations`
   - ✅ `trigger_glue_transform_job`
   - ✅ `wait_glue_transform_task`
   - ✅ `trigger_crawler_task`
   - ✅ `wait_crawler_task`
   - ✅ `validate_athena_data`

**Expected duration:** 10-20 minutes total

**Success Criteria:**
- ✅ All tasks complete successfully
- ✅ No task failures
- ✅ DAG completes within 25 minutes
- ✅ Each task transitions smoothly

---

### Sub-task 4.2: Validate with Athena Queries (30 min)

**Objective:** Verify data is queryable and correct
 (Dev - use openaq_prod for production)
**Query 1: Check data availability**
```sql
SELECT COUNT(*) as total_records
FROM openaq_dev.aq_vietnam
WHERE year = '2024' AND month = '12' AND day = '28';
```

**Query 2: Check metadata completeness**
```sql
SELECT
    COUNT(*) as total_records,
    COUNT(city_name) as city_populated,
    COUNT(DISTINCT location_id) as unique_locations,
    COUNT(CASE WHEN city_name = 'Unknown' THEN 1 END) as unknown_cities
FROM openaq_dev.aq_vietnam
WHERE year = '2024' AND month = '12' AND day = '28';
```

**Query 3: Check for duplicates**
```sql
SELECT location_id, datetime, COUNT(*) as duplicate_count
FROM openaq_dev.aq_vietnam
WHERE year = '2024' AND month = '12' AND day = '28'
GROUP BY location_id, datetime
HAVING COUNT(*) > 1
LIMIT 10;
```

**Query 4: Sample data inspection**
```sql
SELECT
    location_id,
    datetime,
    city_name,
    country_code,
    latitude,
    longitude,
    pm25,
    pm10
FROM openaq_dev.aq_vietnam
WHERE year = '2024' AND month = '12' AND day = '28'
LIMIT 10;
```
Note:** Replace `openaq_dev` with `openaq_prod` for production environment queries

**
**Success Criteria:**
- ✅ All queries execute successfully
- ✅ Query 1: Records > 0
- ✅ Query 2: city_populated = total_records, unknown_cities = 0
- ✅ Query 3: No duplicate records
- ✅ Query 4: Data looks realistic and complete

---

### Sub-task 4.3: Test Error Scenarios (30 min)

**Objective:** Verify error handling

**Scenario 1: Missing AWS Connection**
```bash
# Temporarily delete AWS connection
airflow connections delete aws_default

# Try to run extraction (should fail)
airflow tasks test openaq_to_athena_pipeline extract_all_vietnam_locations 2024-12-28

# Restore connection
airflow connections add aws_default ...
```

**Expected:** Task fails with clear error message

**Scenario 2: Invalid S3 Path**
```bash
# Modify openaq_pipeline to use invalid bucket
# Run extraction
# Should fail with S3 error

# Revert changes
```

**Expected:** Task fails with clear error message

**Scenario 3: Task Retry**
```bash
# All tasks have retry=2, retry_delay=5 minutes
# Manually stop a running task
# Should retry automatically
```

**Expected:** Task retries after 5 minutes

**Success Criteria:**
- ✅ Errors have clear messages
- ✅ Retries work correctly
- ✅ Pipeline recovers gracefully

---

## Phase 5: Production Deployment & Monitoring

**Duration:** 1.5 hours
**Objective:** Set up monitoring, alerting, and schedule production runs

### Sub-task 5.1: Configure DAG Schedule (30 min)

**Objective:** Enable scheduled execution

**File:** `dags/openaq_dag.py` (line 31)

**Current:**
```python
schedule_interval=None,  # Manual trigger
```

**Change to production schedule:**
```python
schedule_interval='0 0 * * *',  # Daily at midnight UTC
```

**Apply changes:**
```bash
# Restart Airflow scheduler
docker-compose restart airflow-scheduler

# Or if running natively
airflow scheduler restart
```

**Verify schedule:**
```bash
airflow dags next-execution openaq_to_athena_pipeline
airflow dags unpause openaq_to_athena_pipeline
```

**Success Criteria:**
- ✅ Schedule configured correctly
- ✅ Next execution time is correct
- ✅ DAG is unpaused (enabled)

---

### Sub-task 5.2: Set Up Monitoring (30 min)

**Objective:** Create CloudWatch dashboards and alarms

**Create SNS Topic for Alerts:**
```bash
aws sns create-topic \
  --name openaq-pipeline-alerts \
  --region ap-southeast-1

aws sns subscribe \
  --topic-arn arn:aws:sns:ap-southeast-1:ACCOUNT_ID:openaq-pipeline-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com \
  --region ap-southeast-1
```

**Create CloudWatch Alarm for Glue Job Failures:**
```bash
aws cloudwatch put-metric-alarm \
  --alarm-name openaq-glue-job-failures \
  --alarm-description "Alert on Glue job failures" \
  --metric-name glue.driver.aggregate.numFailedTasks \
  --namespace AWS/Glue \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --alarm-actions arn:aws:sns:ap-southeast-1:ACCOUNT_ID:openaq-pipeline-alerts \
  --region ap-southeast-1
```

**Configure Airflow Email Alerts:**

**File:** `dags/openaq_dag.py` (lines 14-24)
```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your-email@example.com'],  # Add your email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
```

**Success Criteria:**
- ✅ SNS topic created
- ✅ Email subscription confirmed
- ✅ CloudWatch alarms configured
- ✅ Airflow email alerts enabled

---

### Sub-task 5.3: Update Documentation (30 min)

**Objective:** Document pipeline for team

**Update CLAUDE.md:**
- Add section on current Python-based extraction
- Document DAG structure and task dependencies
- Add troubleshooting guide

**Create Runbook:**

**File:** `doc/RUNBOOK.md`
```markdown
# OpenAQ Pipeline Operations Runbook

## Common Issues and Solutions

### Extract Task Fails
- Check Airflow task logs
- Verify AWS connection exists
- Check OpenAQ API status

### Glue Job Fails
- Check Glue job logs in CloudWatch
- Verify input files in S3
- Check Glue IAM role permissions

### Crawler Task Fails
- Verify S3 output from Glue job
- Check crawler IAM role
- Check Glue database exists

## Monitoring
- Airflow UI: http://localhost:8080
- CloudWatch: AWS Console → CloudWatch → Dashboards
- Task logs: Airflow UI → DAG → Task → Logs

## Rollback
- To pause pipeline: `airflow dags pause openaq_to_athena_pipeline`
- To resume: `airflow dags unpause openaq_to_athena_pipeline`
```

**Success Criteria:**
- ✅ CLAUDE.md updated
- ✅ Runbook created
- ✅ Documentation committed to git

---

## Success Criteria Summary

**Phase 1: Prerequisites**
- ✅ Dependencies verified
- ✅ Airflow configured and running
- ✅ AWS services accessible

**Phase 2: Extraction**
- ✅ Extract task runs successfully
- ✅ Data uploaded to S3
- ✅ XCom contains extraction result

**Phase 3: Glue Transformation**
- ✅ Glue job triggered successfully
- ✅ Transformed Parquet created
- ✅ Metadata fields populated

**Phase 4: End-to-End**
- ✅ Complete pipeline runs successfully
- ✅ Athena queries return correct data
- ✅ Error scenarios handled gracefully

**Phase 5: Production**
- ✅ DAG scheduled for daily runs
- ✅ Monitoring and alerting configured
- ✅ Documentation updated

---

## Files Modified

### Files to Update:
1. **`dags/openaq_dag.py`** (optional)
   - Change schedule_interval for production (Phase 5.1)
   - Add email alerts (Phase 5.2)

2. **`CLAUDE.md`** (Phase 5.3)
   - Add Python extraction section
   - Update architecture diagram

3. **`doc/RUNBOOK.md`** (new file, Phase 5.3)
   - Create operational runbook

### Files to Review (no changes needed):
- `pipelines/openaq_pipeline.py` - Extraction logic
- `etls/openaq_etl.py` - OpenAQ API integration
- `glue_jobs/process_openaq_raw.py` - Transformation logic
- `utils/aws_utils.py` - S3 utilities
- `tests/` - Unit tests

---

## Quick Reference

**Start DAG scheduler:**
```bash
docker-compose up -d
# or
airflow scheduler &
```

**Test individual tasks:**
```bash
airflow tasks test openaq_to_athena_pipeline extract_all_vietnam_locations 2024-12-28
airflow tasks test openaq_to_athena_pipeline trigger_glue_transform_job 2024-12-28
airflow tasks test openaq_to_athena_pipeline wait_glue_transform_task 2024-12-28
airflow tasks test openaq_to_athena_pipeline trigger_crawler_task 2024-12-28
airflow tasks test openaq_to_athena_pipeline wait_crawler_task 2024-12-28
airflow tasks test openaq_to_athena_pipeline validate_athena_data 2024-12-28
```

**View logs:**
```bash
docker-compose logs -f airflow-scheduler
docker-compose logs -f  (Dev environment):**
```bash
aws glue get-job-runs --job-name openaq_transform_measurements_dev --max-results 5 --region ap-southeast-1
```

**For production:**
```bash
aws glue get-job-runs --job-name openaq_transform_measurements_prod --max-results 5ws glue get-job-runs \
  --job-name openaq_transform_measurements_prod \
  --max-results 5 \
  --region ap-southeast-1
```

---

## Next Steps After Implementation

1. **Monitor First Week:** Watch for failures, verify data quality
2. **Optimize:** Adjust Glue worker count, Lambda memory (if using Lambda later)
3. **Scale:** Add more regions/countries, increase frequency
4. **Enhance:** Add data quality metrics, implement data lineage tracking
