# OpenAQ Historical Backfill 2025 - Implementation Plan

## Executive Summary

**Objective**: Extract full year 2025 air quality data for Vietnam (January 1 - December 31, 2025)

**Strategy**:
- 12 monthly chunks processed SEQUENTIALLY (avoid API rate limits)
- Backward-compatible Lambda modification
- New separate DAG (zero impact on existing daily pipeline)
- Reuse existing Glue Transform → Crawler → Athena validation flow

**Timeline**:
- Development: 2-3 days
- Testing: 1 day (single month test)
- Execution: 6-8 hours (12 months × 30-40 min/month)

---

## 1. Lambda Handler Modifications

### File: `lambda_functions/openaq_fetcher/handler.py`

### Changes Required

**Current Logic (Line 268-269)**:
```python
date_to = datetime.utcnow()
date_from = date_to - timedelta(hours=lookback_hours)
```

**New Logic (Backward Compatible)**:
```python
# Support both relative (lookback_hours) and absolute (date_from_iso/date_to_iso) date ranges
if 'date_from_iso' in event and 'date_to_iso' in event:
    # Historical backfill mode - absolute dates
    date_from = datetime.fromisoformat(event['date_from_iso'].replace('Z', '+00:00'))
    date_to = datetime.fromisoformat(event['date_to_iso'].replace('Z', '+00:00'))
    date_from = date_from.replace(tzinfo=None)
    date_to = date_to.replace(tzinfo=None)
    print(f"[INFO] Using absolute date range: {date_from.isoformat()} to {date_to.isoformat()}")
else:
    # Daily pipeline mode - relative lookback
    lookback_hours = event.get('lookback_hours', 24)
    date_to = datetime.utcnow()
    date_from = date_to - timedelta(hours=lookback_hours)
    print(f"[INFO] Using relative lookback: {lookback_hours} hours")
```

### Updated Event Schema

**Historical Backfill (new)**:
```json
{
    "file_name": "vietnam_backfill_2025_01_20250131_120000",
    "vietnam_wide": true,
    "date_from_iso": "2025-01-01T00:00:00Z",
    "date_to_iso": "2025-01-31T23:59:59Z",
    "required_parameters": ["PM2.5", "PM10", "NO2", "O3", "SO2", "CO", "BC"],
    "rate_limit_delay": 10
}
```

### Validation Update (handler.py, line 104-134)

```python
def validate_event(event: dict) -> dict:
    """Validate Lambda event payload (updated for backfill support)."""

    # Check if this is backfill mode or daily mode
    is_backfill = 'date_from_iso' in event or 'date_to_iso' in event

    if is_backfill:
        # Backfill mode - require absolute dates
        required_fields = ['file_name', 'vietnam_wide', 'date_from_iso', 'date_to_iso']
        for field in required_fields:
            if field not in event:
                raise ValueError(f"[FAIL] Missing required field for backfill mode: {field}")

        # Validate ISO format
        try:
            datetime.fromisoformat(event['date_from_iso'].replace('Z', '+00:00'))
            datetime.fromisoformat(event['date_to_iso'].replace('Z', '+00:00'))
        except ValueError as e:
            raise ValueError(f"[FAIL] Invalid ISO datetime format: {str(e)}")
    else:
        # Daily mode - require lookback_hours
        required_fields = ['file_name', 'vietnam_wide', 'lookback_hours']
        for field in required_fields:
            if field not in event:
                raise ValueError(f"[FAIL] Missing required field in event: {field}")

        if not isinstance(event['lookback_hours'], int) or event['lookback_hours'] <= 0:
            raise ValueError("[FAIL] lookback_hours must be a positive integer")

    # Common validations
    if not isinstance(event['file_name'], str):
        raise ValueError("[FAIL] file_name must be a string")
    if not isinstance(event['vietnam_wide'], bool):
        raise ValueError("[FAIL] vietnam_wide must be a boolean")

    # Set defaults
    event.setdefault('required_parameters', ['PM2.5', 'PM10', 'NO2', 'O3', 'SO2', 'CO', 'BC'])
    event.setdefault('rate_limit_delay', 0)

    return event
```

---

## 2. Rate Limiting Implementation

### File: `lambda_functions/openaq_fetcher/extract_api.py`

Add delay between sensor requests to avoid 429 errors:

```python
def extract_measurements(headers: dict, sensor_ids: list,
                        date_from: datetime, date_to: datetime,
                        rate_limit_delay: int = 0) -> list:
    """
    Extract hourly air quality measurements from sensors.

    Args:
        rate_limit_delay: Seconds to wait between sensor requests (default: 0)
    """
    all_measurements = []
    total_records = 0

    print(f"[INFO] Extracting measurements from {len(sensor_ids)} sensors")
    print(f"       Period: {date_from.isoformat()} to {date_to.isoformat()}")
    if rate_limit_delay > 0:
        print(f"       Rate limit delay: {rate_limit_delay} seconds between requests")

    for idx, sensor_id in enumerate(sensor_ids, 1):
        try:
            # Add rate limiting delay
            if rate_limit_delay > 0 and idx > 1:
                import time
                time.sleep(rate_limit_delay)

            meas_url = f"{BASE_URL}/sensors/{sensor_id}/measurements"
            # ... rest of extraction logic
```

Update handler.py to pass rate_limit_delay (line 271):
```python
measurements = extract_measurements(
    headers,
    active_sensor_ids,
    date_from,
    date_to,
    rate_limit_delay=event.get('rate_limit_delay', 0)
)
```

---

## 3. New DAG Structure

### File: `dags/openaq_historical_backfill_2025_dag.py` (NEW)

```python
"""
DAG for historical backfill of OpenAQ data for Vietnam - Year 2025
One-time execution: Extracts 12 months of data (Jan-Dec 2025) in sequential batches
"""
import sys
sys.path.insert(0, '/opt/airflow/')

from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from tasks.glue_transform_tasks import create_glue_transform_tasks
from tasks.catalog_tasks import create_catalog_tasks
from tasks.validation_tasks import create_validate_athena_task
import json

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='openaq_historical_backfill_2025',
    default_args=default_args,
    description='One-time historical backfill for Vietnam 2025 (12 monthly chunks)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['openaq', 'backfill', 'historical', '2025', 'vietnam']
)

# Month definitions
MONTHS = [
    {'month': 1, 'name': 'January', 'days': 31},
    {'month': 2, 'name': 'February', 'days': 28},
    {'month': 3, 'name': 'March', 'days': 31},
    {'month': 4, 'name': 'April', 'days': 30},
    {'month': 5, 'name': 'May', 'days': 31},
    {'month': 6, 'name': 'June', 'days': 30},
    {'month': 7, 'name': 'July', 'days': 31},
    {'month': 8, 'name': 'August', 'days': 31},
    {'month': 9, 'name': 'September', 'days': 30},
    {'month': 10, 'name': 'October', 'days': 31},
    {'month': 11, 'name': 'November', 'days': 30},
    {'month': 12, 'name': 'December', 'days': 31},
]


def create_monthly_extract_task(dag, month_info):
    """Create Lambda extraction task for a specific month."""
    month_num = month_info['month']
    month_name = month_info['name']
    last_day = month_info['days']

    date_from = f"2025-{month_num:02d}-01T00:00:00Z"
    date_to = f"2025-{month_num:02d}-{last_day}T23:59:59Z"

    timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%S')
    file_name = f"vietnam_backfill_2025_{month_num:02d}_{timestamp}"

    payload = {
        "file_name": file_name,
        "vietnam_wide": True,
        "date_from_iso": date_from,
        "date_to_iso": date_to,
        "required_parameters": ['PM2.5', 'PM10', 'NO2', 'O3', 'SO2', 'CO', 'BC'],
        "rate_limit_delay": 10
    }

    task = LambdaInvokeFunctionOperator(
        task_id=f'extract_month_{month_num:02d}_{month_name.lower()}',
        function_name='openaq-fetcher',
        payload=json.dumps(payload),
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        log_type='Tail',
        dag=dag,
        do_xcom_push=True,
        execution_timeout=timedelta(minutes=45)
    )

    return task


def create_monthly_pipeline_tasks(dag, month_info):
    """Create complete pipeline for one month."""
    month_num = month_info['month']

    extract_task = create_monthly_extract_task(dag, month_info)

    trigger_glue, wait_glue = create_glue_transform_tasks(dag)
    trigger_glue.task_id = f'trigger_glue_month_{month_num:02d}'
    wait_glue.task_id = f'wait_glue_month_{month_num:02d}'

    trigger_crawler, wait_crawler = create_catalog_tasks(dag)
    trigger_crawler.task_id = f'trigger_crawler_month_{month_num:02d}'
    wait_crawler.task_id = f'wait_crawler_month_{month_num:02d}'

    validate_task = create_validate_athena_task(dag)
    validate_task.task_id = f'validate_month_{month_num:02d}'

    extract_task >> trigger_glue >> wait_glue >> trigger_crawler >> wait_crawler >> validate_task

    return extract_task, validate_task


# Build sequential pipeline for all 12 months
previous_month_validate = None

for month_info in MONTHS:
    extract_task, validate_task = create_monthly_pipeline_tasks(dag, month_info)

    if previous_month_validate:
        previous_month_validate >> extract_task

    previous_month_validate = validate_task
```

### Task Graph

```
Month 1: January → Extract → Glue Transform → Crawler → Validate
    ↓
Month 2: February → Extract → Glue Transform → Crawler → Validate
    ↓
Month 3: March → Extract → Glue Transform → Crawler → Validate
    ↓
... (continues for all 12 months)
    ↓
Month 12: December → Extract → Glue Transform → Crawler → Validate
```

**Execution Strategy**: SEQUENTIAL (recommended to avoid API rate limits)

---

## 4. S3 Storage Structure

### File Naming Convention

- **Daily pipeline**: `raw_vietnam_national_20250115T103000.json`
- **Historical backfill**: `raw_vietnam_backfill_2025_01_20250131T120000.json`

### Storage Location

Both daily and historical data go to SAME location (no conflicts due to date partitioning):

```
s3://openaq-data-pipeline/aq_raw/2025/01/01/00/raw_vietnam_backfill_2025_01_*.json
s3://openaq-data-pipeline/aq_raw/2025/01/01/01/raw_vietnam_backfill_2025_01_*.json
...
s3://openaq-data-pipeline/aq_raw/2025/12/31/23/raw_vietnam_backfill_2025_12_*.json
```

**Why No Conflicts?**
- Daily pipeline extracts T-24h (recent data)
- Historical backfill extracts past dates (2025 data)
- Different file names prevent overwrites
- Glue Transform deduplicates by `location_id + datetime`

---

## 5. Testing Strategy

### Phase 1: Single Month Test (January 2025)

1. Modify Lambda handler with backward-compatible date handling
2. Deploy Lambda (update code + increase timeout to 900s)
3. Create test DAG with only January 2025
4. Manually trigger and monitor

### Test DAG: `openaq_historical_backfill_2025_test_dag.py`

```python
# Same structure as full DAG, but only January
MONTHS = [
    {'month': 1, 'name': 'January', 'days': 31},
]
```

### Validation Queries (Athena)

```sql
-- 1. Check January 2025 record count
SELECT
    COUNT(*) as total_records,
    MIN(datetime) as earliest_datetime,
    MAX(datetime) as latest_datetime
FROM openaq_dev.vietnam
WHERE year = '2025' AND month = '01';
-- Expected: ~35,000-45,000 records

-- 2. Check location coverage
SELECT
    location_id,
    city_name,
    COUNT(*) as measurement_count
FROM openaq_dev.vietnam
WHERE year = '2025' AND month = '01'
GROUP BY location_id, city_name
ORDER BY measurement_count DESC;
-- Expected: ~40 locations

-- 3. Check parameter availability
SELECT
    SUM(CASE WHEN pm25 IS NOT NULL THEN 1 ELSE 0 END) as pm25_count,
    SUM(CASE WHEN pm10 IS NOT NULL THEN 1 ELSE 0 END) as pm10_count,
    SUM(CASE WHEN no2 IS NOT NULL THEN 1 ELSE 0 END) as no2_count,
    SUM(CASE WHEN o3 IS NOT NULL THEN 1 ELSE 0 END) as o3_count,
    SUM(CASE WHEN so2 IS NOT NULL THEN 1 ELSE 0 END) as so2_count,
    SUM(CASE WHEN co IS NOT NULL THEN 1 ELSE 0 END) as co_count
FROM openaq_dev.vietnam
WHERE year = '2025' AND month = '01';
```

---

## 6. Deployment Steps

### Step 1: Lambda Code Update (Day 1)

1. **Backup current Lambda**:
   ```bash
   cd lambda_functions/openaq_fetcher
   git checkout -b feature/backfill-2025-support
   ```

2. **Modify files**:
   - `handler.py`: Add absolute date handling
   - `extract_api.py`: Add rate_limit_delay parameter

3. **Update Lambda configuration** (AWS Console):
   - Timeout: 300s → **900s** (15 minutes)
   - Memory: 1024 MB → **1536 MB**

4. **Deploy to AWS Lambda**:
   ```bash
   cd lambda_functions/openaq_fetcher
   zip -r openaq_fetcher.zip . -x "*.git*" -x "*__pycache__*"
   aws lambda update-function-code \
     --function-name openaq-fetcher \
     --zip-file fileb://openaq_fetcher.zip \
     --region ap-southeast-1
   ```

5. **Test Lambda directly** (AWS Console):
   ```json
   {
     "file_name": "test_backfill_jan_2025",
     "vietnam_wide": true,
     "date_from_iso": "2025-01-01T00:00:00Z",
     "date_to_iso": "2025-01-03T23:59:59Z",
     "required_parameters": ["PM2.5", "PM10"],
     "rate_limit_delay": 5
   }
   ```

### Step 2: Test DAG Creation (Day 1)

1. Create test DAG with only January 2025
2. Copy to Airflow (Docker volume mount)
3. Verify DAG loads in Airflow UI

### Step 3: Single Month Test (Day 2)

1. Trigger test DAG
2. Monitor Lambda logs (CloudWatch)
3. Monitor S3 uploads
4. Monitor Glue Job execution
5. Run validation queries

### Step 4: Full Year DAG Creation (Day 3)

1. Create full DAG with all 12 months
2. Code review
3. Commit changes

### Step 5: Production Execution (Day 4)

1. Trigger full backfill DAG
2. Monitor progress (estimated 6-8 hours)
3. Checkpoint after each month
4. Final validation

---

## 7. Risk Mitigation

### Risk 1: API Rate Limiting (429 Errors)

**Mitigation**:
- Rate limit delay: 10 seconds between sensor requests
- Sequential months processing
- Airflow retries (1 retry with 10-minute delay)

**If 429 occurs**: Increase `rate_limit_delay` to 15-20 seconds

### Risk 2: Lambda Timeout

**Current**: 300s (5 min)
**Recommended**: 900s (15 min)

**Why**: ~40 sensors × 10s delay = 400s + API time = ~9-10 min expected

### Risk 3: Data Duplication

**Mitigation**:
- Glue deduplication by `location_id + datetime`
- Distinct file names with `backfill` identifier
- Athena validation queries

### Risk 4: Partial Month Failure

**Recovery**:
1. Clear failed task in Airflow
2. Rerun from failed task
3. Pipeline continues from that month

---

## 8. Timeline Estimate

### Development Phase (2-3 days)

| Task | Duration |
|------|----------|
| Lambda handler modification | 4 hours |
| Lambda testing | 2 hours |
| Test DAG creation | 2 hours |
| Single month test | 2 hours |
| Validation queries | 2 hours |
| Full DAG creation | 1 hour |
| Code review | 2 hours |
| **Total** | **15 hours (2 days)** |

### Testing Phase (1 day)

| Task | Duration |
|------|----------|
| Lambda deployment | 30 min |
| Test DAG execution | 45 min |
| Glue Transform | 10 min |
| Crawler run | 5 min |
| Validation | 30 min |
| Review | 1 hour |
| **Total** | **3 hours** |

### Execution Phase (6-8 hours)

| Month | Total Time |
|-------|------------|
| Each month | ~45-50 min |
| All 12 months | ~10 hours |

**Recommended**: Run overnight or during low-traffic hours

---

## 9. Monitoring and Verification

### Real-time Monitoring

1. **Airflow UI**: Task status tracking
2. **CloudWatch Logs**: Lambda and Glue job logs
3. **S3 File Count**: Track uploads per month

### Post-Execution Validation

```sql
-- Record count verification
SELECT
    year,
    month,
    COUNT(*) as records,
    COUNT(DISTINCT location_id) as locations,
    MIN(datetime) as earliest,
    MAX(datetime) as latest
FROM openaq_dev.vietnam
WHERE year = '2025'
GROUP BY year, month
ORDER BY month;

-- Data quality checks
SELECT
    SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) as null_location_id,
    SUM(CASE WHEN datetime IS NULL THEN 1 ELSE 0 END) as null_datetime
FROM openaq_dev.vietnam
WHERE year = '2025';
```

---

## 10. Success Criteria

### Technical Success
- [OK] All 12 months extracted without failures
- [OK] No duplicate records in Athena tables
- [OK] All expected parameters present
- [OK] Date range coverage: 2025-01-01 to 2025-12-31
- [OK] ~40 locations represented
- [OK] Total records: 400,000-550,000

### Operational Success
- [OK] No impact on daily pipeline
- [OK] Backward compatibility maintained
- [OK] Lambda timeout <15 min per month
- [OK] No API rate limit errors
- [OK] S3 storage within limits (~600 MB)

### Data Quality Success
- [OK] <1% null values in critical columns
- [OK] Date gaps <5%
- [OK] 80%+ locations have data for all months
- [OK] Parameter availability: PM2.5 >90%, PM10 >90%

---

## Critical Files for Implementation

| File Path | Action | Purpose |
|-----------|--------|---------|
| `lambda_functions/openaq_fetcher/handler.py` | MODIFY | Add absolute date support (lines 104-134, 268-270) |
| `lambda_functions/openaq_fetcher/extract_api.py` | MODIFY | Add rate limiting (line 194, 220-266) |
| `dags/openaq_historical_backfill_2025_dag.py` | CREATE | New backfill DAG with 12 monthly tasks |
| `dags/tasks/lambda_extract_tasks.py` | REFERENCE | Pattern for Lambda invocation |
| `pipelines/glue_pipeline.py` | NO CHANGE | Already supports variable input paths |

---

## Final Recommendations

1. **Start with single month test** (January 2025) to validate end-to-end flow
2. **Use SEQUENTIAL execution** for all 12 months (safer, avoids rate limits)
3. **Increase Lambda timeout to 900s** (maximum allowed)
4. **Use 10-second rate limit delay** (balances speed vs API limits)
5. **Process per-month Glue Transform** (faster feedback, easier debugging)
6. **Monitor first 2-3 months closely** before letting it run unattended

---

## Next Steps

1. Review and approve this plan
2. Execute Step 1: Lambda Code Update
3. Execute Step 2-3: Testing with January 2025
4. Execute Step 4-5: Full Year Backfill

**Estimated Total Time**: 3-4 days development + 8 hours execution
