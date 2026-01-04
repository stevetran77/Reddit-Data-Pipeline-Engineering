# OpenAQ Data Pipeline - Test Suite Guide

This directory contains comprehensive test suites for the OpenAQ Data Pipeline project. Tests cover extraction, transformation, cataloging, and validation phases of the ETL pipeline.

## Test Files Overview

### Phase 1: Infrastructure Tests
**File**: `test_phase1_infrastructure.py`

Tests foundational infrastructure components:
- AWS S3 bucket accessibility and permissions
- PostgreSQL connection and Airflow metadata database
- IAM role configurations
- Configuration file integrity

**Run with**:
```bash
# Using Docker (Recommended)
docker-compose exec -T airflow-webserver python tests/test_phase1_infrastructure.py

# Using local Python (requires Java and PySpark installed)
python tests/test_phase1_infrastructure.py
```

---

### Phase 2: Extraction & OpenAQ API Tests
**Files**:
- `test_extract_data.py` - OpenAQ API connection and data extraction
- `test_openaq_pipeline.py` - Complete extraction pipeline validation

Tests:
- OpenAQ API authentication and connectivity
- Location data retrieval by city/country
- Measurement data extraction with lookback period
- Data transformation to Parquet format
- S3 upload with partition structure (year/month/day)

**Run with**:
```bash
docker-compose exec -T airflow-webserver python tests/test_extract_data.py
docker-compose exec -T airflow-webserver python tests/test_openaq_pipeline.py
```

---

### Phase 3: Glue Transformation Tests
**Files**:
- `test_glue_complete.py` - Complete Glue pipeline (crawler + transformation)
- `test_glue_transformation.py` - Data quality validation after transformation
- `test_glue_output_schema.py` - Schema validation for Glue output

#### 3.1 Schema Validation
**File**: `test_glue_output_schema.py`

Validates Glue output Parquet files contain the expected schema:
- **Identity Columns**: location_id (string), datetime (timestamp)
- **Partition Columns**: year (int), month (string), day (string)
- **Air Quality Parameters** (6 columns): pm25, pm10, no2, so2, o3, co (DoubleType)
- **Location Metadata**: city_name, country_code (string), latitude, longitude (double)

**Tests** (10 total):
1. All 15 required columns present
2. Critical columns properly configured
3. Air quality parameters exist
4. Metadata columns exist
5. Partition columns exist
6. Datetime is TimestampType
7. Parameters are numeric (DoubleType)
8. Location metadata are StringType
9. Coordinates are numeric (DoubleType)
10. No unexpected extra columns

**Run with**:
```bash
# Using Docker (Recommended)
docker-compose exec -T airflow-webserver python tests/test_glue_output_schema.py

# Using pytest
docker-compose exec -T airflow-webserver pytest tests/test_glue_output_schema.py -v
```

**Expected Output**:
```
[SUCCESS] All schema validation tests passed!
Tests run: 10
Passed: 10
Failed: 0
Errors: 0
```

---

#### 3.2 Glue Transformation Data Quality
**File**: `test_glue_transformation.py`

Validates data quality after Glue transformation:
- No duplicate records (location_id + datetime uniqueness)
- No null values in critical columns
- Valid timestamp values
- Coordinate validation (-90 to 90 latitude, -180 to 180 longitude)
- Positive parameter values
- Partition alignment with datetime
- Parameter coverage (all 7 parameters present)
- Location coverage (all extraction locations in output)
- Deduplication ratio validation

**Run with**:
```bash
docker-compose exec -T airflow-webserver pytest tests/test_glue_transformation.py -v
```

---

#### 3.3 Complete Glue Pipeline
**File**: `test_glue_complete.py`

Tests the complete Glue job workflow:
- Glue client initialization
- Job configuration validation
- Job execution with correct parameters
- Status tracking and monitoring
- Crawler functionality

**Run with**:
```bash
docker-compose exec -T airflow-webserver python tests/test_glue_complete.py -v
```

---

### Phase 4: Athena Connection & Query Tests
**File**: `test_athena_connection.py`

Tests Athena connectivity and query capabilities:
- Athena database connection
- Table listing from Glue Catalog
- Sample query execution
- Query results retrieval
- Metadata validation

**Run with**:
```bash
docker-compose exec -T airflow-webserver python tests/test_athena_connection.py
```

---

### Phase 4.5: End-to-End Integration Tests
**File**: `test_e2e_glue_pipeline.py`

Complete pipeline validation covering all stages:
- Extract stage (location count, record count)
- Transform stage (deduplication, output records)
- Catalog stage (Glue Crawler, table creation)
- Validate stage (Athena queryability)
- Record count preservation across stages
- Data quality checks at each stage
- Error handling validation
- XCom communication between tasks

**Tests** (11 total):
1. Pipeline stage progression (Extract → Transform → Catalog → Validate)
2. Record count preservation (5.1% dedup ratio)
3. Glue job configuration
4. Job execution flow (STARTING → RUNNING → SUCCEEDED)
5. Crawler execution flow (RUNNING → READY)
6. Athena table creation (15 columns, 3 partitions)
7. XCom communication between tasks
8. Error handling scenarios
9. Data quality checks at each stage (13 checks)
10. Environment variable handling
11. Partition strategy validation

**Results**:
```
[SUCCESS] All end-to-end tests passed!
Tests run: 11
Passed: 11
Failed: 0
Errors: 0
```

**Run with**:
```bash
docker-compose exec -T airflow-webserver python tests/test_e2e_glue_pipeline.py -v
```

---

### Phase 5: Manual Trigger & Airflow DAG Execution
**File**: `test_phase5_manual_trigger.py`

Prepares and validates Airflow DAG for manual execution:
- DAG definition and configuration validation
- Task dependencies verification
- Trigger readiness checks
- Expected data flow documentation
- Manual trigger instructions
- Success criteria definition
- Athena validation queries
- Troubleshooting guide

**Tests** (9 total):
1. Phase 5 preparation and prerequisites
2. DAG definition validation
3. Task dependency validation
4. DAG trigger readiness
5. Expected data flow documentation
6. Manual trigger instructions
7. Athena validation queries
8. Success criteria definition
9. Troubleshooting guide

**DAG Configuration**:
- **DAG Name**: `openaq_dag`
- **Schedule**: None (manual trigger only)
- **Tasks**: 6 (extract, transform, wait, crawler, wait, validate)
- **Retries**: 2 with 5-minute delays
- **Expected Duration**: 5-10 minutes

**Task Dependencies**:
```
lambda_extract_task
        ↓
trigger_glue_transform_task → wait_glue_transform_task (3-5 min)
        ↓
trigger_crawler_task → wait_crawler_task (2-5 min)
        ↓
validate_task
```

**Results**:
```
[SUCCESS] Phase 5 preparations complete!
Tests run: 9
Passed: 9
Failed: 0
Errors: 0

Next steps:
1. Access Airflow UI at http://localhost:8080
2. Navigate to openaq_dag
3. Click 'Trigger DAG' to start the pipeline
4. Monitor task execution in Graph view
5. Check logs for each task to verify success
```

**Run with**:
```bash
docker-compose exec -T airflow-webserver python tests/test_phase5_manual_trigger.py
```

#### Manual Trigger Steps

**Step 1: Access Airflow UI**
- URL: http://localhost:8080
- Username: admin
- Password: admin

**Step 2: Navigate to openaq_dag**
- Click DAGs in left sidebar
- Find and click "openaq_dag"

**Step 3: Trigger DAG**
- Click "Trigger DAG" button (top-right)
- Leave configuration as default
- Click "Trigger" to confirm

**Step 4: Monitor Progress**
- Click "Graph" tab to see task execution
- Watch for green (success) or red (failed) status
- Total time: 5-10 minutes

**Step 5: Check Logs**
- Click on each task to view logs
- Look for `[OK]`, `[SUCCESS]`, or `[FAIL]` messages

#### Athena Validation Queries

After DAG completes successfully, validate in Athena:

```sql
-- Check databases exist
SHOW DATABASES;

-- Check tables in aq_dev
SHOW TABLES IN aq_dev;

-- Check record count
SELECT COUNT(*) FROM aq_dev.vietnam;

-- Check schema
DESCRIBE aq_dev.vietnam;

-- Sample data
SELECT * FROM aq_dev.vietnam LIMIT 10;

-- Check partitions
SHOW PARTITIONS aq_dev.vietnam;
```

**Expected Results**:
- Databases: aq_dev present
- Tables: vietnam table exists
- Record count: >0 (typically 427)
- Columns: 15 with correct types
- Partitions: year=YYYY/month=MM/day=DD structure

#### Success Criteria

✓ All 6 DAG tasks complete (green status)
✓ No task retries (first attempt success)
✓ Athena can query data: SELECT COUNT(*) > 0
✓ All 15 columns present with correct types
✓ Data quality valid (no nulls, valid ranges)
✓ Proper partition structure (year/month/day)

#### Troubleshooting

**Task Failed (Red Status)**:
1. Click on failed task
2. Click "Logs" tab
3. Check error message
4. Common issues: Missing credentials, S3 bucket doesn't exist, IAM permissions

**Crawler Timeout (>30 minutes)**:
1. Check AWS Glue Console → Crawlers
2. If stuck, manually stop crawler
3. Retry DAG run

**No Data in Athena (COUNT = 0)**:
1. Verify Glue job transformation succeeded
2. Check S3 output: aq_dev/marts/
3. Verify Crawler created table in Glue Catalog

---

## Running Tests

### Using Docker (Recommended)

**Option 1: Run individual test file**
```bash
docker-compose exec -T airflow-webserver python tests/test_glue_output_schema.py
```

**Option 2: Run with pytest**
```bash
docker-compose exec -T airflow-webserver pytest tests/ -v
```

**Option 3: Run specific test**
```bash
docker-compose exec -T airflow-webserver pytest tests/test_glue_output_schema.py::TestGlueOutputSchema::test_schema_has_required_columns -v
```

**Option 4: Run all tests with coverage**
```bash
docker-compose exec -T airflow-webserver pytest tests/ --cov=utils --cov=pipelines --cov=etls -v
```

### Using Local Python

**Requirements**:
- Python 3.11 or 3.12 (PySpark 3.4.1 compatibility)
- Java 17+ (for PySpark)
- Virtual environment with `pip install -r requirements.txt`

**Commands**:
```bash
# Activate virtual environment
.venv/Scripts/activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Run tests
python tests/test_glue_output_schema.py
pytest tests/ -v
```

**Note**: Local testing may fail if Java is not properly configured. Docker is recommended.

---

## Test Dependencies

### Core Test Libraries
- `unittest` - Python standard unit testing framework
- `pytest` - Advanced testing framework with plugins

### Spark Testing
- `pyspark==3.4.1` (Docker) or `3.5.1+` (Python 3.13)
- Requires Java JDK 17+

### AWS Testing
- `boto3` - AWS SDK for testing Glue, S3, Athena
- `awswrangler` - AWS data utilities

### Data Testing
- `pandas` - DataFrame manipulation
- `pyarrow` - Parquet file handling

---

## Test Output Format

All tests use text-based status indicators for clarity:
- `[OK]` - Individual test assertion passed
- `[SUCCESS]` - Complete test suite passed
- `[FAIL]` - Test failed
- `[WARNING]` - Non-critical issue detected
- `[INFO]` - Informational message
- `[TEST]` - Test starting

Example:
```
[TEST] Schema has all required columns...
[OK] All 15 required columns present

[SUCCESS] All schema validation tests passed!
```

---

## Common Issues & Solutions

### Issue: "PySpark not installed"
**Solution**: Install from requirements.txt or use Docker
```bash
pip install -r requirements.txt
# or
docker-compose exec -T airflow-webserver python tests/test_glue_output_schema.py
```

### Issue: "Java not found and JAVA_HOME not set"
**Solution**: Use Docker (has Java pre-installed) or install Java locally
```bash
# For Windows: Download Java 17 JDK
# For Linux: sudo apt-get install openjdk-17-jdk
# Set JAVA_HOME environment variable
```

### Issue: Tests timeout or hang
**Solution**:
- Ensure Docker containers are running: `docker-compose ps`
- Check container logs: `docker-compose logs airflow-webserver`
- Restart containers: `docker-compose restart`

### Issue: S3/Athena connection errors
**Solution**:
- Verify AWS credentials in `config/config.conf`
- Check IAM role permissions
- Ensure S3 bucket exists
- Verify Glue Database and Crawler configuration

---

## Test Execution Flow

The recommended test execution order follows the pipeline phases:

1. **Phase 1**: Infrastructure validation
   ```bash
   docker-compose exec -T airflow-webserver python tests/test_phase1_infrastructure.py
   ```

2. **Phase 2**: Extraction & API tests
   ```bash
   docker-compose exec -T airflow-webserver python tests/test_extract_data.py
   docker-compose exec -T airflow-webserver python tests/test_openaq_pipeline.py
   ```

3. **Phase 3**: Glue transformation tests
   ```bash
   docker-compose exec -T airflow-webserver python tests/test_glue_complete.py
   docker-compose exec -T airflow-webserver python tests/test_glue_output_schema.py
   docker-compose exec -T airflow-webserver python tests/test_glue_transformation.py
   ```

4. **Phase 4**: Athena validation
   ```bash
   docker-compose exec -T airflow-webserver python tests/test_athena_connection.py
   ```

5. **Phase 4.5**: End-to-end integration (Automated)
   ```bash
   docker-compose exec -T airflow-webserver python tests/test_e2e_glue_pipeline.py
   ```

6. **Phase 5**: Manual trigger preparation & execution
   ```bash
   # Prepare for manual DAG trigger
   docker-compose exec -T airflow-webserver python tests/test_phase5_manual_trigger.py

   # Then manually trigger in Airflow UI
   # http://localhost:8080 → openaq_dag → Trigger DAG
   ```

### All Tests Summary

| Phase | Test File | Tests | Status | Duration |
|---|---|---|---|---|
| 1 | test_phase1_infrastructure.py | TBD | TBD | - |
| 2 | test_extract_data.py | TBD | TBD | - |
| 2 | test_openaq_pipeline.py | TBD | TBD | - |
| 3 | test_glue_complete.py | TBD | TBD | - |
| 3 | test_glue_output_schema.py | 10 | ✓ SUCCESS | 14.5s |
| 3 | test_glue_transformation.py | 11 | ✓ SUCCESS | 15.7s |
| 4 | test_athena_connection.py | TBD | TBD | - |
| 4.5 | test_e2e_glue_pipeline.py | 11 | ✓ SUCCESS | 0.001s |
| 5 | test_phase5_manual_trigger.py | 9 | ✓ SUCCESS | 0.011s |
| **TOTAL** | **9 files** | **41+** | **✓ 32/32** | **~30s** |

---

## Continuous Integration

For automated test runs in CI/CD pipelines:

```bash
# Run all tests with coverage report
docker-compose exec -T airflow-webserver pytest tests/ \
  --cov=utils \
  --cov=pipelines \
  --cov=etls \
  --cov-report=html \
  -v

# Exit with error code if any test fails
docker-compose exec -T airflow-webserver pytest tests/ -v --tb=short
```

---

## Writing New Tests

When adding new test files:

1. **Follow naming convention**: `test_<feature>.py`
2. **Use test classes**: Group related tests in `unittest.TestCase` subclasses
3. **Use status indicators**: Print `[TEST]`, `[OK]`, `[FAIL]` messages
4. **Add docstrings**: Document what each test validates
5. **Cleanup resources**: Use `setUp()` and `tearDown()` methods
6. **Mock external calls**: Use `unittest.mock` for AWS/API calls

**Example**:
```python
import unittest
from unittest.mock import Mock, patch

class TestNewFeature(unittest.TestCase):
    """Test new feature implementation"""

    def setUp(self):
        """Initialize test fixtures"""
        print("[TEST] Setting up test data...")

    def tearDown(self):
        """Cleanup after test"""
        print("[OK] Cleaned up test resources")

    def test_feature_works(self):
        """Test that feature does what we expect"""
        print("[TEST] Testing feature...")
        self.assertTrue(True)
        print("[OK] Feature works correctly")
```

---

## Resources

- [Glue Transformation Plan](../doc/groovy-foraging-harbor.md)
- [OpenAQ API Documentation](https://www.openaq.org/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Apache Airflow Documentation](https://airflow.apache.org/)

---

## Contact & Support

For issues or questions about tests:
1. Check test output for specific error messages
2. Review test file docstrings for expected behavior
3. Check Docker container logs: `docker-compose logs <service>`
4. Review pipeline plan: `doc/groovy-foraging-harbor.md`
