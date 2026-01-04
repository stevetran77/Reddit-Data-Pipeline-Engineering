# OpenAQ Data Pipeline Engineering

An end-to-end data engineering pipeline that extracts air quality data from the **OpenAQ API v3**, processes it with AWS services, and makes it queryable via Amazon Athena. The system uses Apache Airflow for orchestration, AWS Lambda for extraction, AWS S3 for storage, AWS Glue for transformation, and Amazon Athena for querying.

**Status**: [OK] Production-ready with recent bug fixes and enhancements

---

## Architecture Overview

### Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    Airflow (Docker Local)                       │
│                     openaq_to_athena_pipeline                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │   AWS Lambda: openaq-fetcher  │
              │  (Python 3.11, Serverless)    │
              │  - Fetch Vietnam locations    │
              │  - Extract measurements       │
              │  - Enrich with metadata       │
              └────────────┬───────────────────┘
                           │
                           ▼
              ┌──────────────────────────────┐
              │  Amazon S3 Data Lake          │
              │  ├── aq_raw/                  │
              │  │   └── NDJSON (raw data)    │
              │  └── aq_dev/                  │
              │      └── Parquet (processed)  │
              └────────────┬───────────────────┘
                           │
                           ▼
              ┌──────────────────────────────┐
              │      AWS Glue                 │
              │  ├── Glue Jobs (PySpark)      │
              │  │   - Transform & deduplicate│
              │  └── Glue Crawlers            │
              │      - Update Data Catalog    │
              └────────────┬───────────────────┘
                           │
                           ▼
              ┌──────────────────────────────┐
              │   AWS Athena (SQL Engine)     │
              │   ├── aq_dev.vietnam          │
              │   └── aq_prod.vietnam         │
              └──────────────────────────────┘
```

### S3 Data Lake Structure

```
s3://openaq-data-pipeline/
├── config/                          # Configuration files
│   └── config.conf
├── aq_raw/                          # Raw Zone (immutable)
│   └── YYYY/MM/DD/HH/
│       └── raw_vietnam_*.json       # NDJSON from Lambda
└── aq_dev/                          # Dev Zone (processed)
    └── YYYY/MM/DD/
        └── *.parquet                # Glue transformation output
```

---

## Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Language** | Python | 3.11 |
| **Orchestration** | Apache Airflow | 2.7.1 (Docker) |
| **Extraction** | AWS Lambda | Python 3.11 |
| **Storage** | Amazon S3 | - |
| **Transformation** | AWS Glue + PySpark | 3.4.1 |
| **Query Engine** | Amazon Athena | - |
| **Catalog** | AWS Glue Data Catalog | - |
| **Infrastructure** | Docker & Docker Compose | - |

**Key Dependencies**:
```
apache-airflow==2.7.1
boto3>=1.26.0
awswrangler>=3.0.0
pandas>=2.0.0
pyarrow>=10.0.0
pyspark==3.4.1
openaq>=0.2.0
```

---

## Prerequisites

1. **Docker Desktop** (4GB+ RAM)
2. **AWS Account** with:
   - S3 bucket created
   - IAM roles for Lambda, Glue, Athena
   - OpenAQ API key from [openaq.org](https://openaq.org/)
3. **Git** for cloning repository
4. **AWS CLI** configured with credentials

---

## Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/stevetran77/OpenAQ-Data-Pipeline-Engineering.git
cd OpenAQ-Data-Pipeline-Engineering
```

### 2. Create Configuration

```bash
# Copy template
cp config/config.conf.example config/config.conf

# Edit with your values
# - AWS credentials and S3 bucket name
# - OpenAQ API key
# - Glue database and crawler names
```

### 3. Deploy AWS Infrastructure

```bash
# Create S3 bucket
aws s3 mb s3://openaq-data-pipeline --region ap-southeast-1

# Upload config to S3
aws s3 cp config/config.conf s3://openaq-data-pipeline/config/config.conf --region ap-southeast-1
```

### 4. Deploy Lambda Function

```bash
cd lambda_functions/openaq_fetcher

# Run deployment script (PowerShell on Windows, bash on Linux/Mac)
powershell -ExecutionPolicy Bypass -Command "Set-Location '.'; & '.\deploy.ps1'"

# Verify deployment
aws lambda get-function --function-name openaq-fetcher --region ap-southeast-1 | grep LastModified
```

See [Lambda README](lambda_functions/openaq_fetcher/README.md) for detailed deployment guide.

### 5. Start Airflow

```bash
docker-compose build
docker-compose up -d

# Airflow UI: http://localhost:8080
# Username/Password: admin/admin
```

### 6. Trigger Pipeline

1. Access Airflow UI at http://localhost:8080
2. Find DAG: `openaq_to_athena_pipeline`
3. Click **Trigger DAG** button
4. Monitor execution in Graph view

---

## Pipeline Execution

### DAG Tasks

The `openaq_to_athena_pipeline` DAG performs:

1. **lambda_extract_task**: Invokes Lambda to fetch and upload raw data
2. **trigger_glue_transform_task**: Starts Glue job for transformation
3. **wait_glue_transform_task**: Polls Glue job status (timeout: 30 min)
4. **trigger_crawler_task**: Starts Glue Crawler to update catalog
5. **wait_crawler_task**: Polls Crawler status (timeout: 30 min)
6. **validate_athena_data**: Verifies data is queryable in Athena

### Expected Duration

- Lambda extraction: 2-5 minutes
- Glue transformation: 3-5 minutes
- Glue Crawler: 2-5 minutes
- Total: 10-15 minutes

---

## Recent Changes & Bug Fixes

### [FIXED] Parameter Filtering Bug (December 2025)

**Issue**: HCMC location (ID 3276359) wasn't being extracted despite having active PM2.5 sensor.

**Root Cause**: Parameter name mismatch
```
Required:  'PM2.5' → normalized: 'pm25'
API:       'pm25'  → normalized: 'pm25'
Old logic: substring match failed
```

**Status**: [OK] Fixed in both extract_api.py files

**Impact**:
- HCMC data now extracted with ~72 PM2.5 measurements
- Expanded coverage from 3 (Hanoi) to 4 (Hanoi + HCMC) locations

See [LAMBDA_BUG_FIX_REPORT.md](LAMBDA_BUG_FIX_REPORT.md) for detailed analysis.

### [FIXED] City Name Mapping (December 2025)

**Issue**: HCMC location appeared as `city_name = "Unknown"` in Athena.

**Root Cause**: OpenAQ API returns `locality: null` for HCMC

**Status**: [OK] Fixed with location ID mapping

**Solution**:
```python
LOCATION_CITY_MAP = {
    3276359: "Ho Chi Minh",  # CMT8 - HCMC
}
```

**Expected Result**: HCMC now shows `city_name = "Ho Chi Minh"` in Athena

---

## Project Structure

```
.
├── config/                      # Configuration (gitignored)
│   ├── config.conf.example     # Template
│   └── config.conf             # Your credentials
│
├── lambda_functions/            # AWS Lambda functions
│   └── openaq_fetcher/
│       ├── handler.py           # Entry point
│       ├── extract_api.py       # OpenAQ API extraction
│       ├── s3_uploader.py       # S3 upload logic
│       ├── deploy.ps1           # Deployment script
│       └── README.md            # Lambda documentation
│
├── dags/                        # Airflow DAG definitions
│   ├── openaq_dag.py           # Main pipeline DAG
│   └── tasks/
│       ├── lambda_extract_tasks.py
│       ├── glue_tasks.py
│       └── athena_tasks.py
│
├── glue_jobs/                   # PySpark transformation scripts
│   └── process_openaq_raw.py
│
├── pipelines/                   # High-level orchestration
├── etls/                        # ETL utilities
├── utils/                       # Shared utilities (AWS, Constants)
├── tests/                       # Unit and integration tests
│
├── docker-compose.yml           # Container orchestration
├── Dockerfile                   # Custom Airflow image
├── requirements.txt             # Python dependencies
├── CLAUDE.md                    # Development guidelines
└── README.md                    # This file
```

---

## Testing

### Run All Tests

```bash
# Inside Docker
docker-compose exec -T airflow-webserver pytest tests/ -v

# Locally
pytest tests/ -v
```

### Test Specific Components

```bash
# Lambda extraction
docker-compose exec -T airflow-webserver pytest tests/test_extract_data.py -v

# Glue transformation
docker-compose exec -T airflow-webserver pytest tests/test_glue_transformation.py -v

# Athena connectivity
docker-compose exec -T airflow-webserver pytest tests/test_athena_connection.py -v

# End-to-end integration
docker-compose exec -T airflow-webserver pytest tests/test_e2e_glue_pipeline.py -v
```

---

## Common Operations

### Deploy/Update Lambda

```bash
cd lambda_functions/openaq_fetcher

# Deploy with PowerShell
powershell -ExecutionPolicy Bypass -Command "Set-Location '.'; & '.\deploy.ps1'"

# Verify
aws lambda get-function --function-name openaq-fetcher --region ap-southeast-1
```

### View Lambda Logs

```bash
# Real-time logs
aws logs tail /aws/lambda/openaq-fetcher --follow --region ap-southeast-1

# Last 50 lines
aws logs tail /aws/lambda/openaq-fetcher --max-items 50 --region ap-southeast-1
```

### Check Extracted Data

```bash
# List raw files
aws s3 ls s3://openaq-data-pipeline/aq_raw/2025/12/29/ --recursive

# Check HCMC data (location 3276359)
aws s3 cp s3://openaq-data-pipeline/aq_raw/2025/12/29/15/raw_vietnam_national_*.json - \
  | grep 3276359 | wc -l
# Expected: >70 records
```

### Query Athena

```sql
-- Check databases
SHOW DATABASES;

-- Check tables
SHOW TABLES IN aq_dev;

-- Sample data
SELECT location_id, location_name, city_name, COUNT(*) as count
FROM aq_dev.vietnam
GROUP BY location_id, location_name, city_name
ORDER BY count DESC;

-- Verify HCMC
SELECT DISTINCT city_name FROM aq_dev.vietnam WHERE location_id = 3276359;
-- Expected: Ho Chi Minh
```

---

## Troubleshooting

### Lambda Deployment Fails

**Error**: `command not found` when running `.\deploy.ps1`

**Solution**: Use PowerShell, not Git Bash
```bash
# WRONG
bash $ .\deploy.ps1  # Git Bash - FAILS

# CORRECT
powershell -ExecutionPolicy Bypass -Command "Set-Location '.'; & '.\deploy.ps1'"
```

### No Data in Athena (COUNT = 0)

**Steps to debug**:
1. Check Lambda execution logs: `aws logs tail /aws/lambda/openaq-fetcher --follow`
2. Look for `[OK] Uploaded` message confirming S3 upload
3. Verify S3 raw data exists: `aws s3 ls s3://openaq-data-pipeline/aq_raw/2025/12/29/`
4. Check Glue job logs in AWS Console
5. Verify Glue Crawler ran and created tables

### DAG Timeout

**Issue**: Glue Crawler or Job takes >30 minutes

**Solution**:
1. Check AWS Glue Console for stuck jobs/crawlers
2. Manually stop them if necessary
3. Verify data size in S3 isn't too large
4. Increase timeout in DAG definition if needed

### HCMC Shows "Unknown" City

**Issue**: `city_name = "Unknown"` in Athena for HCMC (location 3276359)

**Solution**:
1. Verify Lambda city mapping is deployed: `grep -n "LOCATION_CITY_MAP" lambda_functions/openaq_fetcher/extract_api.py`
2. Re-deploy Lambda: `powershell -ExecutionPolicy Bypass -Command "Set-Location '.'; & '.\deploy.ps1'"`
3. Re-trigger DAG to extract fresh data
4. Verify in Athena: `SELECT DISTINCT city_name FROM aq_dev.vietnam WHERE location_id = 3276359;`

---
## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository 
2. Create a new branch (`git checkout -b feature/your-feature-name`)
3. Make your changes and commit (`git commit -m 'Add some feature'`)
4. Push to the branch (`git push origin feature/your-feature-name`)
5. Open a Pull Request
---

## License

MIT License - See [LICENSE](LICENSE) file

---

## Support

For issues or questions:
1. Check [Troubleshooting](#troubleshooting) section
2. Review logs: CloudWatch for AWS services, Airflow UI for DAG execution
3. See [Bug Fix Report](LAMBDA_BUG_FIX_REPORT.md) for known issues
4. Check [CLAUDE.md](CLAUDE.md) for development patterns

---

**Last Updated**: December 29, 2025
**Maintained By**: Steve Tran
