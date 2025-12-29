# OpenAQ Lambda Fetcher - Deployment Guide

AWS Lambda function for serverless extraction of OpenAQ air quality data.

## Overview

This Lambda function replaces the Airflow extraction tasks with a serverless alternative:
- **Function Name**: `openaq-fetcher`
- **Runtime**: Python 3.11
- **Timeout**: 300 seconds (5 minutes)
- **Memory**: 1024 MB
- **Handler**: `handler.lambda_handler`
- **Package Size**: ~17 MB
- **Configuration**: Reads from S3 (`s3://openaq-data-pipeline/config/config.conf`)

## Architecture

```
Airflow DAG
    ↓
LambdaInvokeFunctionOperator (with event payload)
    ↓
Lambda Function (openaq-fetcher)
    ├─ load_config_from_s3() → Read OpenAQ API key from S3
    ├─ fetch_all_vietnam_locations()
    ├─ filter_active_sensors()
    ├─ extract_measurements()
    ├─ transform_measurements()
    ├─ enrich_measurements_with_metadata()
    └─ upload_measurements_to_s3() → NDJSON format
         ↓
    S3: aq_raw/YYYY/MM/DD/HH/raw_*.json
```

## Configuration Strategy

**IMPORTANT**: This Lambda function uses **S3-based configuration** to avoid hardcoding credentials:

1. **Config File Storage**: `s3://openaq-data-pipeline/config/config.conf`
2. **Lambda reads config at runtime** using `load_config_from_s3()` function
3. **No environment variables required** for API keys (only PIPELINE_ENV for logging)
4. **Credentials managed by Lambda execution role** (no AWS keys in environment)

### Advantages of S3 Config Approach:
- ✅ No hardcoded API keys in Lambda environment variables
- ✅ Centralized configuration management
- ✅ Easy to update config without redeploying Lambda
- ✅ Same config file used by Airflow and Lambda
- ✅ Secure credential storage

## Prerequisites

Before deploying, ensure you have:

1. **AWS CLI v2** installed and configured
   ```bash
   aws --version  # Should show aws-cli/2.x.x
   aws configure  # Set up your credentials
   ```

2. **Config file uploaded to S3**:
   ```bash
   aws s3 cp config/config.conf s3://openaq-data-pipeline/config/config.conf
   ```

3. **Lambda execution role** with permissions:
   - S3 read access to `s3://openaq-data-pipeline/config/*`
   - S3 write access to `s3://openaq-data-pipeline/aq_raw/*`
   - CloudWatch Logs write access (for logging)

## Deployment Methods

### Method 1: Automated PowerShell Script (Windows - Recommended)

The `deploy.ps1` script automates the entire deployment process.

**Steps**:

```powershell
# 1. Navigate to Lambda function directory
cd lambda_functions\openaq_fetcher

# 2. Ensure config is uploaded to S3
aws s3 cp ..\..\config\config.conf s3://openaq-data-pipeline/config/config.conf

# 3. Run deployment script from PowerShell (NOT Git Bash)
powershell -ExecutionPolicy Bypass -Command "Set-Location '.'; & '.\deploy.ps1'"

# OR if using PowerShell directly:
.\deploy.ps1
```

**IMPORTANT**: Execute in **PowerShell**, not Git Bash. Git Bash will fail with `command not found`.

**What the script does**:
1. Creates IAM role `lambda-openaq-role` with S3 and CloudWatch permissions (if not exists)
2. Installs Python dependencies to `deployment/` folder
3. Copies Lambda code files (handler.py, extract_api.py, s3_uploader.py)
4. Creates ZIP deployment package
5. Creates or updates Lambda function
6. Sets timeout (300s) and memory (1024 MB)
7. **Note**: Script will set environment variables, but our updated handler doesn't require them

**Expected Output**:
```
[OK] Account ID: 387158739004
[OK] Role exists: arn:aws:iam::387158739004:role/lambda-openaq-role
[INFO] Building deployment package...
[INFO] Installing dependencies...
[INFO] Copying Lambda code...
[INFO] Creating ZIP package...
[OK] Deployment package created: openaq-fetcher.zip
[INFO] Checking if Lambda function exists...
[OK] Function code updated
[OK] Environment variables set
[SUCCESS] Lambda Function Deployed!
================================================
Function Name: openaq-fetcher
Runtime: Python 3.11
Handler: handler.lambda_handler
Memory: 1024 MB
Timeout: 300 seconds
Region: ap-southeast-1
================================================
[OK] Done!
```

**Verify Deployment**:
```powershell
# Check Lambda function last modified timestamp
aws lambda get-function --function-name openaq-fetcher --region ap-southeast-1 | grep -A 1 LastModified

# Expected: Recent timestamp showing deployment was successful
```

### Method 2: Manual Deployment (Linux/Mac/Windows)

**Step 1: Upload Config to S3**
```bash
aws s3 cp ../../config/config.conf s3://openaq-data-pipeline/config/config.conf --region ap-southeast-1
```

**Step 2: Build Deployment Package**
```bash
cd lambda_functions/openaq_fetcher

# Clean previous builds
rm -rf deployment openaq-fetcher.zip

# Create deployment directory
mkdir deployment

# Install dependencies (Windows: use appropriate pip command)
pip install -r requirements.txt -t deployment --upgrade

# Copy Lambda code
cp handler.py extract_api.py s3_uploader.py deployment/

# Create ZIP (Windows: use Python's zipfile or deploy.ps1)
cd deployment
zip -r ../openaq-fetcher.zip . -x "*.pyc" "__pycache__/*"
# OR on Windows with Python:
python -m zipfile -c ../openaq-fetcher.zip *
cd ..
```

**Step 3: Create Lambda Execution Role** (if not exists)

```bash
# Create role trust policy
cat > trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF

# Create role
aws iam create-role \
  --role-name lambda-openaq-role \
  --assume-role-policy-document file://trust-policy.json

# Attach policies
aws iam attach-role-policy \
  --role-name lambda-openaq-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
  --role-name lambda-openaq-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Wait for role propagation
sleep 10
```

**Step 4: Deploy Lambda Function**

```bash
FUNCTION_NAME="openaq-fetcher"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/lambda-openaq-role"

# Create or update function
if aws lambda get-function --function-name $FUNCTION_NAME --region ap-southeast-1 &>/dev/null; then
    echo "[INFO] Updating Lambda function..."
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://openaq-fetcher.zip \
        --region ap-southeast-1
else
    echo "[INFO] Creating Lambda function..."
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime python3.11 \
        --role $ROLE_ARN \
        --handler handler.lambda_handler \
        --zip-file fileb://openaq-fetcher.zip \
        --timeout 300 \
        --memory-size 1024 \
        --region ap-southeast-1
fi

echo "[SUCCESS] Deployment complete!"
```

### Method 3: AWS Console UI (Manual)

1. **Upload Config to S3** (if not done):
   - AWS Console → S3 → `openaq-data-pipeline` bucket
   - Create folder `config/` if not exists
   - Upload `config.conf` file to `config/` folder

2. **Build Deployment Package locally**:
   - Follow **Step 2** from Method 2 to create `openaq-fetcher.zip`

3. **Create Lambda Function**:
   - AWS Console → Lambda → Create function
   - Function name: `openaq-fetcher`
   - Runtime: Python 3.11
   - Execution role: Create new role `lambda-openaq-role` or use existing

4. **Upload Code**:
   - Code source → Upload from → .zip file
   - Select `openaq-fetcher.zip`
   - Click Save

5. **Configure Function**:
   - Configuration → General configuration:
     - Timeout: 300 seconds
     - Memory: 1024 MB
   - Configuration → Environment variables (optional):
     - `PIPELINE_ENV`: `dev` (for logging only)

6. **Set Permissions**:
   - Configuration → Permissions → Execution role
   - Add policies:
     - `AmazonS3FullAccess` (or custom policy with read/write to specific bucket)
     - `CloudWatchLogsFullAccess` (for logging)

## Testing Lambda Function

### Test Event 1: Small Extraction
```json
{
    "file_name": "vietnam_test_small",
    "vietnam_wide": true,
    "lookback_hours": 1,
    "required_parameters": ["PM2.5", "PM10"]
}
```

### Test Event 2: Full Vietnam (24 hours)
```json
{
    "file_name": "vietnam_national_test",
    "vietnam_wide": true,
    "lookback_hours": 24,
    "required_parameters": ["PM2.5", "PM10", "NO2", "O3", "SO2", "CO", "BC"]
}
```

### Test in AWS Console:

1. Lambda → `openaq-fetcher` → Test tab
2. Create new test event with JSON above
3. Click "Test"
4. Check execution result

### Expected Output:
```
[START] OpenAQ Lambda Extraction - 2025-12-28T07:30:00.123456
[INFO] Event: {"file_name": "vietnam_national_test", "vietnam_wide": true, ...}
[1/8] Loading environment configuration...
[OK] Config loaded: bucket=openaq-data-pipeline, region=ap-southeast-1, env=dev
[2/8] Validating event payload...
[OK] Event validated
[3/8] Preparing OpenAQ API headers...
[OK] Headers prepared
[4/8] Fetching all Vietnam locations with sensors...
[OK] Found 53 locations with 89 sensors
[5/8] Filtering active sensors...
[OK] Found 39 active sensors (from 89 total)
[6/8] Extracting measurements from sensors...
[OK] Extracted 312 measurements
[7/8] Transforming measurements...
[OK] Transformed 312 records
[8/8] Enriching measurements with location metadata...
[OK] Enriched 312 records
[9/8] Uploading to S3...
[INFO] S3 path: s3://openaq-data-pipeline/aq_raw/2025/12/28/07/raw_vietnam_national_test.json
[SUCCESS] Extraction complete:
  - Total Locations: 53
  - Active Sensors: 39
  - Records Extracted: 312
  - Raw data: s3://openaq-data-pipeline/aq_raw/2025/12/28/07/raw_vietnam_national_test.json
  - Duration: 45.23 seconds

Response:
{
    "statusCode": 200,
    "body": {
        "status": "SUCCESS",
        "location_count": 53,
        "active_sensor_count": 39,
        "record_count": 312,
        "raw_s3_path": "s3://openaq-data-pipeline/aq_raw/2025/12/28/07/raw_vietnam_national_test.json",
        "duration_seconds": 45.23
    }
}
```

### Verify S3 Upload:
```bash
aws s3 ls s3://openaq-data-pipeline/aq_raw/ --recursive | tail -10
```

Expected:
```
2025-12-28 07:30:45   15234 aq_raw/2025/12/28/07/raw_vietnam_national_test.json
```

## CRITICAL: Parameter Filtering Fix (December 2025)

### Issue Identified
A critical bug was discovered in the parameter filtering logic that prevented extraction of HCMC (Ho Chi Minh City) air quality data.

**Root Cause**: Parameter name mismatch between required parameters and API responses:
- Required parameter: `'PM2.5'` (with decimal)
- API returns: `'pm25'` (without decimal)
- Old logic: `'pm2.5' in 'pm25'` → **FALSE** ❌ (FILTERED OUT)
- Fixed logic: Normalize both by removing decimals → **TRUE** ✓ (INCLUDED)

### What Was Fixed
**File**: `extract_api.py` - `filter_active_sensors()` function (lines 167-179)

**Before** (Buggy):
```python
if any(req.lower() in param_name.lower() for req in required_parameters):
```

**After** (Fixed):
```python
param_normalized = param_name.lower().replace('.', '')
for req in required_parameters:
    req_normalized = req.lower().replace('.', '')
    if req_normalized == param_normalized or req_normalized in param_normalized:
        # Include this sensor
```

### Impact
- **HCMC Location**: ID 3276359 (CMT8) - NOW EXTRACTED ✓
- **HCMC PM2.5 Sensor**: ID 11357424 - NOW EXTRACTED ✓
- **Data Coverage**: Expanded from Hanoi-only (3 locations) to Hanoi + HCMC (4 locations)
- **Records**: Expected +72 hourly PM2.5 measurements from HCMC

### Deployment Note
The `deploy.ps1` script automatically includes this fix. When you deploy, ensure you're deploying the latest version with the fix applied to both:
1. `extract_api.py` (source file)
2. `deployment/extract_api.py` (deployment copy)

**Verify the fix is in place**:
```bash
grep -A 10 "param_normalized = param_name" extract_api.py | head -15
# Should show the normalization logic with .replace('.', '')
```

### Validation After Deployment
After deploying the fixed Lambda, HCMC data should appear in S3:

```bash
# Check for HCMC location ID (3276359) in latest raw file
aws s3 cp s3://openaq-data-pipeline/aq_raw/2025/12/29/XX/raw_vietnam_national_*.json - \
  | grep 3276359 | head -5

# Expected: >70 records from HCMC
```

### Related Documentation
See `LAMBDA_BUG_FIX_REPORT.md` for detailed analysis and investigation.

---

## Integration with Airflow

### Airflow DAG Configuration

The Lambda function is invoked from Airflow using `LambdaInvokeFunctionOperator`:

**File**: `dags/tasks/lambda_extract_tasks.py`

```python
import json
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

def create_lambda_extract_task(dag, function_name: str = 'openaq-fetcher'):
    timestamp = "{{ ts_nodash }}"

    payload = {
        "file_name": f"vietnam_national_{timestamp}",
        "vietnam_wide": True,
        "lookback_hours": 24,
        "required_parameters": ['PM2.5', 'PM10', 'NO2', 'O3', 'SO2', 'CO', 'BC']
    }

    task = LambdaInvokeFunctionOperator(
        task_id='lambda_extract_vietnam',
        function_name=function_name,
        payload=json.dumps(payload),  # Must be JSON string
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        log_type='Tail',
        dag=dag
    )

    return task
```

### Airflow AWS Connection Setup

Ensure Airflow has AWS credentials configured in `airflow.env`:

```bash
AIRFLOW_CONN_AWS_DEFAULT=aws://ACCESS_KEY_ID:SECRET_ACCESS_KEY@?region_name=ap-southeast-1
```

Or configure via Airflow UI:
1. Admin → Connections → Add Connection
2. Connection Id: `aws_default`
3. Connection Type: `Amazon Web Services`
4. AWS Access Key ID: `your-access-key`
5. AWS Secret Access Key: `your-secret-key`
6. Extra: `{"region_name": "ap-southeast-1"}`

## CloudWatch Logs Monitoring

### View Logs:
```bash
# Get latest log stream
aws logs describe-log-streams \
  --log-group-name /aws/lambda/openaq-fetcher \
  --order-by LastEventTime \
  --descending \
  --max-items 1 \
  --region ap-southeast-1

# Tail logs (requires aws-cli v2)
aws logs tail /aws/lambda/openaq-fetcher --follow --region ap-southeast-1
```

### Check for Errors:
```bash
aws logs filter-log-events \
  --log-group-name /aws/lambda/openaq-fetcher \
  --filter-pattern "[FAIL]" \
  --region ap-southeast-1
```

## Troubleshooting

### Error: "Failed to load config from S3"

**Cause**: Config file not found or Lambda role lacks S3 read permission

**Solution**:
1. Verify config file exists:
   ```bash
   aws s3 ls s3://openaq-data-pipeline/config/config.conf
   ```
2. Check Lambda execution role has S3 read permission:
   ```bash
   aws iam list-attached-role-policies --role-name lambda-openaq-role
   ```
3. Ensure `AmazonS3FullAccess` or custom S3 read policy is attached

### Error: "openaq_api_key not found in config file"

**Cause**: Config file doesn't contain `openaq_api_key` field

**Solution**:
1. Check config file contents:
   ```bash
   aws s3 cp s3://openaq-data-pipeline/config/config.conf - | grep openaq_api_key
   ```
2. Ensure config file has this line:
   ```
   [api_keys]
   openaq_api_key = YOUR_API_KEY_HERE
   ```

### Error: "User is not authorized to perform: s3:PutObject"

**Cause**: Lambda role lacks S3 write permission

**Solution**:
```bash
aws iam attach-role-policy \
  --role-name lambda-openaq-role \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

### Error: "Task timeout after 300 seconds"

**Cause**: Lambda function taking too long

**Solutions**:
1. Increase Lambda timeout:
   ```bash
   aws lambda update-function-configuration \
     --function-name openaq-fetcher \
     --timeout 600 \
     --region ap-southeast-1
   ```
2. Or reduce `lookback_hours` in Airflow event
3. Or filter to fewer parameters

### No Data in S3 After Successful Execution

**Possible Causes**:
1. No active sensors found (check CloudWatch logs for active sensor count)
2. S3 path permissions issue
3. Lambda returned success but upload failed

**Debug**:
1. Check CloudWatch logs for step-by-step execution
2. Look for `[OK] Uploaded` message
3. Verify S3 path in logs matches expected location

## Deployment Checklist

### Pre-Deployment
- [ ] Config file uploaded to S3: `s3://openaq-data-pipeline/config/config.conf`
- [ ] Config contains `openaq_api_key` in `[api_keys]` section
- [ ] Parameter filtering fix applied to `extract_api.py` (check for `param_normalized` logic)
- [ ] Lambda execution role `lambda-openaq-role` created
- [ ] Role has `AmazonS3FullAccess` policy (or custom S3 read/write policy)
- [ ] Role has `AWSLambdaBasicExecutionRole` policy (for CloudWatch logging)

### Deployment Steps
- [ ] Navigate to: `cd lambda_functions/openaq_fetcher`
- [ ] Run: `powershell -ExecutionPolicy Bypass -Command "Set-Location '.'; & '.\deploy.ps1'"`
- [ ] Wait for `[SUCCESS] Lambda Function Deployed!` message
- [ ] Verify: `aws lambda get-function --function-name openaq-fetcher --region ap-southeast-1 | grep LastModified`

### Post-Deployment Verification
- [ ] Lambda function configuration:
  - [ ] Runtime: Python 3.11
  - [ ] Handler: `handler.lambda_handler`
  - [ ] Timeout: 300 seconds
  - [ ] Memory: 1024 MB
- [ ] Lambda tested with test event (use Test Event 2: Full Vietnam)
- [ ] CloudWatch logs visible: `/aws/lambda/openaq-fetcher`
- [ ] Logs show successful execution with all `[OK]` messages
- [ ] Data uploaded to S3 `aq_raw/` folder
- [ ] **CRITICAL**: Verify HCMC data (location 3276359) appears in S3 raw data
  ```bash
  aws s3 cp s3://openaq-data-pipeline/aq_raw/2025/12/29/XX/raw_vietnam_national_*.json - | grep 3276359 | wc -l
  # Expected: >70 records
  ```

### Airflow Integration
- [ ] Airflow connection `aws_default` configured with AWS credentials
- [ ] Airflow DAG uses `LambdaInvokeFunctionOperator`
- [ ] DAG trigger test completed successfully
- [ ] All downstream tasks (Glue, Athena) executed without errors
- [ ] Athena queries return HCMC data alongside Hanoi data

## File Structure

```
lambda_functions/openaq_fetcher/
├── handler.py              # Lambda entry point (with load_config_from_s3)
├── extract_api.py          # OpenAQ API extraction functions
├── s3_uploader.py          # S3 NDJSON upload functions
├── requirements.txt        # Python dependencies
├── deploy.ps1              # Automated deployment script (Windows)
├── openaq-fetcher.zip      # Deployment package (generated)
├── deployment/             # Build directory (gitignored)
└── README.md               # This file
```

## Next Steps

1. ✅ Deploy Lambda function using one of the methods above
2. ✅ Upload config to S3
3. ✅ Test Lambda with console test events
4. ✅ Verify data appears in `s3://openaq-data-pipeline/aq_raw/`
5. Update Airflow DAG to use Lambda operator (if not already done)
6. Run end-to-end Airflow pipeline test
7. Monitor CloudWatch logs for performance
8. Adjust timeout/memory based on metrics

## Quick Reference - Common Commands

### Deploy Lambda with Fix
```powershell
# From Git Bash or Command Prompt:
cd lambda_functions\openaq_fetcher

# Deploy (execute in PowerShell)
powershell -ExecutionPolicy Bypass -Command "Set-Location '.'; & '.\deploy.ps1'"
```

### Verify Deployment
```bash
# Check Lambda was updated
aws lambda get-function --function-name openaq-fetcher --region ap-southeast-1 | grep LastModified

# View latest CloudWatch logs
aws logs tail /aws/lambda/openaq-fetcher --follow --region ap-southeast-1
```

### Validate Data Extraction
```bash
# List all raw files
aws s3 ls s3://openaq-data-pipeline/aq_raw/2025/12/29/ --recursive | tail -5

# Check HCMC location data exists (should have >70 records)
aws s3 cp s3://openaq-data-pipeline/aq_raw/2025/12/29/XX/raw_vietnam_national_*.json - | grep 3276359 | wc -l
```

### Test Lambda Manually
```bash
# Invoke with test event
aws lambda invoke \
  --function-name openaq-fetcher \
  --payload '{"file_name":"test_manual","vietnam_wide":true,"lookback_hours":24,"required_parameters":["PM2.5","PM10"]}' \
  --region ap-southeast-1 \
  response.json

# View response
cat response.json
```

### Troubleshoot Parameter Filtering
```bash
# Check if fix is in place
grep -n "param_normalized = param_name" extract_api.py

# Should show line ~170 with the normalization logic
```

## Additional Resources

- [AWS Lambda Python Runtime](https://docs.aws.amazon.com/lambda/latest/dg/lambda-python.html)
- [AWS Lambda Environment Variables](https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html)
- [Boto3 S3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
- [OpenAQ API Documentation](https://api.openaq.org/)
- [Apache Airflow Lambda Operator](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/lambda.html)

---

## Recent Changes (December 2025)

**Parameter Filtering Bug Fix**:
- Fixed critical issue where HCMC location wasn't being extracted
- Updated `filter_active_sensors()` to handle parameter name normalization
- Parameter matching now handles decimal point differences (PM2.5 vs pm25)
- See `LAMBDA_BUG_FIX_REPORT.md` for detailed analysis

**Updated Documentation**:
- Added PowerShell execution instructions to deployment section
- Added post-deployment HCMC verification checklist
- Added quick reference commands for common tasks
- Added troubleshooting steps for parameter filtering
