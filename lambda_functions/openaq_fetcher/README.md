# OpenAQ Lambda Fetcher - Deployment Guide

AWS Lambda function for serverless extraction of OpenAQ air quality data.

## Overview

This Lambda function replaces the Airflow extraction tasks with a serverless alternative:
- **Function Name**: `openaq-fetcher`
- **Runtime**: Python 3.11
- **Timeout**: 300 seconds (5 minutes)
- **Memory**: 1024 MB
- **Handler**: `handler.lambda_handler`
- **Package Size**: ~8 MB (no pandas dependency)

## Architecture

```
Airflow DAG
    ↓
LambdaInvokeFunctionOperator (with event payload)
    ↓
Lambda Function (openaq-fetcher)
    ├─ fetch_all_vietnam_locations()
    ├─ filter_active_sensors()
    ├─ extract_measurements()
    ├─ transform_measurements()
    ├─ enrich_measurements_with_metadata()
    └─ upload_measurements_to_s3() → NDJSON format
```

## Deployment Methods

### Method 1: PowerShell Script (Windows)

**Prerequisites**:
- PowerShell 7+ (or Windows PowerShell 5.1)
- AWS CLI v2 installed
- AWS credentials configured (`aws configure`)
- 7-Zip or PowerShell's `Compress-Archive`

**Steps**:

```powershell
# 1. Navigate to project root
cd C:\Users\cau.tran\OpenAQ-Data-Pipeline-Engineering

# 2. Build deployment package
cd lambda_functions\openaq_fetcher

# Create deployment directory
if (Test-Path deployment) { Remove-Item -Recurse -Force deployment }
mkdir deployment

# Install dependencies to deployment folder
pip install -r requirements.txt -t deployment --upgrade

# Copy Lambda code to deployment folder
Copy-Item handler.py deployment\
Copy-Item extract_api.py deployment\
Copy-Item s3_uploader.py deployment\

# Create ZIP package
cd deployment
Compress-Archive -Path * -DestinationPath ../openaq-fetcher.zip -Force
cd ..

# 3. Deploy to AWS Lambda
$FunctionName = "openaq-fetcher"
$ZipPath = "openaq-fetcher.zip"

# Check if function exists
$FunctionExists = aws lambda get-function --function-name $FunctionName --region ap-southeast-1 --query 'Configuration.FunctionName' --output text 2>$null

if ($null -eq $FunctionExists -or $FunctionExists -eq "") {
    # Create function
    Write-Host "[INFO] Creating Lambda function..."
    aws lambda create-function `
        --function-name $FunctionName `
        --runtime python3.11 `
        --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-openaq-role `
        --handler handler.lambda_handler `
        --zip-file fileb://$ZipPath `
        --timeout 300 `
        --memory-size 1024 `
        --region ap-southeast-1 `
        --environment Variables="{OPENAQ_API_KEY=YOUR_API_KEY,AWS_BUCKET_NAME=openaq-data-pipeline,PIPELINE_ENV=dev,AWS_REGION=ap-southeast-1}"
    Write-Host "[OK] Function created successfully"
} else {
    # Update existing function
    Write-Host "[INFO] Updating Lambda function..."
    aws lambda update-function-code `
        --function-name $FunctionName `
        --zip-file fileb://$ZipPath `
        --region ap-southeast-1
    Write-Host "[OK] Function updated successfully"
}

# Update environment variables
aws lambda update-function-configuration `
    --function-name $FunctionName `
    --timeout 300 `
    --memory-size 1024 `
    --environment Variables="{OPENAQ_API_KEY=YOUR_API_KEY,AWS_BUCKET_NAME=openaq-data-pipeline,PIPELINE_ENV=dev,AWS_REGION=ap-southeast-1}" `
    --region ap-southeast-1

Write-Host "[SUCCESS] Deployment complete!"
```

### Method 2: AWS CLI (Linux/Mac)

```bash
# 1. Navigate to project root
cd /path/to/OpenAQ-Data-Pipeline-Engineering
cd lambda_functions/openaq_fetcher

# 2. Build deployment package
rm -rf deployment && mkdir deployment

pip install -r requirements.txt -t deployment --upgrade --no-user

cp handler.py extract_api.py s3_uploader.py deployment/

cd deployment
zip -r ../openaq-fetcher.zip . -x "*.pyc" "__pycache__/*"
cd ..

# 3. Deploy to AWS
FUNCTION_NAME="openaq-fetcher"
ZIP_PATH="openaq-fetcher.zip"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/lambda-openaq-role"

# Create or update function
if aws lambda get-function --function-name $FUNCTION_NAME --region ap-southeast-1 &>/dev/null; then
    echo "[INFO] Updating Lambda function..."
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://$ZIP_PATH \
        --region ap-southeast-1
else
    echo "[INFO] Creating Lambda function..."
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime python3.11 \
        --role $ROLE_ARN \
        --handler handler.lambda_handler \
        --zip-file fileb://$ZIP_PATH \
        --timeout 300 \
        --memory-size 1024 \
        --region ap-southeast-1
fi

# Update environment variables (only these 3 - AWS_REGION/credentials are reserved)
aws lambda update-function-configuration \
    --function-name $FUNCTION_NAME \
    --timeout 300 \
    --memory-size 1024 \
    --environment Variables="{OPENAQ_API_KEY=YOUR_API_KEY,AWS_BUCKET_NAME=openaq-data-pipeline,PIPELINE_ENV=dev}" \
    --region ap-southeast-1

echo "[SUCCESS] Deployment complete!"
```

### Method 3: AWS Console UI (Manual)

1. **Build Deployment Package**:
   - Open PowerShell/Terminal in `lambda_functions/openaq_fetcher/`
   - Run the build commands from **Method 1** (PowerShell) or **Method 2** (Linux/Mac)
   - This creates `openaq_fetcher.zip` with code + dependencies
   - Verify ZIP contains: handler.py, extract_api.py, s3_uploader.py, boto3/, requests/, dateutil/

2. **Navigate to Lambda**:
   - Go to AWS Console → Lambda → Functions
   - Click "Create function"

3. **Basic Configuration**:
   - Function name: `openaq-fetcher`
   - Runtime: Python 3.11
   - Architecture: x86_64
   - Execution role: Select existing role `lambda-openaq-role` (create if needed)

4. **Upload Code**:
   - In "Code source" section, click "Upload from" → ".zip file"
   - Select the ZIP file you built: `openaq_fetcher.zip`
   - Click "Deploy"
   - Wait for "Upload successful" message

5. **Configure Function**:
   - Go to "Configuration" tab
   - General configuration:
     - Timeout: 300 seconds
     - Memory: 1024 MB
   - Environment variables (only set these 3):
     - `OPENAQ_API_KEY`: (from config/config.conf)
     - `AWS_BUCKET_NAME`: `openaq-data-pipeline`
     - `PIPELINE_ENV`: `dev`

   **⚠️ Important**: DO NOT set `AWS_REGION`, `AWS_ACCESS_KEY_ID`, or `AWS_SECRET_ACCESS_KEY` - they are reserved by AWS Lambda and will cause configuration errors. Lambda automatically provides:
   - **Region**: Extracted from Lambda function ARN (defaults to ap-southeast-1)
   - **Credentials**: Provided by Lambda execution role

6. **Set Permissions**:
   - Go to "Configuration" tab → "Permissions"
   - Click on execution role name
   - Add inline policy for S3:
   ```json
   {
       "Effect": "Allow",
       "Action": ["s3:PutObject"],
       "Resource": "arn:aws:s3:::openaq-data-pipeline/aq_raw/*"
   }
   ```

## Testing Lambda Function

### Test Event 1: Small Extraction
```json
{
    "file_name": "test_small",
    "vietnam_wide": false,
    "lookback_hours": 1,
    "required_parameters": ["PM2.5"]
}
```

### Test Event 2: Full Vietnam
```json
{
    "file_name": "vietnam_national_test",
    "vietnam_wide": true,
    "lookback_hours": 24,
    "required_parameters": ["PM2.5", "PM10"]
}
```

### Test Steps in AWS Console:

1. Go to Lambda → Functions → `openaq-fetcher`
2. Click "Test" tab
3. Create new test event:
   - Event name: `test_full_vietnam`
   - Paste the test event JSON above
   - Click "Create"
4. Click "Test" to invoke
5. Check logs in "Execution result" section

### Expected Output:
```
[START] OpenAQ Lambda Extraction - 2024-01-15T10:30:00.000000
[1/8] Loading environment configuration...
[OK] Config loaded: bucket=openaq-data-pipeline, region=ap-southeast-1, env=dev
[2/8] Validating event payload...
[OK] Event validated
[3/8] Preparing OpenAQ API headers...
[OK] Headers prepared
[4/8] Fetching all Vietnam locations with sensors...
[INFO] Fetching ALL Vietnam locations (countries_id=56)...
[INFO] Page 1: +100 locations (TOTAL: 100)
[OK] Found 53 locations with 89 sensors
[5/8] Filtering active sensors...
[INFO] Filtering sensors: lookback=7 days, required=['PM2.5', 'PM10']
[OK] Found 39 active sensors (from 89 total)
[6/8] Extracting measurements from sensors...
[INFO] Extracting measurements from 39 sensors
[INFO] Period: 2024-01-14T10:30:00 to 2024-01-15T10:30:00
[INFO] Processed 10/39 sensors, 100 records so far
[INFO] Processed 20/39 sensors, 200 records so far
[OK] Extracted 312 measurements
[7/8] Transforming measurements...
[OK] Transformed 312 records
[8/8] Enriching measurements with location metadata...
[OK] Enriched 312 records
[9/8] Uploading to S3...
[INFO] Uploading 312 records to S3...
[INFO] S3 path: s3://openaq-data-pipeline/aq_raw/2024/01/15/10/raw_vietnam_national_test.json
[OK] Upload complete: s3://openaq-data-pipeline/aq_raw/2024/01/15/10/raw_vietnam_national_test.json
[SUCCESS] Extraction complete:
  - Total Locations: 53
  - Total Sensors: 89
  - Active Sensors: 39
  - Records Extracted: 312
  - Raw data: s3://openaq-data-pipeline/aq_raw/2024/01/15/10/raw_vietnam_national_test.json
  - Duration: 45.32 seconds

Response:
{
    "statusCode": 200,
    "body": "{\"status\": \"SUCCESS\", \"location_count\": 53, \"active_sensor_count\": 39, \"record_count\": 312, \"raw_s3_path\": \"s3://openaq-data-pipeline/aq_raw/2024/01/15/10/raw_vietnam_national_test.json\", \"duration_seconds\": 45.32}"
}
```

## CloudWatch Logs Monitoring

View logs in CloudWatch:
1. AWS Console → CloudWatch → Log groups
2. Find `/aws/lambda/openaq-fetcher`
3. Click on latest log stream
4. Search for `[FAIL]` or `[ERROR]` to find issues

## Lambda Limitations & Optimization

### Timeout Limits
- **Current**: 300 seconds (5 minutes)
- **Vietnam-wide extraction**: ~60-120 seconds
- **Buffer**: 180+ seconds for safety

If timeout occurs:
1. Check if API is slow: Compare API response times in logs
2. Increase timeout to 900 seconds (max 15 minutes)
3. Reduce `lookback_hours` in Airflow event
4. Filter to specific parameters instead of all Vietnam

### Memory Optimization
- **Current allocation**: 1024 MB
- **Typical usage**: ~200-300 MB
- **Peak usage**: ~500 MB (Vietnam-wide, 24h lookback)

Rationale:
- No pandas = significant memory savings
- Plain Python dicts/lists for measurements
- Minimal NDJSON conversion overhead

### Package Size Optimization
- **Current size**: ~8 MB (vs 100+ MB with pandas)
- **boto3**: ~15 MB (already in Lambda runtime)
- **requests**: ~200 KB
- **python-dateutil**: ~300 KB

## Integration with Airflow

### Airflow Task Definition

Update `dags/openaq_dag.py`:

```python
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
import json
from datetime import datetime

extract_vietnam_lambda = LambdaInvokeFunctionOperator(
    task_id='extract_vietnam_lambda',
    function_name='openaq-fetcher',
    payload=json.dumps({
        'file_name': 'vietnam_national_{{ ts_nodash }}',
        'vietnam_wide': True,
        'lookback_hours': 24,
        'required_parameters': ['PM2.5', 'PM10']
    }),
    aws_conn_id='aws_default',
    region_name='ap-southeast-1'
)

# Update DAG task dependencies
extract_vietnam_lambda >> trigger_glue_crawler >> ...
```

### Extracting S3 Path from Lambda Response

To capture the S3 path returned by Lambda:

```python
def get_lambda_result(context, **kwargs):
    """Parse Lambda response and push S3 path to XCom"""
    ti = context['task_instance']

    # Get Lambda response from previous task
    lambda_response = ti.xcom_pull(
        task_ids='extract_vietnam_lambda',
        key='Payload'
    )

    if lambda_response:
        body = json.loads(lambda_response.get('body', '{}'))
        s3_path = body.get('raw_s3_path')
        ti.xcom_push(key='s3_path', value=s3_path)
        print(f"[OK] Lambda S3 path: {s3_path}")

parse_lambda_result = PythonOperator(
    task_id='parse_lambda_result',
    python_callable=get_lambda_result,
    provide_context=True
)
```

## Troubleshooting

### Error: "Missing required environment variable: OPENAQ_API_KEY"
**Solution**: Set environment variables in Lambda configuration:
1. AWS Console → Lambda → openaq-fetcher → Configuration → Environment variables
2. Add `OPENAQ_API_KEY` with value from `config/config.conf`

### Error: "User is not authorized to perform: s3:PutObject"
**Solution**: Add S3 permission to Lambda execution role:
1. AWS IAM → Roles → lambda-openaq-role
2. Add inline policy:
```json
{
    "Effect": "Allow",
    "Action": ["s3:PutObject"],
    "Resource": "arn:aws:s3:::openaq-data-pipeline/aq_raw/*"
}
```

### Error: "Access Denied" from OpenAQ API
**Solution**:
1. Verify API key is valid: `curl -H "X-API-Key: YOUR_KEY" https://api.openaq.org/v3/locations?countries_id=56&limit=10`
2. Check if API key has rate limiting
3. Increase Lambda timeout and retry logic

### Error: "Lambda Timeout after 300 seconds"
**Solution**:
1. Check CloudWatch logs to see where it's slow
2. Increase timeout in Lambda configuration
3. Or reduce `lookback_hours` in Airflow event
4. Or filter to fewer parameters

### Function returns empty records
**Possible causes**:
1. No active sensors with required parameters (expected in off-hours)
2. API down or no data available
3. `lookback_hours` too small (no recent data)

**Debug**: Check CloudWatch logs for step-by-step execution

## Deployment Checklist

- [ ] Lambda function `openaq-fetcher` created in AWS
- [ ] Execution role with S3 write permission created
- [ ] Environment variables set (only these 3):
  - [ ] `OPENAQ_API_KEY` (from config/config.conf)
  - [ ] `AWS_BUCKET_NAME` (openaq-data-pipeline)
  - [ ] `PIPELINE_ENV` (dev)
- [ ] Timeout set to 300 seconds (5 minutes)
- [ ] Memory set to 1024 MB
- [ ] ⚠️ AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY are NOT set (reserved variables)
- [ ] Handler set to `handler.lambda_handler`
- [ ] Lambda tested with test events
- [ ] CloudWatch logs visible
- [ ] Airflow connection `aws_default` configured
- [ ] Airflow DAG updated with `LambdaInvokeFunctionOperator`
- [ ] Glue crawler can read Lambda output files

## Next Steps

1. Deploy Lambda function using one of the methods above
2. Test with Lambda console test events
3. Update Airflow DAG to use `LambdaInvokeFunctionOperator`
4. Verify Glue crawler can read Lambda output
5. Run end-to-end pipeline test
6. Monitor CloudWatch logs for performance insights
7. Adjust timeout/memory based on actual execution metrics

## File Structure

```
lambda_functions/openaq_fetcher/
├── handler.py              # Lambda entry point
├── extract_api.py          # API extraction functions (optimized for Lambda)
├── s3_uploader.py          # S3 upload in NDJSON format
├── requirements.txt        # Minimal dependencies (boto3, requests, python-dateutil)
├── openaq-fetcher.zip      # Deployment package (generated by build script)
├── deployment/             # Build directory (temporary, gitignored)
└── README.md              # This file
```

## Additional Resources

- [AWS Lambda Python Runtime](https://docs.aws.amazon.com/lambda/latest/dg/lambda-python.html)
- [AWS Lambda Limits](https://docs.aws.amazon.com/lambda/latest/dg/limits.html)
- [Boto3 S3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
- [OpenAQ API Documentation](https://api.openaq.org/)
- [Apache Airflow Lambda Operator](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/lambda.html)
