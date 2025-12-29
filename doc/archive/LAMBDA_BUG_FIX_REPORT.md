# Lambda Parameter Filtering Bug - Investigation & Fix Report

**Date**: 2025-12-29
**Status**: [OK] FIXED
**Impact**: Critical - Prevented HCMC data extraction

---

## Executive Summary

A critical bug in the Lambda function's parameter filtering logic prevented extraction of HCMC (Ho Chi Minh City) air quality data. The issue: parameter name matching failed to account for decimal point differences between required parameters (`PM2.5`) and API responses (`pm25`).

**Result**: While HCMC location with active PM2.5 sensor data exists and was returned by the OpenAQ API, it was silently filtered out before measurement extraction could occur.

---

## Problem Statement

### User Question
> "Check Lambda execution logs to see if it attempted to extract HCMC?"

### Investigation Path
1. Discovered HCMC location (ID 3276359, CMT8) exists in Vietnam
2. Confirmed HCMC has active PM2.5 sensor data (ID 11357424)
3. Verified location passes activity filter (updated 2025-12-29 14:00:00 UTC)
4. Found: HCMC not in extracted S3 raw data (only 2,519 Hanoi records)
5. Traced root cause to parameter matching logic

---

## Root Cause Analysis

### Code Location
**File**: `lambda_functions/openaq_fetcher/deployment/extract_api.py` (line 169)
**Function**: `filter_active_sensors()`

### Original Buggy Logic
```python
if any(req.lower() in param_name.lower() for req in required_parameters):
```

### The Problem
This substring matching approach fails when parameter names have different formatting:

**Example**:
```
Required parameter:  'PM2.5' → lowercased: 'pm2.5'
API returns:         'pm25'
Substring check:     'pm2.5' in 'pm25' → FALSE ❌
```

### Parameter Matching Analysis

| API Parameter | Normalized | Required | Normalized | Match | Status |
|---|---|---|---|---|---|
| `pm1` | `pm1` | N/A | - | - | ❌ FILTERED |
| `pm25` | `pm25` | `PM2.5` | `pm25` | **SHOULD MATCH** | ❌ FILTERED (BUG) |
| `pm10` | `pm10` | `PM10` | `pm10` | ✓ Matches | ✓ INCLUDED |
| `no2` | `no2` | `NO2` | `no2` | ✓ Matches | ✓ INCLUDED |
| `o3` | `o3` | `O3` | `o3` | ✓ Matches | ✓ INCLUDED |
| `co` | `co` | `CO` | `co` | ✓ Matches | ✓ INCLUDED |
| `so2` | `so2` | `SO2` | `so2` | ✓ Matches | ✓ INCLUDED |

### Why Only Hanoi Succeeded
Hanoi locations have parameters like `no2`, `co`, `o3`, `so2` which match due to coincidental substring matching:
- `'no2' in 'no2'` ✓
- `'co' in 'co'` ✓
- `'o3' in 'o3'` ✓

But `'pm2.5' in 'pm25'` fails because the decimal point is missing in the API response.

---

## Evidence of HCMC Data Availability

### HCMC Location Details
```json
{
  "id": 3276359,
  "name": "CMT8",
  "country": { "code": "VN", "name": "Vietnam" },
  "timezone": "Asia/Ho_Chi_Minh",
  "coordinates": { "latitude": 10.78533, "longitude": 106.67029 },
  "datetimeLast": { "utc": "2025-12-29T14:00:00Z" }
}
```

**Status**: ✓ Active (last update 2025-12-29 14:00:00 UTC, within 7-day window)

### PM2.5 Sensor Available
```json
{
  "id": 11357424,
  "name": "pm25 µg/m³",
  "parameter": { "id": 2, "name": "pm25", "units": "µg/m³" }
}
```

### Sample PM2.5 Measurements (2025-12-27 to 2025-12-29)
- 2025-12-27T00:00:00Z: 48.07 µg/m³
- 2025-12-27T01:00:00Z: 48.89 µg/m³
- 2025-12-27T02:00:00Z: 51.45 µg/m³
- 2025-12-27T03:00:00Z: 64.62 µg/m³
- 2025-12-27T04:00:00Z: 92.45 µg/m³
- ... (hourly measurements, all 72 hours available)

**Data Coverage**: 100% complete with hourly intervals, >10 measurements available

---

## Impact Assessment

### Current Data Extraction
**S3 Raw File**: `aq_raw/2025/12/29/15/raw_vietnam_national_20251229T151018.json`

| Location | ID | Records | Status |
|---|---|---|---|
| Hanoi (556 Nguyễn Văn Cừ) | 4946811 | 538 | ✓ Extracted |
| Hanoi (Nguyễn Hữu Cảnh) | 4946812 | 1,126 | ✓ Extracted |
| Hanoi (Lê Hồng Phong) | 4946813 | 855 | ✓ Extracted |
| HCMC (CMT8) | 3276359 | 0 | ❌ Filtered Out |
| **TOTAL** | - | **2,519** | **Incomplete** |

### Missing Data
- **Records Not Extracted**: ~72 hourly PM2.5 measurements from HCMC
- **Geographic Coverage**: 0% of HCMC, only 100% of Hanoi
- **Parameter Coverage**: Missing HCMC PM2.5 (only Hanoi has measured data)

---

## Solution Implemented

### Fix Strategy
Normalize both API parameter names and required parameters by removing decimal points before comparison.

### Fixed Code
**File**: `lambda_functions/openaq_fetcher/extract_api.py` (lines 167-179)

```python
if param_name:
    # Normalize parameter name by removing decimal points for matching
    # API returns 'pm25' but required_parameters has 'PM2.5'
    param_normalized = param_name.lower().replace('.', '')

    # Check if this parameter matches any required parameter
    for req in required_parameters:
        req_normalized = req.lower().replace('.', '')
        if req_normalized == param_normalized or req_normalized in param_normalized:
            sensor_id = sensor.get('id')
            if sensor_id and sensor_id not in active_sensor_ids:
                active_sensor_ids.append(sensor_id)
            break
```

### Why This Works
1. **Decimal Normalization**: `'PM2.5'.lower().replace('.', '')` → `'pm25'`
2. **API Parameter**: `'pm25'.lower().replace('.', '')` → `'pm25'`
3. **Comparison**: `'pm25' == 'pm25'` → **TRUE** ✓

### Testing Results
```
[VALIDATION] Fixed Parameter Matching Logic

API param 'pm1' (normalized: 'pm1')
  Status: FILTERED OUT

API param 'pm25' (normalized: 'pm25')
  Status: INCLUDED matches ['PM2.5'] ✓

API param 'pm10' (normalized: 'pm10')
  Status: INCLUDED matches ['PM10'] ✓

API param 'no2' (normalized: 'no2')
  Status: INCLUDED matches ['NO2'] ✓

API param 'co' (normalized: 'co')
  Status: INCLUDED matches ['CO'] ✓

API param 'so2' (normalized: 'so2')
  Status: INCLUDED matches ['SO2'] ✓

API param 'o3' (normalized: 'o3')
  Status: INCLUDED matches ['O3'] ✓
```

---

## Files Modified

### 1. Source File
**Path**: `lambda_functions/openaq_fetcher/extract_api.py`
**Lines**: 167-179
**Change**: Updated `filter_active_sensors()` function's parameter matching logic

### 2. Deployment Copy
**Path**: `lambda_functions/openaq_fetcher/deployment/extract_api.py`
**Lines**: 167-179
**Change**: Applied identical fix to deployment version

---

## Next Steps

### 1. Redeploy Lambda Function
The Lambda deployment package needs to be rebuilt and redeployed to AWS:

```bash
cd lambda_functions/openaq_fetcher/
# Copy fixed extract_api.py to deployment folder
cp extract_api.py deployment/

# Create deployment package
cd deployment/
zip -r ../openaq_fetcher.zip .

# Deploy to AWS (if using AWS CLI)
# aws lambda update-function-code --function-name openaq-fetcher --zip-file fileb://../openaq_fetcher.zip
```

### 2. Rerun DAG Trigger
Once Lambda is redeployed, trigger the Airflow DAG again:

```bash
# In Airflow UI:
# 1. Click "openaq_to_athena_pipeline"
# 2. Click "Trigger DAG" button
# 3. Monitor execution
```

### 3. Verify HCMC Data Extraction

Check that HCMC data appears in new raw S3 file:
```bash
# List latest raw files
aws s3 ls s3://openaq-data-pipeline/aq_raw/2025/12/29/ --recursive | tail -10

# Check HCMC location ID in data
aws s3 cp s3://openaq-data-pipeline/aq_raw/2025/12/29/16/raw_vietnam_national_*.json - | grep 3276359 | head -5
```

### 4. Validate Athena Results
```sql
-- Check HCMC records in Athena
SELECT
    location_id,
    location_name,
    COUNT(*) as record_count
FROM aq_dev.vn
WHERE location_id = 3276359
GROUP BY location_id, location_name;

-- Expected: ~72 records for PM2.5 from HCMC

-- Check parameter distribution including HCMC
SELECT parameter, COUNT(*) as count
FROM aq_dev.vn
WHERE location_id = 3276359
GROUP BY parameter;
```

---

## Technical Details

### API Parameter Name Formats
OpenAQ API V3 returns parameter names in lowercase without decimal notation:
- `pm25` (not `pm2.5`)
- `pm10` (not `pm1.0`)
- `no2` (not `n02`)
- `so2` (not `s02`)

### Lambda Handler Configuration
The Lambda function receives required parameters from Airflow event:
```json
{
  "file_name": "vietnam_national_20251229T151018",
  "vietnam_wide": true,
  "lookback_hours": 24,
  "required_parameters": ["PM2.5", "PM10", "NO2", "O3", "SO2", "CO", "BC"]
}
```

These are case-sensitive in the handler but need case-insensitive matching with API responses.

---

## Related Documentation

- **CLAUDE.md**: Project architecture and implementation patterns
- **OpenAQ API Docs**: https://api.openaq.org/v3/
- **S3 Data Path**: `s3://openaq-data-pipeline/aq_raw/YYYY/MM/DD/HH/`
- **Athena Databases**: `aq_dev` (development), `aq_prod` (production)

---

## Sign-Off

| Item | Status |
|---|---|
| Root cause identified | ✓ [OK] |
| Bug reproduced | ✓ [OK] |
| Fix implemented | ✓ [OK] |
| Fix validated | ✓ [OK] |
| Files updated | ✓ [OK] |
| Ready for redeployment | ✓ [OK] |

**Note**: Lambda function requires redeployment to AWS before changes take effect. Current local edits are in source control but not yet active in AWS Lambda runtime.
