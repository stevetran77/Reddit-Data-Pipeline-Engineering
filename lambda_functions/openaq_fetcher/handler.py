"""
AWS Lambda handler for OpenAQ data extraction and S3 upload.

Entry point: lambda_handler(event, context)

Event Schema (from Airflow):
{
    "file_name": "vietnam_national_20240115_103000",
    "vietnam_wide": true,
    "lookback_hours": 24,
    "required_parameters": ["PM2.5", "PM10"]
}

Environment Variables:
- OPENAQ_API_KEY: OpenAQ API key (required)
- AWS_BUCKET_NAME: S3 bucket name (required)
- PIPELINE_ENV: Environment (dev/prod) for logging (optional)
- AWS_REGION: AWS region (optional - auto-detected from Lambda context, default: ap-southeast-1)

Response Schema:
{
    "statusCode": 200,
    "body": {
        "status": "SUCCESS",
        "location_count": 53,
        "active_sensor_count": 39,
        "record_count": 120,
        "raw_s3_path": "s3://bucket/aq_raw/2024/01/15/10/raw_vietnam_national_20240115_103000.json"
    }
}

On Error:
{
    "statusCode": 500,
    "body": {
        "status": "FAIL",
        "error": "Error message details"
    }
}
"""

import json
import os
from datetime import datetime, timedelta

from lambda_functions.openaq_fetcher.deployment.extract_api import (
    connect_openaq,
    fetch_all_vietnam_locations,
    filter_active_sensors,
    extract_measurements,
    transform_measurements,
    enrich_measurements_with_metadata
)
from lambda_functions.openaq_fetcher.deployment.s3_uploader import upload_measurements_to_s3


def get_env_var(key: str, default: str = None) -> str:
    """Get environment variable with optional default."""
    value = os.environ.get(key, default)
    if value is None:
        raise ValueError(f"[FAIL] Missing required environment variable: {key}")
    return value


def validate_event(event: dict) -> dict:
    """
    Validate Lambda event payload.

    Args:
        event: Lambda event from Airflow

    Returns:
        dict: Validated and normalized event

    Raises:
        ValueError: If required fields are missing
    """
    required_fields = ['file_name', 'vietnam_wide', 'lookback_hours']

    for field in required_fields:
        if field not in event:
            raise ValueError(f"[FAIL] Missing required field in event: {field}")

    # Set defaults for optional fields
    event.setdefault('required_parameters', ['PM2.5', 'PM10'])

    # Validate types
    if not isinstance(event['file_name'], str):
        raise ValueError("[FAIL] file_name must be a string")
    if not isinstance(event['vietnam_wide'], bool):
        raise ValueError("[FAIL] vietnam_wide must be a boolean")
    if not isinstance(event['lookback_hours'], int) or event['lookback_hours'] <= 0:
        raise ValueError("[FAIL] lookback_hours must be a positive integer")

    return event


def lambda_handler(event, context):
    """
    Main Lambda handler function for OpenAQ data extraction.

    Orchestrates the complete extraction pipeline:
    1. Fetch all Vietnam locations with sensors
    2. Filter active sensors (recent data + required parameters)
    3. Extract measurements from active sensors
    4. Transform measurements to standardized format
    5. Enrich with location metadata
    6. Upload to S3 in NDJSON format

    Args:
        event: Lambda event from Airflow with:
               - file_name: str - Unique filename for raw data
               - vietnam_wide: bool - If True, extract all Vietnam locations
               - lookback_hours: int - Hours of historical data to extract
               - required_parameters: list[str] - Parameter names to include (optional)
        context: Lambda context object

    Returns:
        dict: Response with statusCode and body containing extraction metadata
    """
    start_time = datetime.utcnow()
    print(f"[START] OpenAQ Lambda Extraction - {start_time.isoformat()}")
    print(f"[INFO] Event: {json.dumps(event)}")

    try:
        # ====================================================================
        # Load environment configuration
        # ====================================================================
        print("[1/8] Loading environment configuration...")
        try:
            api_key = get_env_var('OPENAQ_API_KEY')
            bucket_name = get_env_var('AWS_BUCKET_NAME')

            # Get AWS region from Lambda context (invoked_function_arn)
            # ARN format: arn:aws:lambda:REGION:ACCOUNT:function:FUNCTION_NAME
            aws_region = 'ap-southeast-1'  # default
            try:
                arn_parts = context.invoked_function_arn.split(':')
                if len(arn_parts) >= 4:
                    aws_region = arn_parts[3]  # Extract region from ARN
            except Exception:
                pass  # Use default if parsing fails

            pipeline_env = os.environ.get('PIPELINE_ENV', 'dev')
            print(f"[OK] Config loaded: bucket={bucket_name}, region={aws_region}, env={pipeline_env}")
        except ValueError as e:
            return error_response(500, str(e))

        # ====================================================================
        # Validate event
        # ====================================================================
        print("[2/8] Validating event payload...")
        try:
            event = validate_event(event)
            print(f"[OK] Event validated")
        except ValueError as e:
            return error_response(400, str(e))

        # Extract event parameters
        file_name = event['file_name']
        vietnam_wide = event['vietnam_wide']
        lookback_hours = event['lookback_hours']
        required_parameters = event.get('required_parameters', ['PM2.5', 'PM10'])

        print(f"[INFO] Parameters:")
        print(f"       - file_name: {file_name}")
        print(f"       - vietnam_wide: {vietnam_wide}")
        print(f"       - lookback_hours: {lookback_hours}")
        print(f"       - required_parameters: {required_parameters}")

        # ====================================================================
        # STEP 1: Connect to OpenAQ
        # ====================================================================
        print("[3/8] Preparing OpenAQ API headers...")
        try:
            headers = connect_openaq(api_key=api_key)
            print("[OK] Headers prepared")
        except Exception as e:
            return error_response(500, f"[FAIL] Failed to connect OpenAQ: {str(e)}")

        # ====================================================================
        # STEP 2: Fetch all Vietnam locations
        # ====================================================================
        print("[4/8] Fetching all Vietnam locations with sensors...")
        try:
            sensor_ids, location_objs = fetch_all_vietnam_locations(headers)
            location_count = len(location_objs)
            initial_sensor_count = len(sensor_ids)
            print(f"[OK] Found {location_count} locations with {initial_sensor_count} sensors")
        except Exception as e:
            return error_response(500, f"[FAIL] Failed to fetch locations: {str(e)}")

        # ====================================================================
        # STEP 3: Filter active sensors
        # ====================================================================
        print("[5/8] Filtering active sensors...")
        try:
            active_sensor_ids = filter_active_sensors(
                location_objs,
                lookback_days=7,
                required_parameters=required_parameters
            )
            active_sensor_count = len(active_sensor_ids)

            if active_sensor_count == 0:
                warning_msg = "[WARNING] No active sensors found. Lambda stopping."
                print(warning_msg)
                return success_response({
                    'status': 'WARNING',
                    'location_count': location_count,
                    'active_sensor_count': 0,
                    'record_count': 0,
                    'raw_s3_path': None
                })

            print(f"[OK] Found {active_sensor_count} active sensors (from {initial_sensor_count} total)")
        except Exception as e:
            return error_response(500, f"[FAIL] Failed to filter sensors: {str(e)}")

        # ====================================================================
        # STEP 4: Extract measurements
        # ====================================================================
        print("[6/8] Extracting measurements from sensors...")
        try:
            date_to = datetime.utcnow()
            date_from = date_to - timedelta(hours=lookback_hours)

            measurements = extract_measurements(headers, active_sensor_ids, date_from, date_to)

            if len(measurements) == 0:
                warning_msg = "[WARNING] No measurements extracted. Lambda stopping."
                print(warning_msg)
                return success_response({
                    'status': 'WARNING',
                    'location_count': location_count,
                    'active_sensor_count': active_sensor_count,
                    'record_count': 0,
                    'raw_s3_path': None
                })

            print(f"[OK] Extracted {len(measurements)} measurements")
        except Exception as e:
            return error_response(500, f"[FAIL] Failed to extract measurements: {str(e)}")

        # ====================================================================
        # STEP 5: Transform measurements
        # ====================================================================
        print("[7/8] Transforming measurements...")
        try:
            transformed = transform_measurements(measurements)
            print(f"[OK] Transformed {len(transformed)} records")
        except Exception as e:
            return error_response(500, f"[FAIL] Failed to transform measurements: {str(e)}")

        # ====================================================================
        # STEP 6: Enrich with metadata
        # ====================================================================
        print("[8/8] Enriching measurements with location metadata...")
        try:
            enriched = enrich_measurements_with_metadata(transformed, location_objs)
            print(f"[OK] Enriched {len(enriched)} records")
        except Exception as e:
            return error_response(500, f"[FAIL] Failed to enrich measurements: {str(e)}")

        # ====================================================================
        # STEP 7: Upload to S3
        # ====================================================================
        print("[9/8] Uploading to S3...")
        try:
            # Lambda execution role will automatically provide AWS credentials
            # No need to pass AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (they're reserved)
            success, s3_path = upload_measurements_to_s3(
                measurements=enriched,
                bucket_name=bucket_name,
                file_name=file_name,
                aws_access_key_id=None,
                aws_secret_access_key=None,
                aws_region=aws_region
            )

            if not success:
                return error_response(500, s3_path)

            print(f"[OK] Upload complete: {s3_path}")
        except Exception as e:
            return error_response(500, f"[FAIL] Failed to upload to S3: {str(e)}")

        # ====================================================================
        # Success Response
        # ====================================================================
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        result = {
            'status': 'SUCCESS',
            'location_count': location_count,
            'active_sensor_count': active_sensor_count,
            'record_count': len(enriched),
            'raw_s3_path': s3_path,
            'duration_seconds': duration
        }

        print(f"[SUCCESS] Extraction complete:")
        print(f"  - Total Locations: {location_count}")
        print(f"  - Total Sensors: {initial_sensor_count}")
        print(f"  - Active Sensors: {active_sensor_count}")
        print(f"  - Records Extracted: {result['record_count']}")
        print(f"  - Raw data: {result['raw_s3_path']}")
        print(f"  - Duration: {duration:.2f} seconds")

        return success_response(result)

    except Exception as e:
        error_msg = f"[FAIL] Unexpected error: {str(e)}"
        print(error_msg)
        return error_response(500, error_msg)


def success_response(body: dict) -> dict:
    """Format successful Lambda response."""
    return {
        'statusCode': 200,
        'body': json.dumps(body),
        'headers': {
            'Content-Type': 'application/json'
        }
    }


def error_response(status_code: int, error_message: str) -> dict:
    """Format error Lambda response."""
    return {
        'statusCode': status_code,
        'body': json.dumps({
            'status': 'FAIL',
            'error': error_message
        }),
        'headers': {
            'Content-Type': 'application/json'
        }
    }


# For local testing
if __name__ == '__main__':
    # Test event
    test_event = {
        'file_name': 'vietnam_national_test',
        'vietnam_wide': True,
        'lookback_hours': 24,
        'required_parameters': ['PM2.5', 'PM10']
    }

    # Mock context
    class MockContext:
        def __init__(self):
            self.function_name = 'openaq-fetcher'
            self.function_version = '$LATEST'
            self.invoked_function_arn = 'arn:aws:lambda:ap-southeast-1:123456789:function:openaq-fetcher'
            self.memory_limit_in_mb = 1024
            self.aws_request_id = 'test-request-id'
            self.log_group_name = '/aws/lambda/openaq-fetcher'
            self.log_stream_name = 'test-stream'

    context = MockContext()

    # Run handler
    response = lambda_handler(test_event, context)
    print(f"\nLambda Response:\n{json.dumps(response, indent=2)}")
