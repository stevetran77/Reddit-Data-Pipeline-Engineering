"""
Lightweight S3 uploader for Lambda function.

Converts measurement dicts directly to NDJSON format (newline-delimited JSON)
for Spark/Glue compatibility. No pandas dependency - direct boto3 usage.

NDJSON Format:
- Each line is a valid JSON object
- Objects are delimited by newlines
- No array wrapper, no commas between objects
- Example:
  {"sensor_id": 123, "datetime": "2024-01-15T10:30:00Z", "value": 45.2}
  {"sensor_id": 124, "datetime": "2024-01-15T10:30:00Z", "value": 38.5}
"""

import json
import boto3
from datetime import datetime
from typing import List, Dict, Tuple


def get_s3_client(aws_access_key_id: str = None, aws_secret_access_key: str = None,
                  aws_region: str = 'ap-southeast-1') -> boto3.client:
    """
    Create S3 client with AWS credentials or Lambda execution role.

    Args:
        aws_access_key_id: AWS access key (optional - None to use Lambda execution role)
        aws_secret_access_key: AWS secret key (optional - None to use Lambda execution role)
        aws_region: AWS region (default: ap-southeast-1 for Vietnam)

    Returns:
        boto3.client: S3 client instance

    Note:
        If credentials are None, boto3 will use the default credential chain:
        1. Lambda execution role (recommended for Lambda)
        2. Environment variables
        3. ~/.aws/credentials (local testing)
    """
    # If credentials are provided, use them. Otherwise, boto3 will use default chain
    if aws_access_key_id and aws_secret_access_key:
        return boto3.client(
            's3',
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
    else:
        # Use default credential chain (Lambda execution role, env vars, ~/.aws/credentials)
        return boto3.client(
            's3',
            region_name=aws_region
        )


def measurements_to_ndjson(measurements: List[Dict]) -> str:
    """
    Convert list of measurement dicts to NDJSON format.

    Args:
        measurements: List of measurement dicts from enrich_measurements_with_metadata()

    Returns:
        str: NDJSON-formatted string (newline-delimited JSON)

    Example:
        >>> measurements = [
        ...     {'sensor_id': 1, 'value': 45.2},
        ...     {'sensor_id': 2, 'value': 38.5}
        ... ]
        >>> ndjson = measurements_to_ndjson(measurements)
        >>> print(ndjson)
        {"sensor_id": 1, "value": 45.2}
        {"sensor_id": 2, "value": 38.5}
    """
    lines = []
    for measurement in measurements:
        # Convert each measurement dict to JSON string
        json_line = json.dumps(measurement, default=str)  # default=str handles datetime objects
        lines.append(json_line)

    # Join with newlines (no trailing newline at end)
    return '\n'.join(lines)


def upload_measurements_to_s3(
    measurements: List[Dict],
    bucket_name: str,
    file_name: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    aws_region: str = 'ap-southeast-1'
) -> Tuple[bool, str]:
    """
    Upload measurements to S3 in NDJSON format.

    Generates S3 path based on current UTC datetime:
    s3://bucket/aq_raw/YYYY/MM/DD/HH/raw_{file_name}.json

    Args:
        measurements: List of enriched measurement dicts
        bucket_name: S3 bucket name (e.g., 'openaq-data-pipeline')
        file_name: Base file name (e.g., 'vietnam_national_20240115_103000')
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        aws_region: AWS region (default: ap-southeast-1)

    Returns:
        Tuple[bool, str]: (success: bool, s3_path: str)
                         - success: True if upload succeeded
                         - s3_path: Full S3 path (s3://bucket/key) or error message

    Raises:
        (No exceptions raised - returns success/error status)
    """
    try:
        if not measurements:
            error_msg = "[FAIL] No measurements to upload"
            print(error_msg)
            return False, error_msg

        # Create S3 client
        s3_client = get_s3_client(aws_access_key_id, aws_secret_access_key, aws_region)

        # Generate S3 path: aq_raw/YYYY/MM/DD/HH/raw_{file_name}.json
        now = datetime.utcnow()
        s3_key = (
            f"aq_raw/{now.year}/{now.strftime('%m')}/{now.strftime('%d')}/"
            f"{now.strftime('%H')}/raw_{file_name}.json"
        )

        # Convert measurements to NDJSON
        ndjson_content = measurements_to_ndjson(measurements)
        ndjson_bytes = ndjson_content.encode('utf-8')

        # Upload to S3
        print(f"[INFO] Uploading {len(measurements)} records to S3...")
        print(f"[INFO] S3 path: s3://{bucket_name}/{s3_key}")

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=ndjson_bytes,
            ContentType='application/x-ndjson'
        )

        s3_path = f"s3://{bucket_name}/{s3_key}"
        print(f"[SUCCESS] Uploaded {len(measurements)} records to {s3_path}")
        return True, s3_path

    except Exception as e:
        error_msg = f"[FAIL] Failed to upload measurements to S3: {str(e)}"
        print(error_msg)
        return False, error_msg


def get_s3_file_size(bucket_name: str, key: str,
                     aws_access_key_id: str,
                     aws_secret_access_key: str,
                     aws_region: str = 'ap-southeast-1') -> int:
    """
    Get file size from S3.

    Args:
        bucket_name: S3 bucket name
        key: S3 object key
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        aws_region: AWS region

    Returns:
        int: File size in bytes, or 0 if file not found/error
    """
    try:
        s3_client = get_s3_client(aws_access_key_id, aws_secret_access_key, aws_region)
        response = s3_client.head_object(Bucket=bucket_name, Key=key)
        return response['ContentLength']
    except Exception as e:
        print(f"[WARNING] Failed to get file size: {str(e)}")
        return 0
