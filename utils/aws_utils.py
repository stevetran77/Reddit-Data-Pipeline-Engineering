import boto3
import io
import pandas as pd
from datetime import datetime
from utils.constants import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
    AWS_REGION,
    AWS_BUCKET_NAME
)


def get_s3_client():
    """
    Create and return authenticated S3 client.

    Returns:
        boto3.client: S3 client instance
    """
    session_config = {
        'aws_access_key_id': AWS_ACCESS_KEY_ID,
        'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
        'region_name': AWS_REGION
    }

    if AWS_SESSION_TOKEN:
        session_config['aws_session_token'] = AWS_SESSION_TOKEN

    return boto3.client('s3', **session_config)


def upload_to_s3(data: pd.DataFrame, bucket: str, key: str,
                 format: str = 'json') -> bool:
    """
    Upload DataFrame to S3 as JSON or Parquet.

    Args:
        data: pandas DataFrame to upload
        bucket: S3 bucket name
        key: S3 object key (file path)
        format: 'json' or 'parquet'

    Returns:
        bool: True if successful
    """
    s3_client = get_s3_client()

    try:
        if format == 'json':
            # Convert DataFrame to JSON
            json_buffer = data.to_json(orient='records', date_format='iso')
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json_buffer,
                ContentType='application/json'
            )
        elif format == 'parquet':
            # Save to temporary parquet file then upload
            buffer = io.BytesIO()
            data.to_parquet(buffer, engine='pyarrow', compression='snappy')
            buffer.seek(0)
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue()
            )
        else:
            raise ValueError(f"Unsupported format: {format}")

        print(f"[SUCCESS] Uploaded {len(data)} records to s3://{bucket}/{key}")
        return True

    except Exception as e:
        print(f"[FAIL] Failed to upload to S3: {str(e)}")
        raise


def check_s3_connection() -> bool:
    """
    Test S3 connection by listing buckets.

    Returns:
        bool: True if connection successful
    """
    try:
        s3_client = get_s3_client()
        s3_client.list_buckets()
        print("[OK] S3 connection successful")
        return True
    except Exception as e:
        print(f"[FAIL] S3 connection failed: {str(e)}")
        return False


def upload_to_s3_partitioned(data: pd.DataFrame, bucket: str, base_key: str,
                             partition_cols: list, format: str = 'json') -> bool:
    """
    Upload DataFrame to S3 with date partitioning.

    Args:
        data: pandas DataFrame to upload
        bucket: S3 bucket name
        base_key: Base S3 path (e.g., 'airquality/hanoi')
        partition_cols: Columns to partition by (e.g., ['year', 'month', 'day'])
        format: 'json' or 'parquet'

    Returns:
        bool: True if successful
    """
    # Add partition columns if not exist
    if 'datetime' in data.columns:
        data['year'] = data['datetime'].dt.year
        data['month'] = data['datetime'].dt.month.astype(str).str.zfill(2)
        data['day'] = data['datetime'].dt.day.astype(str).str.zfill(2)

    # Group by partition
    for partition, group_df in data.groupby(partition_cols):
        partition_path = '/'.join([f"{col}={val}" for col, val in zip(partition_cols, partition)])
        s3_key = f"{base_key}/{partition_path}/data.{format}"
        upload_to_s3(group_df, bucket, s3_key, format)

    return True
