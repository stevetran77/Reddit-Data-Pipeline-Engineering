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
    """
    # Ưu tiên dùng Airflow Connection nếu có (để bảo mật hơn)
    # Nhưng giữ logic cũ tương thích với file config hiện tại
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
            # engine='pyarrow' mặc định hỗ trợ tốt nhất
            data.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
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
    REMOVES partition columns from the file content to avoid Glue duplicate errors.
    """
    # Create a copy to avoid SettingWithCopyWarning
    df = data.copy()

    # Add partition columns if not exist
    if 'datetime' in df.columns:
        df['year'] = df['datetime'].dt.year
        df['month'] = df['datetime'].dt.month.astype(str).str.zfill(2)
        df['day'] = df['datetime'].dt.day.astype(str).str.zfill(2)

    # Group by partition
    for partition, group_df in df.groupby(partition_cols):
        # Tạo đường dẫn thư mục Partition (Hive style: key=value)
        partition_path = '/'.join([f"{col}={val}" for col, val in zip(partition_cols, partition)])
        s3_key = f"{base_key}/{partition_path}/data.{format}"
        
        # QUAN TRỌNG: Loại bỏ các cột partition khỏi nội dung file
        # Vì thông tin này đã nằm trên đường dẫn thư mục rồi.
        # Nếu để lại sẽ gây lỗi "Duplicate columns" trong Athena/Glue.
        group_df_clean = group_df.drop(columns=partition_cols)
        
        upload_to_s3(group_df_clean, bucket, s3_key, format)

    return True