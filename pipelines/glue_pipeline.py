"""
Glue and Athena pipeline functions for Airflow DAG tasks.
Handles Glue Crawler cataloging, Glue Transform job orchestration, and Athena validation.
"""
from utils.glue_utils import (
    start_crawler, get_crawler_status, start_glue_job, get_job_run_status, get_job_run_details
)
from utils.athena_utils import get_table_count, list_tables
from utils.constants import (
    GLUE_CRAWLER_NAME, GLUE_TRANSFORM_JOB_NAME, ATHENA_DATABASE, ENV,
    RAW_FOLDER, AWS_BUCKET_NAME
)
from utils.constants import OPENAQ_TARGET_COUNTRY

def trigger_crawler_task(crawler_name: str = None, **context) -> str:
    """Trigger Glue Crawler - callable for Airflow task."""
    crawler = crawler_name or GLUE_CRAWLER_NAME
    print(f"[START] Triggering Glue Crawler: {crawler}")

    start_crawler(crawler)

    # Store crawler name in XCom for downstream tasks
    context['ti'].xcom_push(key='crawler_name', value=crawler)

    print(f"[OK] Crawler trigger completed")
    return crawler


def check_crawler_status(**context) -> bool:
    """Check if crawler has completed - callable for Airflow sensor."""
    crawler_name = context['ti'].xcom_pull(key='crawler_name') or GLUE_CRAWLER_NAME
    status = get_crawler_status(crawler_name)

    if status == 'READY':
        print(f"[SUCCESS] Crawler '{crawler_name}' is ready")
        return True
    elif status in ['STOPPING', 'RUNNING']:
        print(f"[INFO] Crawler '{crawler_name}' status: {status}")
        return False
    else:
        print(f"[WARNING] Crawler '{crawler_name}' unexpected status: {status}")
        return False


def validate_athena_data(**context) -> bool:
    """Validate data is queryable in Athena after Glue cataloging."""
    print(f"[START] Validating Athena data availability in DB: {ATHENA_DATABASE} ({ENV} env)")

    try:
        # 1. Lấy danh sách bảng
        tables = list_tables(ATHENA_DATABASE)
        print(f"[INFO] Found {len(tables)} tables in database '{ATHENA_DATABASE}'")

        if not tables:
            print(f"[WARNING] No tables found in Athena database '{ATHENA_DATABASE}'")
            # Nếu là môi trường mới chưa có bảng thì fail để cảnh báo
            return False

        # 2. Kiểm tra dữ liệu trong các bảng
        print(f"[INFO] Validating data in {len(tables)} tables...")
        
        tables_with_data = 0
        empty_tables = []

        for table_name in tables:
            try:
                # Đếm số dòng
                count = get_table_count(table_name, ATHENA_DATABASE)
                
                if count > 0:
                    print(f"[OK] Table '{table_name}' has {count} rows")
                    tables_with_data += 1
                else:
                    print(f"[WARNING] Table '{table_name}' is empty")
                    empty_tables.append(table_name)

            except Exception as e:
                print(f"[WARNING] Failed to validate table '{table_name}': {e}")
                continue

        # 3. Kết luận
        # Chỉ cần có ít nhất 1 bảng có dữ liệu là coi như Pipeline chạy thành công
        if tables_with_data > 0:
            print(f"[SUCCESS] Validation Passed: {tables_with_data}/{len(tables)} tables have data.")
            return True
        else:
            print(f"[FAIL] All tables in '{ATHENA_DATABASE}' are empty or inaccessible.")
            return False

    except Exception as e:
        print(f"[FAIL] Athena validation failed: {e}")
        raise


def trigger_glue_transform_job(job_name: str = None, **context) -> str:
    """
    Trigger AWS Glue PySpark transformation job.

    This function:
    1. Retrieves raw data S3 path from upstream extraction task via XCom
    2. Prepares job arguments (input/output paths)
    3. Starts Glue job with PySpark transformation script
    4. Stores job run_id in XCom for downstream monitoring

    Input: Raw JSON measurements from OpenAQ extraction
    - s3://bucket/aq_raw/year/month/day/hour/*.json

    Transformations applied by Glue job:
    - Parse datetime strings to Spark timestamps
    - Deduplicate measurements by location_id + datetime
    - Pivot parameter columns (PM2.5, PM10, NO2, SO2, O3, CO)
    - Enrich with location metadata (coordinates, city, country)
    - Extract year/month/day partition columns

    Output: Partitioned Parquet files
    - s3://bucket/aq_dev/marts/year=YYYY/month=MM/day=DD/*.parquet

    Args:
        job_name: Glue job name (uses config default if None)
        **context: Airflow context (ti, task_instance, etc.)

    Returns:
        str: Glue job run ID

    XCom Push:
        - glue_transform_job_run_id: Run ID for monitoring
        - glue_transform_job_name: Job name
    """
    job_name = job_name or GLUE_TRANSFORM_JOB_NAME

    print(f"[START] Triggering Glue Transform Job: {job_name}")

    ti = context['ti']

    # Pull extraction result from upstream Lambda task
    try:
        import json

        # Lambda task stores response in 'return_value' key
        extraction_result = ti.xcom_pull(
            task_ids='lambda_extract_vietnam',
            key='return_value'
        )

        print(f"[DEBUG] Raw XCom data type: {type(extraction_result)}")
        print(f"[DEBUG] Raw XCom data: {str(extraction_result)[:500]}")  # First 500 chars

        # Parse the Lambda response - handle multiple possible formats
        if extraction_result:
            # Case 1: If it's already a string, try to parse it as JSON
            if isinstance(extraction_result, str):
                extraction_result = json.loads(extraction_result)

            # Case 2: If it's a dict, check if it's a Lambda response wrapper
            if isinstance(extraction_result, dict):
                # Lambda response wrapper with Payload
                if 'Payload' in extraction_result:
                    payload_str = extraction_result['Payload'].read().decode('utf-8')
                    extraction_result = json.loads(payload_str)

                # Lambda HTTP response with statusCode and body
                if 'statusCode' in extraction_result:
                    body = extraction_result.get('body')
                    if isinstance(body, str):
                        extraction_result = json.loads(body)
                    elif isinstance(body, dict):
                        extraction_result = body

        print(f"[DEBUG] Parsed extraction_result type: {type(extraction_result)}")
        print(f"[DEBUG] Parsed extraction_result keys: {extraction_result.keys() if isinstance(extraction_result, dict) else 'N/A'}")

    except Exception as e:
        print(f"[WARNING] Failed to pull extraction result: {e}")
        import traceback
        print(f"[DEBUG] Traceback: {traceback.format_exc()}")
        extraction_result = None

    if not extraction_result:
        print("[WARNING] No extraction result found. Using default paths.")
        extraction_result = {
            'status': 'WARNING',
            'location_count': 0,
            'record_count': 0,
            'raw_s3_path': f"s3://{AWS_BUCKET_NAME}/{RAW_FOLDER}/"
        }

    raw_s3_path = extraction_result.get('raw_s3_path', f"s3://{AWS_BUCKET_NAME}/{RAW_FOLDER}/")
    location_count = extraction_result.get('location_count', 0)
    record_count = extraction_result.get('record_count', 0)

    print(f"[INFO] Extraction metadata:")
    print(f"  - Locations: {location_count}")
    print(f"  - Records: {record_count}")
    print(f"  - Raw data path: {raw_s3_path}")

    # Map country codes to full names for table naming
    country_name_map = {"VN": "vietnam", "TH": "thailand"}
    country_folder = country_name_map.get(OPENAQ_TARGET_COUNTRY, OPENAQ_TARGET_COUNTRY.lower())
    
    # Prepare Glue job arguments
    input_path = f"s3://{AWS_BUCKET_NAME}/{RAW_FOLDER}/"
    output_path = f"s3://{AWS_BUCKET_NAME}/aq_dev/marts/{country_folder}/"

    job_arguments = {
        '--input_path': input_path,
        '--output_path': output_path,
        '--env': ENV,
        '--partition_cols': 'year,month,day',
        '--TempDir': f"s3://{AWS_BUCKET_NAME}/glue-temp/",
    }

    print(f"[INFO] Glue Job Arguments:")
    for key, value in job_arguments.items():
        print(f"  {key}: {value}")

    try:
        # Start Glue job
        run_id = start_glue_job(job_name=job_name, arguments=job_arguments)

        # Push to XCom for downstream tasks
        ti.xcom_push(key='glue_transform_job_run_id', value=run_id)
        ti.xcom_push(key='glue_transform_job_name', value=job_name)

        print(f"[OK] Glue transform job triggered successfully")
        print(f"[INFO] Job run ID: {run_id}")

        return run_id

    except Exception as e:
        print(f"[FAIL] Failed to trigger Glue transform job: {str(e)}")
        raise


def check_glue_transform_status(**context) -> bool:
    """Check if Glue transform job has completed - callable for Airflow sensor."""
    run_id = context['ti'].xcom_pull(key='glue_transform_job_run_id')
    job_name = context['ti'].xcom_pull(key='glue_transform_job_name') or GLUE_TRANSFORM_JOB_NAME

    if not run_id:
        print("[WARNING] No job run ID found in XCom. Waiting...")
        return False

    status = get_job_run_status(job_name, run_id)

    print(f"[INFO] Glue transform job status: {status}")
    print(f"  Job: {job_name}")
    print(f"  Run ID: {run_id}")

    if status == 'SUCCEEDED':
        print(f"[SUCCESS] Glue transform job '{job_name}' completed successfully")
        return True
    elif status in ['RUNNING', 'STARTING']:
        print(f"[INFO] Glue transform job '{job_name}' status: {status}")
        return False
    else:
        # FAILED, ERROR, TIMEOUT
        details = get_job_run_details(job_name, run_id)
        error_msg = details.get('error_message', 'Unknown error')
        print(f"[FAIL] Glue transform job '{job_name}' failed: {error_msg}")
        raise Exception(f"Glue job {job_name} failed with status: {status}")