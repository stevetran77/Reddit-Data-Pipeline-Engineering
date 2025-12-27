from datetime import datetime, timedelta
import pandas as pd
import json

# Import các biến môi trường và folder từ constants
from utils.constants import (
    OPENAQ_API_KEY,
    OPENAQ_TARGET_CITY,
    OPENAQ_TARGET_COUNTRY,
    OPENAQ_LOOKBACK_HOURS,
    AWS_BUCKET_NAME,
    ENV,
    CURRENT_ENV_FOLDER,
    RAW_FOLDER,
    GLUE_TRANSFORM_JOB_NAME,
    GLUE_JOB_TIMEOUT,
    GLUE_WORKER_TYPE,
    GLUE_NUM_WORKERS,
)

# Import refactored extraction functions from openaq_etl
from etls.openaq_etl import (
    connect_openaq,
    fetch_all_vietnam_locations,
    filter_active_sensors,
    extract_measurements,
    transform_measurements,
    enrich_measurements_with_metadata
)

# Import S3 upload functions
from utils.aws_utils import upload_to_s3, get_s3_client

# Import Glue job utilities for triggering transform job
from utils.glue_utils import start_glue_job


def openaq_pipeline(file_name: str, city: str = None, country: str = None,
                   lookback_hours: int = None, vietnam_wide: bool = False, **kwargs):
    """
    Main OpenAQ Extraction Pipeline (Simplified for Spark processing)

    This pipeline ONLY handles:
    1. Extract locations and sensor IDs (from mock JSON or live API)
    2. Extract measurements for those sensor IDs
    3. Enrich with metadata
    4. Upload raw JSON to S3

    Transformation (pivot, dedup, etc.) is now handled by AWS Glue PySpark job.
    Glue job is triggered separately in Airflow DAG.

    Args:
        file_name: Unique filename for raw data archive
        city: Target city (currently not used - reserved for future)
        country: Target country (currently not used - reserved for future)
        lookback_hours: Hours of historical data to extract
        vietnam_wide: If True, extract all Vietnam locations (currently required)
        **kwargs: Additional Airflow context (ti, task_instance, etc.)

    Returns:
        dict: Metadata about extracted data (status, location_count, record_count, raw_s3_path)
    """
    # Use defaults from config if not provided
    city = city or OPENAQ_TARGET_CITY
    country = country or OPENAQ_TARGET_COUNTRY
    lookback_hours = lookback_hours or OPENAQ_LOOKBACK_HOURS

    print(f"[START] OpenAQ Extraction Pipeline - {datetime.now()}")
    if vietnam_wide:
        print(f"Target: ALL Vietnam locations | Lookback: {lookback_hours} hours")
    else:
        print(f"Target: {city}, {country} | Lookback: {lookback_hours} hours")

    try:
        # STEP 1: Connect to OpenAQ (Get Headers)
        print("[1/6] Preparing OpenAQ API Headers...")
        headers = connect_openaq(api_key=OPENAQ_API_KEY)
        print("[OK] Headers prepared")

        # STEP 2: Fetch all Vietnam locations with pagination
        print("[2/6] Fetching all Vietnam locations with sensors...")
        sensor_ids, location_objs = fetch_all_vietnam_locations(headers)

        location_count = len(location_objs)
        initial_sensor_count = len(sensor_ids)
        print(f"[OK] Found {location_count} locations with {initial_sensor_count} sensors")

        # STEP 3: Filter active sensors (recent data + required parameters)
        print("[3/6] Filtering active sensors...")
        active_sensor_ids = filter_active_sensors(
            location_objs,
            lookback_days=7,
            required_parameters=['PM2.5', 'PM10']
        )

        active_sensor_count = len(active_sensor_ids)
        if active_sensor_count == 0:
            print("[WARNING] No active sensors found. Pipeline stopping.")
            return {
                'status': 'WARNING',
                'location_count': location_count,
                'record_count': 0,
                'raw_s3_path': None
            }

        print(f"[OK] Found {active_sensor_count} active sensors (from {initial_sensor_count} total)")

        # STEP 4: Extract measurements from active sensors
        print("[4/6] Extracting measurements from sensors...")
        date_to = datetime.now()
        date_from = date_to - timedelta(hours=lookback_hours)

        measurements = extract_measurements(headers, active_sensor_ids, date_from, date_to)

        if len(measurements) == 0:
            print("[WARNING] No measurements extracted. Pipeline stopping.")
            return {
                'status': 'WARNING',
                'location_count': location_count,
                'record_count': 0,
                'raw_s3_path': None
            }

        print(f"[OK] Extracted {len(measurements)} measurements")

        # STEP 5: Transform measurements into structured DataFrame
        print("[5/6] Transforming measurements...")
        df = transform_measurements(measurements)
        print(f"[OK] Transformed {len(df)} records")

        # STEP 6: Enrich with location metadata (city, coordinates, timezone, etc)
        print("[6/6] Enriching measurements with location metadata...")
        df_enriched = enrich_measurements_with_metadata(df, location_objs)
        print(f"[OK] Enriched {len(df_enriched)} records with location metadata")

        # STEP 7: Archive raw data to S3 (JSON format for Glue to process)
        print("[7/7] Archiving raw data to S3...")
        now = datetime.now()

        # Structure: aq_raw/year/month/day/hour/raw_measurements.json
        raw_key = f"{RAW_FOLDER}/{now.year}/{now.strftime('%m')}/{now.strftime('%d')}/{now.strftime('%H')}/raw_{file_name}.json"

        # Wrap data in API response format (like data/ folder structure)
        wrapped_data = {
            'meta': {
                'name': 'openaq-api',
                'website': 'https://api.openaq.org/v3',
                'found': len(df_enriched),
                'extracted_at': now.isoformat()
            },
            'results': df_enriched.to_dict(orient='records')
        }

        # Upload wrapped data to S3 as single JSON (not NDJSON)
        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=raw_key,
            Body=json.dumps(wrapped_data, default=str),
            ContentType='application/json'
        )
        print(f"[SUCCESS] Uploaded {len(df_enriched)} records to s3://{AWS_BUCKET_NAME}/{raw_key}")

        result = {
            'status': 'SUCCESS',
            'location_count': location_count,
            'record_count': len(measurements),
            'raw_s3_path': f"s3://{AWS_BUCKET_NAME}/{raw_key}"
        }

        print(f"[SUCCESS] Extraction complete:")
        print(f"  - Total Locations: {location_count}")
        print(f"  - Total Sensors: {initial_sensor_count}")
        print(f"  - Active Sensors: {active_sensor_count}")
        print(f"  - Records Extracted: {result['record_count']}")
        print(f"  - Raw data: {result['raw_s3_path']}")

        return result

    except Exception as e:
        print(f"[FAIL] Pipeline failed: {str(e)}")
        raise