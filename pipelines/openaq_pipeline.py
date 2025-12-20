from datetime import datetime, timedelta
import pandas as pd

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

from etls.openaq_etl import (
    connect_openaq,
    extract_locations,
    extract_measurements,
    fetch_all_vietnam_locations,
    filter_active_locations
)

# Import S3 upload functions
from utils.aws_utils import upload_to_s3

# Import Glue job utilities for triggering transform job
from utils.glue_utils import start_glue_job


def openaq_pipeline(file_name: str, city: str = None, country: str = None,
                   lookback_hours: int = None, vietnam_wide: bool = False, **kwargs):
    """
    Main OpenAQ Extraction Pipeline (Simplified for Spark processing)

    This pipeline now ONLY handles:
    1. Extract measurements from OpenAQ API
    2. Upload raw JSON to S3

    Transformation (pivot, dedup, enrich) is now handled by AWS Glue PySpark job.
    Glue job is triggered separately in Airflow DAG.

    Args:
        file_name: Unique filename for raw data archive
        city: Target city (ignored if vietnam_wide=True)
        country: Target country (ignored if vietnam_wide=True)
        lookback_hours: Hours of historical data to extract
        vietnam_wide: If True, extract all Vietnam locations; if False, extract single city
        **kwargs: Additional Airflow context (ti, task_instance, etc.)

    Returns:
        dict: Metadata about extracted data (location_count, record_count, raw_s3_path)
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
        print("[1/4] Preparing OpenAQ API Headers...")
        headers = connect_openaq(api_key=OPENAQ_API_KEY)
        print("[OK] Headers prepared")

        # STEP 2: Get monitoring locations
        if vietnam_wide:
            print("[2/4] Fetching ALL Vietnam locations...")
            all_locations = fetch_all_vietnam_locations(headers)

            # Filter active locations
            location_objs = filter_active_locations(
                all_locations,
                lookback_days=7,
                required_parameters=['PM2.5', 'PM10']
            )

            # Extract list of location IDs
            location_ids = [loc['id'] for loc in location_objs]
            print(f"[OK] Found {len(location_ids)} active monitoring locations")
        else:
            print(f"[2/4] Fetching locations for {city}...")
            location_ids = extract_locations(headers, city, country)
            location_objs = []
            print(f"[OK] Found {len(location_ids)} monitoring locations")

        if len(location_ids) == 0:
            print("[WARNING] No locations found. Pipeline stopping.")
            return {
                'status': 'WARNING',
                'location_count': 0,
                'record_count': 0,
                'raw_s3_path': None
            }

        # STEP 3: Extract measurements from API
        print(f"[3/4] Extracting measurements...")
        date_to = datetime.now()
        date_from = date_to - timedelta(hours=lookback_hours)

        measurements = extract_measurements(headers, location_ids, date_from, date_to)
        print(f"[OK] Extracted {len(measurements)} measurements")

        if len(measurements) == 0:
            print("[WARNING] No measurements extracted. Pipeline stopping.")
            return {
                'status': 'WARNING',
                'location_count': len(location_ids),
                'record_count': 0,
                'raw_s3_path': None
            }

        # STEP 4: Archive raw data to S3 (JSON format for Glue to process)
        print(f"[4/4] Archiving raw data to S3...")
        now = datetime.now()

        # Structure: aq_raw/year/month/day/hour/raw_measurements.json
        raw_key = f"{RAW_FOLDER}/{now.year}/{now.strftime('%m')}/{now.strftime('%d')}/{now.strftime('%H')}/raw_{file_name}.json"

        # Convert list of dicts to DataFrame for upload
        df_raw = pd.DataFrame(measurements)
        upload_to_s3(df_raw, AWS_BUCKET_NAME, raw_key, format='json')

        result = {
            'status': 'SUCCESS',
            'location_count': len(location_ids),
            'record_count': len(measurements),
            'raw_s3_path': f"s3://{AWS_BUCKET_NAME}/{raw_key}"
        }

        print(f"[SUCCESS] Extraction complete:")
        print(f"  - Locations: {result['location_count']}")
        print(f"  - Records: {result['record_count']}")
        print(f"  - Raw data: {result['raw_s3_path']}")

        return result

    except Exception as e:
        print(f"[FAIL] Pipeline failed: {str(e)}")
        raise