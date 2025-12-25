"""
AWS Lambda Handler for OpenAQ Data Extraction

This Lambda function extracts air quality data from OpenAQ API for Vietnam
and uploads raw JSON to S3 for further processing.

Environment Variables Required:
- OPENAQ_API_KEY: API key for OpenAQ v3
- AWS_BUCKET_NAME: S3 bucket name (default: openaq-data-pipeline)
- PIPELINE_ENV: dev or prod (default: dev)
"""

import json
import os
from datetime import datetime, timedelta
import boto3

# Import extraction modules (absolute imports for Lambda compatibility)
from etls.extract_location import extract_location
from etls.extract_sensor_measurement import extract_sensor_measurement
from etls.openaq_etl import connect_openaq


def lambda_handler(event, context):
    """
    Lambda entry point for OpenAQ data extraction.
    
    Expected event format:
    {
        "file_name": "vietnam_national_20251222",
        "vietnam_wide": true,
        "lookback_hours": 24,
        "required_parameters": ["PM2.5", "PM10"]
    }
    
    Returns:
        dict: Response with status code and extraction results
    """
    
    print("[START] OpenAQ Lambda extraction started")
    
    # Parse event
    file_name = event.get('file_name', f'vietnam_national_{datetime.now().strftime("%Y%m%d%H%M%S")}')
    vietnam_wide = event.get('vietnam_wide', True)
    lookback_hours = event.get('lookback_hours', 24)
    required_parameters = event.get('required_parameters', ['PM2.5', 'PM10'])
    
    # Get environment variables
    api_key = os.environ.get('OPENAQ_API_KEY')
    bucket_name = os.environ.get('AWS_BUCKET_NAME', 'openaq-data-pipeline')
    env = os.environ.get('PIPELINE_ENV', 'dev')
    
    if not api_key:
        print("[FAIL] OPENAQ_API_KEY not configured")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'OPENAQ_API_KEY not configured'})
        }
    
    print(f"[INFO] Configuration:")
    print(f"  - File name: {file_name}")
    print(f"  - Vietnam-wide: {vietnam_wide}")
    print(f"  - Lookback hours: {lookback_hours}")
    print(f"  - Environment: {env}")
    print(f"  - Bucket: {bucket_name}")
    
    try:
        # Step 1: Connect to OpenAQ API
        print("[INFO] Connecting to OpenAQ API...")
        headers = connect_openaq(api_key)
        print("[OK] Connected to OpenAQ API")
        
        # Step 2: Extract locations and sensors
        print(f"[INFO] Extracting {'Vietnam-wide' if vietnam_wide else 'city-specific'} locations...")
        
        # extract_location returns (sensor_ids, location_objects) tuple
        sensor_ids, location_objects = extract_location(
            headers=headers,
            vietnam_wide=vietnam_wide,
            lookback_days=7,
            required_parameters=required_parameters
        )
        
        if not sensor_ids or not location_objects:
            print("[WARNING] No locations found")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'NO_DATA',
                    'location_count': 0,
                    'sensor_count': 0,
                    'record_count': 0
                })
            }
        
        location_count = len(location_objects)
        sensor_count = len(sensor_ids)
        print(f"[OK] Found {location_count} locations and {sensor_count} sensors")
        
        # Step 3: Extract measurements
        print(f"[INFO] Extracting measurements (lookback: {lookback_hours}h)...")
        
        date_from = datetime.utcnow() - timedelta(hours=lookback_hours)
        date_to = datetime.utcnow()
        
        measurements_list = extract_sensor_measurement(
            headers=headers,
            sensor_ids=sensor_ids,
            date_from=date_from,
            date_to=date_to
        )
        
        if not measurements_list:
            print("[WARNING] No measurements found")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'NO_MEASUREMENTS',
                    'location_count': location_count,
                    'sensor_count': sensor_count,
                    'record_count': 0
                })
            }
        
        record_count = len(measurements_list)
        print(f"[OK] Extracted {record_count} measurement records")
        
        # Step 4: Enrich measurements with location metadata
        print("[INFO] Enriching measurements with location metadata...")
        
        # Convert location_objects to lookup dict by sensor_id
        sensor_to_location = {}
        for loc in location_objects:
            sensors = loc.get('sensors', [])
            for sensor in sensors:
                sensor_id = sensor.get('id')
                if sensor_id:
                    sensor_to_location[sensor_id] = {
                        'location_id': loc.get('id'),
                        'location_name': loc.get('name'),
                        'city': loc.get('city'),
                        'country': loc.get('country', {}).get('name') if isinstance(loc.get('country'), dict) else 'Vietnam',
                        'latitude': loc.get('coordinates', {}).get('latitude'),
                        'longitude': loc.get('coordinates', {}).get('longitude')
                    }
        
        # Enrich each measurement
        enriched_records = []
        for m in measurements_list:
            sensor_id = m.get('sensor_id')
            loc_info = sensor_to_location.get(sensor_id, {})
            
            enriched_record = {
                **m,  # Include all original fields
                'location_id': loc_info.get('location_id'),
                'location_name': loc_info.get('location_name'),
                'city': loc_info.get('city'),
                'country': loc_info.get('country'),
                'latitude': loc_info.get('latitude'),
                'longitude': loc_info.get('longitude')
            }
            enriched_records.append(enriched_record)
        
        print(f"[OK] Enriched {len(enriched_records)} records")
        
        # Step 5: Upload to S3
        now = datetime.now()
        year, month, day, hour = now.year, now.month, now.day, now.hour
        s3_key = f"aq_raw/{year:04d}/{month:02d}/{day:02d}/{hour:02d}/{file_name}.json"
        
        print(f"[INFO] Uploading to S3: s3://{bucket_name}/{s3_key}")
        
        s3_client = boto3.client('s3')
        
        # Convert to newline-delimited JSON (NDJSON for Spark compatibility)
        json_data = '\n'.join(json.dumps(record) for record in enriched_records)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        
        s3_path = f"s3://{bucket_name}/{s3_key}"
        print(f"[SUCCESS] Uploaded {record_count} records to {s3_path}")
        
        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'SUCCESS',
                'location_count': location_count,
                'sensor_count': sensor_count,
                'record_count': record_count,
                'raw_s3_path': s3_path,
                'file_name': file_name,
                'timestamp': now.isoformat()
            })
        }
        
    except Exception as e:
        print(f"[FAIL] Lambda execution failed: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'ERROR',
                'error': str(e),
                'error_type': type(e).__name__
            })
        }
