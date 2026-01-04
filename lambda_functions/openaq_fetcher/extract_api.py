"""
OpenAQ API extraction module for Lambda function.

Optimized version of etls/openaq_etl.py for AWS Lambda:
- No pandas dependency (reduces package size to ~8MB vs 100MB)
- Direct dict/list operations for measurements
- Minimal dependencies: requests, python-dateutil only
"""

import requests
from datetime import datetime, timedelta


BASE_URL = "https://api.openaq.org/v3"


# ============================================================================
# STEP 1: Authentication
# ============================================================================

def connect_openaq(api_key: str) -> dict:
    """
    Create headers for OpenAQ API authentication.

    Args:
        api_key: OpenAQ API key

    Returns:
        dict: Headers with X-API-Key for API requests
    """
    return {'X-API-Key': api_key}


# ============================================================================
# STEP 2: Fetch Locations (Vietnam-wide)
# ============================================================================

def fetch_all_vietnam_locations(headers: dict, countries_id: int = 56,
                                page_size: int = 100) -> tuple:
    """
    Fetch ALL Vietnam locations with pagination.

    This function retrieves all monitoring locations in Vietnam from the OpenAQ API
    with pagination support. Each location contains a list of sensors.

    Args:
        headers: API authentication headers from connect_openaq()
        countries_id: OpenAQ country ID (56 = Vietnam)
        page_size: Results per page (max 100)

    Returns:
        tuple: (sensor_ids_list, location_objects_list)
               - sensor_ids: List of unique sensor IDs (for measurement extraction)
               - locations: List of location objects with sensors (for enrichment)

    Raises:
        Exception: If API request fails
    """
    try:
        all_locations = []
        sensor_ids = set()
        page = 1
        total_fetched = 0

        print(f"[INFO] Fetching ALL Vietnam locations (countries_id={countries_id})...")

        while True:
            params = {
                'countries_id': countries_id,
                'limit': page_size,
                'page': page
            }

            response = requests.get(f"{BASE_URL}/locations", headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                print(f"[FAIL] API Error {response.status_code}: {response.text}")
                break

            data = response.json()
            results = data.get('results', [])

            if not results:
                print(f"[INFO] Pagination complete at page {page}")
                break

            # Extract sensor IDs from locations
            for loc in results:
                sensors = loc.get('sensors', [])
                for sensor in sensors:
                    sensor_id = sensor.get('id')
                    if sensor_id:
                        sensor_ids.add(sensor_id)

            all_locations.extend(results)
            total_fetched += len(results)
            print(f"[INFO] Page {page}: +{len(results)} locations (TOTAL: {total_fetched})")
            page += 1

        print(f"[SUCCESS] Fetched {len(all_locations)} Vietnam locations with {len(sensor_ids)} sensors")
        return list(sensor_ids), all_locations

    except Exception as e:
        print(f"[FAIL] Failed to fetch Vietnam locations: {str(e)}")
        raise


# ============================================================================
# STEP 3: Filter Active Sensors
# ============================================================================

def filter_active_sensors(locations: list, lookback_days: int = 7,
                          required_parameters: list = None) -> list:
    """
    Filter sensors by activity and required parameters.

    Keeps only sensors that:
    1. Have data from the last N days (active)
    2. Measure at least one of the required parameters

    Args:
        locations: List of location objects from fetch_all_vietnam_locations()
        lookback_days: Only keep sensors with data in last N days (default: 7)
        required_parameters: List of required parameter names (default: ['PM2.5', 'PM10'])

    Returns:
        list: Filtered sensor IDs that meet all criteria

    Raises:
        Exception: If filtering fails
    """
    if required_parameters is None:
        required_parameters = ['PM2.5', 'PM10']

    try:
        active_sensor_ids = []
        cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)

        print(f"[INFO] Filtering sensors: lookback={lookback_days} days, required={required_parameters}")

        for loc in locations:
            # Check if location has recent data
            dt_last = loc.get('datetimeLast')
            if not dt_last:
                continue

            last_dt_str = dt_last.get('utc') if isinstance(dt_last, dict) else str(dt_last)
            if not last_dt_str:
                continue

            try:
                # Parse datetime string: "2024-01-15T10:30:00+00:00"
                last_datetime = datetime.fromisoformat(last_dt_str.replace('Z', '+00:00'))
                last_datetime = last_datetime.replace(tzinfo=None)
            except Exception:
                continue

            # Skip if last update is older than cutoff
            if last_datetime < cutoff_date:
                continue

            # Check if location has required parameters
            sensors = loc.get('sensors', [])
            for sensor in sensors:
                param = sensor.get('parameter', {})
                param_name = param.get('name') if isinstance(param, dict) else None

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

        print(f"[SUCCESS] Filtered to {len(active_sensor_ids)} active sensors")
        return active_sensor_ids

    except Exception as e:
        print(f"[FAIL] Failed to filter sensors: {str(e)}")
        raise


# ============================================================================
# STEP 4: Extract Measurements
# ============================================================================

def extract_measurements(headers: dict, sensor_ids: list,
                        date_from: datetime, date_to: datetime) -> list:
    """
    Extract hourly air quality measurements from sensors.

    Fetches measurements from OpenAQ API for each sensor ID in the specified
    date range. Returns raw measurement data with sensor_id, datetime, value, etc.

    Args:
        headers: API authentication headers from connect_openaq()
        sensor_ids: List of sensor IDs to fetch measurements from
        date_from: Start datetime (UTC)
        date_to: End datetime (UTC)

    Returns:
        list: Measurement records, each with:
              {sensor_id, datetime, value, parameter, unit}

    Raises:
        (No exception raised - continues on individual sensor failures)
    """
    all_measurements = []
    total_records = 0

    print(f"[INFO] Extracting measurements from {len(sensor_ids)} sensors")
    print(f"       Period: {date_from.isoformat()} to {date_to.isoformat()}")

    for idx, sensor_id in enumerate(sensor_ids, 1):
        try:
            meas_url = f"{BASE_URL}/sensors/{sensor_id}/measurements"
            meas_params = {
                'datetime_from': date_from.isoformat(),
                'datetime_to': date_to.isoformat(),
                'limit': 1000
            }

            meas_resp = requests.get(meas_url, headers=headers, params=meas_params, timeout=30)

            if meas_resp.status_code != 200:
                continue

            meas_data = meas_resp.json()
            measurements = meas_data.get('results', [])

            if not measurements:
                continue

            for m in measurements:
                # Extract datetime from period.datetimeFrom
                period = m.get('period', {})
                dt_from = period.get('datetimeFrom', {})
                datetime_val = dt_from.get('utc') if isinstance(dt_from, dict) else dt_from

                # Extract parameter info
                param_info = m.get('parameter', {})

                measurement_record = {
                    'sensor_id': sensor_id,
                    'datetime': datetime_val,
                    'value': m.get('value'),
                    'parameter': param_info.get('name'),
                    'unit': param_info.get('units')
                }
                all_measurements.append(measurement_record)
                total_records += 1

            # Log progress every 10 sensors
            if idx % 10 == 0:
                print(f"[INFO] Processed {idx}/{len(sensor_ids)} sensors, {total_records} records so far")

        except Exception as e:
            print(f"[WARNING] Failed to extract sensor {sensor_id}: {str(e)}")
            continue

    print(f"[SUCCESS] Extracted {total_records} measurements from {len(sensor_ids)} sensors")
    return all_measurements


# ============================================================================
# STEP 5: Transform Measurements (simplified - no pandas)
# ============================================================================

def transform_measurements(measurements: list) -> list:
    """
    Transform raw measurements into standardized format.

    Converts list of measurement dicts into enriched list with:
    - Proper datetime parsing
    - Extracted_at timestamp
    - Ready for enrichment with location metadata

    Args:
        measurements: List of measurement dicts from extract_measurements()

    Returns:
        list: Transformed measurement dicts with extracted_at field

    Note:
        Records with invalid datetime are filtered out.
    """
    if not measurements:
        print("[WARNING] No measurements to transform")
        return []

    transformed = []
    now = datetime.utcnow().isoformat()

    for m in measurements:
        try:
            # Validate datetime
            datetime_val = m.get('datetime')
            if not datetime_val:
                continue

            # Basic ISO format validation (should be string like "2024-01-15T10:30:00Z")
            if not isinstance(datetime_val, str):
                continue

            record = {
                'sensor_id': m.get('sensor_id'),
                'datetime': datetime_val,
                'value': m.get('value'),
                'parameter': m.get('parameter'),
                'unit': m.get('unit'),
                'extracted_at': now
            }
            transformed.append(record)
        except Exception:
            continue

    print(f"[SUCCESS] Transformed {len(transformed)} measurement records")
    return transformed


# ============================================================================
# STEP 6: Enrich with Metadata
# ============================================================================

def enrich_measurements_with_metadata(measurements: list, locations: list) -> list:
    """
    Enrich measurement dicts with location metadata (coordinates, city, etc).

    Joins measurement data with location information by mapping sensor_id
    to location details (coordinates, city name, country code, timezone).

    Args:
        measurements: List of measurement dicts from transform_measurements()
        locations: List of location objects from fetch_all_vietnam_locations()

    Returns:
        list: Enriched measurement dicts with location fields

    Raises:
        Exception: If enrichment fails
    """
    try:
        if not measurements:
            print("[WARNING] Empty measurements list, cannot enrich")
            return []

        # Location ID -> City name mapping for locations with null locality
        # Maps location IDs to proper city names when OpenAQ API doesn't provide locality
        LOCATION_CITY_MAP = {
            # Hanoi locations (based on coordinates ~21°N)
            18: "Hanoi",          # SPARTAN - Vietnam Acad. Sci.
            2539: "Hanoi",        # US Diplomatic Post: Hanoi
            307169: "Hanoi",      # nồng độ pm
            6123215: "Hanoi",     # OceanPark

            # Ho Chi Minh City locations (based on coordinates ~10-11°N)
            2446: "Ho Chi Minh City",      # US Diplomatic Post: Ho Chi Minh City
            268816: "Ho Chi Minh City",    # outdoor
            268821: "Ho Chi Minh City",    # outdoor2
            268929: "Ho Chi Minh City",    # od3
            268935: "Ho Chi Minh City",    # od5
            268937: "Ho Chi Minh City",    # od6
            3276359: "Ho Chi Minh City",   # CMT8
            4743591: "Ho Chi Minh City",   # Trường ĐH Khoa học Tự nhiên, ĐHQG-HCM
            6068138: "Ho Chi Minh City",   # Care Centre
        }

        # Build sensor_id -> location metadata mapping
        sensor_to_location = {}
        for loc in locations:
            loc_id = loc.get('id')
            loc_name = loc.get('name')
            locality = loc.get('locality')
            timezone = loc.get('timezone')

            country = loc.get('country', {})
            country_code = country.get('code') if isinstance(country, dict) else 'VN'

            coords = loc.get('coordinates', {})
            latitude = coords.get('latitude') if coords else None
            longitude = coords.get('longitude') if coords else None

            # Determine city name: use locality if available, else use mapped city, else Unknown
            city_name = locality or LOCATION_CITY_MAP.get(loc_id) or 'Unknown'

            sensors = loc.get('sensors', [])
            for sensor in sensors:
                sensor_id = sensor.get('id')
                if sensor_id:
                    sensor_to_location[sensor_id] = {
                        'location_id': loc_id,
                        'location_name': loc_name,
                        'city': city_name,
                        'timezone': timezone,
                        'country_code': country_code,
                        'latitude': latitude,
                        'longitude': longitude
                    }

        # Enrich each measurement with location metadata
        enriched = []
        for m in measurements:
            sensor_id = m.get('sensor_id')
            location_meta = sensor_to_location.get(sensor_id, {})

            enriched_record = {
                **m,  # Include all original fields
                'location_id': location_meta.get('location_id'),
                'location_name': location_meta.get('location_name'),
                'city': location_meta.get('city') or 'Unknown',
                'timezone': location_meta.get('timezone'),
                'country': location_meta.get('country_code') or 'VN',
                'latitude': location_meta.get('latitude'),
                'longitude': location_meta.get('longitude')
            }
            enriched.append(enriched_record)

        print(f"[SUCCESS] Enriched {len(enriched)} records with location metadata")
        return enriched

    except Exception as e:
        print(f"[FAIL] Failed to enrich measurements: {str(e)}")
        raise
