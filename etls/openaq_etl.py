from openaq import OpenAQ
import pandas as pd
from datetime import datetime, timedelta
from utils.constants import OPENAQ_API_KEY


def connect_openaq(api_key: str) -> OpenAQ:
    """
    Create OpenAQ client with API key.

    Args:
        api_key: OpenAQ API key

    Returns:
        OpenAQ: Authenticated OpenAQ client

    Raises:
        Exception: If connection fails
    """
    try:
        client = OpenAQ(api_key=api_key)
        return client
    except Exception as e:
        raise Exception(f"Failed to create OpenAQ client: {str(e)}")


def extract_locations(client: OpenAQ, city: str, country: str) -> list:
    """
    Get all monitoring locations for a specific city.

    Args:
        client: OpenAQ client instance
        city: City name (e.g., "Hanoi")
        country: Country code (e.g., "VN")

    Returns:
        list: List of location IDs
    """
    try:
        locations = client.locations.list(
            city=city,
            country=country,
            limit=1000
        )
        location_ids = [loc['id'] for loc in locations.results]
        return location_ids
    except Exception as e:
        raise Exception(f"Failed to extract locations: {str(e)}")


def extract_measurements(client: OpenAQ, location_ids: list,
                        date_from: datetime, date_to: datetime) -> list:
    """
    Extract hourly air quality measurements for given locations and time range.

    Args:
        client: OpenAQ client instance
        location_ids: List of location IDs
        date_from: Start datetime
        date_to: End datetime

    Returns:
        list: List of measurement dictionaries
    """
    all_measurements = []

    for location_id in location_ids:
        try:
            measurements = client.measurements.list(
                locations_id=location_id,
                date_from=date_from.isoformat(),
                date_to=date_to.isoformat(),
                temporal_resolution='hours',
                limit=10000
            )

            for measurement in measurements.results:
                measurement_data = {
                    'location_id': location_id,
                    'parameter': measurement.get('parameter', {}).get('name'),
                    'value': measurement.get('value'),
                    'unit': measurement.get('parameter', {}).get('units'),
                    'datetime': measurement.get('date', {}).get('local'),
                    'latitude': measurement.get('coordinates', {}).get('latitude'),
                    'longitude': measurement.get('coordinates', {}).get('longitude'),
                    'country': measurement.get('location', {}).get('country'),
                    'city': measurement.get('location', {}).get('city')
                }
                all_measurements.append(measurement_data)

        except Exception as e:
            print(f"[WARNING] Failed to fetch measurements for location {location_id}: {e}")
            continue

    return all_measurements


def transform_measurements(measurements: list) -> pd.DataFrame:
    """
    Transform raw measurements into structured DataFrame.

    Args:
        measurements: List of measurement dictionaries

    Returns:
        pd.DataFrame: Cleaned and pivoted DataFrame
    """
    df = pd.DataFrame(measurements)

    if df.empty:
        return df

    # Convert datetime
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Add extraction timestamp
    df['extracted_at'] = datetime.now()

    # Pivot data so each parameter is a column
    df_pivot = df.pivot_table(
        index=['location_id', 'datetime', 'latitude', 'longitude', 'city', 'country', 'extracted_at'],
        columns='parameter',
        values='value',
        aggfunc='mean'
    ).reset_index()

    # Flatten column names
    df_pivot.columns.name = None

    # Sort by datetime
    df_pivot = df_pivot.sort_values('datetime').reset_index(drop=True)

    return df_pivot
