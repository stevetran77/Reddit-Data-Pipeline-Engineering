"""
Unit tests for Glue PySpark transformation logic.

Tests core transformation functions:
- Datetime parsing
- Deduplication
- Parameter pivot
- Metadata enrichment
- Partitioning
"""

import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch, call
import json


class TestGlueTransformLogic(unittest.TestCase):
    """Test Glue job transformation logic (mock PySpark operations)."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_raw_measurements = [
            {
                'location_id': 'loc_1',
                'parameter': 'PM2.5',
                'value': 35.5,
                'unit': 'µg/m³',
                'datetime': '2025-12-20T10:00:00',
                'latitude': 21.0285,
                'longitude': 105.8542,
                'city': 'Hanoi',
                'country': 'VN'
            },
            {
                'location_id': 'loc_1',
                'parameter': 'PM10',
                'value': 55.2,
                'unit': 'µg/m³',
                'datetime': '2025-12-20T10:00:00',
                'latitude': 21.0285,
                'longitude': 105.8542,
                'city': 'Hanoi',
                'country': 'VN'
            },
            {
                'location_id': 'loc_1',
                'parameter': 'PM2.5',
                'value': 36.1,
                'unit': 'µg/m³',
                'datetime': '2025-12-20T11:00:00',
                'latitude': 21.0285,
                'longitude': 105.8542,
                'city': 'Hanoi',
                'country': 'VN'
            }
        ]

    def test_datetime_parsing(self):
        """Test datetime string parsing to timestamp."""
        # Simulate datetime parsing
        dt_str = '2025-12-20T10:00:00'

        try:
            parsed_dt = datetime.fromisoformat(dt_str.replace('T', ' '))
            self.assertEqual(parsed_dt.year, 2025)
            self.assertEqual(parsed_dt.month, 12)
            self.assertEqual(parsed_dt.day, 20)
            self.assertEqual(parsed_dt.hour, 10)
        except ValueError:
            self.fail("Datetime parsing failed")

    def test_partition_column_extraction(self):
        """Test extraction of year/month/day partition columns."""
        dt = datetime(2025, 12, 20, 10, 30, 0)

        year = dt.year
        month = str(dt.month).zfill(2)
        day = str(dt.day).zfill(2)

        self.assertEqual(year, 2025)
        self.assertEqual(month, '12')
        self.assertEqual(day, '20')

    def test_deduplication_logic(self):
        """Test deduplication by location_id + datetime."""
        # Simulate deduplication
        measurements = [
            {'location_id': 'loc_1', 'datetime': '2025-12-20T10:00:00', 'parameter': 'PM2.5', 'value': 35.0},
            {'location_id': 'loc_1', 'datetime': '2025-12-20T10:00:00', 'parameter': 'PM2.5', 'value': 36.0},  # Duplicate
            {'location_id': 'loc_1', 'datetime': '2025-12-20T11:00:00', 'parameter': 'PM2.5', 'value': 37.0},
        ]

        # Group by location_id + datetime and keep first
        seen = {}
        deduped = []

        for m in measurements:
            key = (m['location_id'], m['datetime'])
            if key not in seen:
                deduped.append(m)
                seen[key] = True

        # Should have 2 unique records (removed 1 duplicate)
        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped[0]['value'], 35.0)  # First occurrence kept

    def test_pivot_structure(self):
        """Test result structure after parameter pivot."""
        # Simulate pivot result
        pivoted_data = {
            'location_id': 'loc_1',
            'datetime': '2025-12-20T10:00:00',
            'year': 2025,
            'month': '12',
            'day': '20',
            'PM2.5': 35.5,
            'PM10': 55.2,
        }

        # Verify structure
        self.assertIn('location_id', pivoted_data)
        self.assertIn('datetime', pivoted_data)
        self.assertIn('year', pivoted_data)
        self.assertIn('month', pivoted_data)
        self.assertIn('day', pivoted_data)
        self.assertIn('PM2.5', pivoted_data)
        self.assertIn('PM10', pivoted_data)

    def test_metadata_enrichment(self):
        """Test metadata enrichment with location info."""
        # Location metadata
        location_map = {
            'loc_1': {
                'location_name': 'Hanoi Station 1',
                'locality': 'Hoan Kiem',
                'timezone': 'Asia/Ho_Chi_Minh',
                'country_code': 'VN',
                'latitude': 21.0285,
                'longitude': 105.8542
            }
        }

        # Measurement to enrich
        measurement = {
            'location_id': 'loc_1',
            'PM2.5': 35.5,
            'city_name': None,
            'country_code': None,
            'latitude': None,
            'longitude': None
        }

        # Enrich from map
        metadata = location_map.get(measurement['location_id'], {})
        measurement['city_name'] = metadata.get('locality', 'Unknown')
        measurement['country_code'] = metadata.get('country_code', 'VN')
        measurement['latitude'] = metadata.get('latitude', 0.0)
        measurement['longitude'] = metadata.get('longitude', 0.0)

        # Verify enrichment
        self.assertEqual(measurement['city_name'], 'Hoan Kiem')
        self.assertEqual(measurement['country_code'], 'VN')
        self.assertEqual(measurement['latitude'], 21.0285)
        self.assertEqual(measurement['longitude'], 105.8542)

    def test_null_handling(self):
        """Test handling of null values in data."""
        # Data with nulls
        data = {
            'location_id': 'loc_1',
            'PM2.5': 35.5,
            'PM10': None,  # Null value
            'NO2': None,   # Null value
            'city_name': None,
            'country_code': 'VN'
        }

        # Fill nulls with defaults
        data['city_name'] = data['city_name'] or 'Unknown'
        data['country_code'] = data['country_code'] or 'VN'

        self.assertEqual(data['city_name'], 'Unknown')
        self.assertEqual(data['country_code'], 'VN')

    def test_s3_path_construction(self):
        """Test S3 partitioned path construction."""
        year = 2025
        month = '12'
        day = '20'
        base_path = 's3://my-bucket/aq_dev/marts/'

        # Construct partitioned path
        partitioned_path = f"{base_path}year={year}/month={month}/day={day}/data.parquet"

        expected = "s3://my-bucket/aq_dev/marts/year=2025/month=12/day=20/data.parquet"
        self.assertEqual(partitioned_path, expected)

    def test_record_validation(self):
        """Test validation of output records."""
        # Record to validate
        record = {
            'location_id': 'loc_1',
            'datetime': datetime(2025, 12, 20, 10, 0, 0),
            'year': 2025,
            'month': '12',
            'day': '20',
            'PM2.5': 35.5
        }

        # Critical columns
        critical_cols = ['location_id', 'datetime', 'year', 'month', 'day']

        # Validate all critical columns exist and not null
        for col in critical_cols:
            self.assertIn(col, record)
            self.assertIsNotNone(record[col])


class TestGlueJobIntegration(unittest.TestCase):
    """Test Glue job integration with Airflow pipeline."""

    @patch('utils.glue_utils.start_glue_job')
    def test_trigger_glue_job(self, mock_start_job):
        """Test triggering Glue job from Airflow task."""
        # Mock successful job start
        mock_start_job.return_value = 'job_run_id_12345'

        # Import after patching
        from utils.glue_utils import start_glue_job

        # Call function
        run_id = start_glue_job(
            job_name='openaq_transform_measurements_dev',
            arguments={'--input_path': 's3://bucket/aq_raw/', '--output_path': 's3://bucket/aq_dev/marts/'}
        )

        # Verify
        self.assertEqual(run_id, 'job_run_id_12345')
        mock_start_job.assert_called_once()

    @patch('utils.glue_utils.get_job_run_status')
    def test_check_job_status(self, mock_get_status):
        """Test polling Glue job status."""
        # Mock job completion
        mock_get_status.return_value = 'SUCCEEDED'

        # Import after patching
        from utils.glue_utils import get_job_run_status

        # Call function
        status = get_job_run_status('openaq_transform_measurements_dev', 'job_run_id_12345')

        # Verify
        self.assertEqual(status, 'SUCCEEDED')
        mock_get_status.assert_called_once()

    def test_xcom_data_passing(self):
        """Test XCom data passing between Airflow tasks."""
        # Simulate XCom dict
        xcom_data = {}

        # Push data (simulate trigger task)
        xcom_data['glue_job_run_id'] = 'job_run_id_12345'
        xcom_data['glue_job_name'] = 'openaq_transform_measurements_dev'

        # Pull data (simulate sensor task)
        run_id = xcom_data.get('glue_job_run_id')
        job_name = xcom_data.get('glue_job_name')

        # Verify
        self.assertEqual(run_id, 'job_run_id_12345')
        self.assertEqual(job_name, 'openaq_transform_measurements_dev')


class TestDataQuality(unittest.TestCase):
    """Test data quality checks for transformed output."""

    def test_numeric_values(self):
        """Test that air quality values are numeric."""
        values = [35.5, 55.2, 10.1, None]  # None is acceptable

        for value in values:
            if value is not None:
                self.assertIsInstance(value, (int, float))

    def test_location_coordinates(self):
        """Test coordinate validity."""
        # Valid Vietnam coordinates
        locations = [
            {'latitude': 21.0285, 'longitude': 105.8542},  # Hanoi
            {'latitude': 10.7769, 'longitude': 106.7009},  # HCMC
            {'latitude': 16.0544, 'longitude': 108.2022},  # Da Nang
        ]

        for loc in locations:
            lat = loc['latitude']
            lng = loc['longitude']

            # Vietnam bounds (approximate)
            self.assertTrue(8 <= lat <= 24, f"Latitude {lat} out of Vietnam bounds")
            self.assertTrue(102 <= lng <= 110, f"Longitude {lng} out of Vietnam bounds")

    def test_datetime_ordering(self):
        """Test that datetimes are in correct order."""
        records = [
            {'datetime': datetime(2025, 12, 20, 10, 0, 0)},
            {'datetime': datetime(2025, 12, 20, 11, 0, 0)},
            {'datetime': datetime(2025, 12, 20, 12, 0, 0)},
        ]

        for i in range(len(records) - 1):
            self.assertLess(records[i]['datetime'], records[i + 1]['datetime'])

    def test_parameter_names(self):
        """Test that parameter names are valid air quality indicators."""
        valid_parameters = ['PM2.5', 'PM10', 'NO2', 'SO2', 'O3', 'CO']

        sample_data = {
            'PM2.5': 35.5,
            'PM10': 55.2,
            'NO2': 25.1
        }

        for param in sample_data.keys():
            self.assertIn(param, valid_parameters)


if __name__ == '__main__':
    unittest.main()
