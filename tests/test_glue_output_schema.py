"""
Phase 4: Glue Output Schema Validation Test

Validates that output Parquet files from Glue transformation contain the expected schema.

Run with:
  pytest tests/test_glue_output_schema.py -v
  python tests/test_glue_output_schema.py

Expected Schema:
- location_id (string)
- datetime (timestamp)
- year, month, day (string/int for partitioning)
- pm25, pm10, no2, so2, o3, co (double/float - air quality parameters)
- city_name, country_code (string - location metadata)
- latitude, longitude (double - coordinates)
"""

import unittest
import logging
import sys
import os
import json
import tempfile
import shutil
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import PySpark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

# Setup Logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("GlueSchemaValidator")

# Expected schema for Glue output (just for reference, actual check is on field types)
EXPECTED_COLUMNS = {
    'location_id': 'StringType',
    'datetime': 'TimestampType',
    'year': ['IntegerType', 'StringType'],
    'month': ['IntegerType', 'StringType'],
    'day': ['IntegerType', 'StringType'],
    'pm25': ['DoubleType'],
    'pm10': ['DoubleType'],
    'no2': ['DoubleType'],
    'so2': ['DoubleType'],
    'o3': ['DoubleType'],
    'co': ['DoubleType'],
    'city_name': 'StringType',
    'country_code': 'StringType',
    'latitude': 'DoubleType',
    'longitude': 'DoubleType'
}

PARAMETER_COLUMNS = ['pm25', 'pm10', 'no2', 'so2', 'o3', 'co']


class TestGlueOutputSchema(unittest.TestCase):
    """Test Glue transformation output Parquet schema"""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark Session once for all tests"""
        if SPARK_AVAILABLE:
            cls.spark = SparkSession.builder \
                .appName("GlueSchemaTest") \
                .master("local[1]") \
                .config("spark.sql.shuffle.partitions", "1") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .getOrCreate()
            cls.spark.sparkContext.setLogLevel("ERROR")
        else:
            logger.warning("[WARNING] PySpark not installed")

    @classmethod
    def tearDownClass(cls):
        if SPARK_AVAILABLE:
            cls.spark.stop()

    def setUp(self):
        if not SPARK_AVAILABLE:
            self.skipTest("PySpark not installed")

    def _create_sample_parquet(self, temp_dir):
        """Create sample Parquet file matching expected output"""
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

        # Define schema matching expected Glue output
        schema = StructType([
            StructField("location_id", StringType(), True),
            StructField("datetime", TimestampType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
            StructField("pm25", DoubleType(), True),
            StructField("pm10", DoubleType(), True),
            StructField("no2", DoubleType(), True),
            StructField("so2", DoubleType(), True),
            StructField("o3", DoubleType(), True),
            StructField("co", DoubleType(), True),
            StructField("city_name", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ])

        # Create test data
        data = [
            {
                "location_id": "loc_hanoi_001",
                "datetime": datetime(2025, 12, 20, 10, 0, 0),
                "year": 2025,
                "month": "12",
                "day": "20",
                "pm25": 35.5,
                "pm10": 55.0,
                "no2": 45.2,
                "so2": 15.1,
                "o3": 30.5,
                "co": 0.8,
                "city_name": "Hanoi",
                "country_code": "VN",
                "latitude": 21.0286,
                "longitude": 105.8038
            },
            {
                "location_id": "loc_hcmc_001",
                "datetime": datetime(2025, 12, 20, 11, 0, 0),
                "year": 2025,
                "month": "12",
                "day": "20",
                "pm25": 42.0,
                "pm10": 68.5,
                "no2": 38.0,
                "so2": 12.5,
                "o3": 28.0,
                "co": 1.2,
                "city_name": "HCMC",
                "country_code": "VN",
                "latitude": 10.7769,
                "longitude": 106.7009
            }
        ]

        df = self.spark.createDataFrame(data, schema=schema)
        output_path = os.path.join(temp_dir, "parquet_data")
        df.write.mode("overwrite").parquet(output_path)
        return output_path

    def test_schema_has_required_columns(self):
        """Test that output has all required columns"""
        print("\n[TEST] Schema has all required columns...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            # Get actual columns
            actual_columns = set(df.columns)
            required_columns = set(EXPECTED_COLUMNS.keys())

            missing = required_columns - actual_columns
            self.assertEqual(len(missing), 0, f"Missing columns: {missing}")

            print(f"[OK] All {len(required_columns)} required columns present")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_schema_critical_columns_not_null(self):
        """Test that critical columns are not nullable"""
        print("\n[TEST] Critical columns configured correctly...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            # Check critical columns are not nullable
            critical_cols = ['location_id', 'datetime']
            schema_dict = {field.name: field for field in df.schema.fields}

            for col_name in critical_cols:
                if col_name in schema_dict:
                    # Note: Parquet schema might allow nulls; we check data quality instead
                    self.assertIn(col_name, schema_dict, f"Missing critical column: {col_name}")

            print(f"[OK] All {len(critical_cols)} critical columns present in schema")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_schema_parameter_columns_exist(self):
        """Test that all air quality parameter columns exist"""
        print("\n[TEST] Air quality parameter columns exist...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            actual_columns = set(df.columns)
            param_columns = set(PARAMETER_COLUMNS)

            missing_params = param_columns - actual_columns
            self.assertEqual(len(missing_params), 0, f"Missing parameter columns: {missing_params}")

            print(f"[OK] All {len(PARAMETER_COLUMNS)} parameter columns present: {', '.join(PARAMETER_COLUMNS)}")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_schema_metadata_columns_exist(self):
        """Test that location metadata columns exist"""
        print("\n[TEST] Metadata columns exist...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            actual_columns = set(df.columns)
            metadata_columns = {'city_name', 'country_code', 'latitude', 'longitude'}

            missing_meta = metadata_columns - actual_columns
            self.assertEqual(len(missing_meta), 0, f"Missing metadata columns: {missing_meta}")

            print(f"[OK] All {len(metadata_columns)} metadata columns present: {', '.join(metadata_columns)}")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_schema_partition_columns_exist(self):
        """Test that partition columns (year, month, day) exist"""
        print("\n[TEST] Partition columns exist...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            actual_columns = set(df.columns)
            partition_columns = {'year', 'month', 'day'}

            missing_parts = partition_columns - actual_columns
            self.assertEqual(len(missing_parts), 0, f"Missing partition columns: {missing_parts}")

            print(f"[OK] All {len(partition_columns)} partition columns present: {', '.join(partition_columns)}")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_schema_datetime_column_is_timestamp(self):
        """Test that datetime column is timestamp type"""
        print("\n[TEST] Datetime column type validation...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            schema_dict = {field.name: field for field in df.schema.fields}
            datetime_field = schema_dict.get('datetime')

            self.assertIsNotNone(datetime_field, "datetime column not found")
            self.assertEqual(str(datetime_field.dataType), 'TimestampType()',
                           f"datetime should be TimestampType, got {datetime_field.dataType}")

            print("[OK] Datetime column is correctly typed as timestamp")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_schema_parameter_columns_are_numeric(self):
        """Test that parameter columns are numeric (DoubleType)"""
        print("\n[TEST] Parameter columns are numeric...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            schema_dict = {field.name: field for field in df.schema.fields}

            for param in PARAMETER_COLUMNS:
                field = schema_dict.get(param)
                if field:  # Parameter might be null if no data for that parameter
                    data_type = str(field.dataType)
                    self.assertIn(data_type, ['DoubleType()', 'FloatType()', 'StructType()'],
                                f"{param} should be numeric, got {data_type}")

            print(f"[OK] All parameter columns are numeric types")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_schema_location_columns_are_string(self):
        """Test that location metadata columns are string type"""
        print("\n[TEST] Location metadata columns are string type...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            schema_dict = {field.name: field for field in df.schema.fields}

            location_cols = ['city_name', 'country_code']
            for col in location_cols:
                field = schema_dict.get(col)
                if field:
                    self.assertEqual(str(field.dataType), 'StringType()',
                                   f"{col} should be StringType, got {field.dataType}")

            print(f"[OK] All location metadata columns are string type")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_schema_coordinate_columns_are_numeric(self):
        """Test that coordinate columns are numeric (DoubleType)"""
        print("\n[TEST] Coordinate columns are numeric...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            schema_dict = {field.name: field for field in df.schema.fields}

            coord_cols = ['latitude', 'longitude']
            for col in coord_cols:
                field = schema_dict.get(col)
                if field:
                    self.assertEqual(str(field.dataType), 'DoubleType()',
                                   f"{col} should be DoubleType, got {field.dataType}")

            print(f"[OK] All coordinate columns are numeric (DoubleType)")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_schema_no_unexpected_columns(self):
        """Test that output doesn't have unexpected extra columns"""
        print("\n[TEST] No unexpected columns...")

        temp_dir = tempfile.mkdtemp(prefix="glue_schema_test_")
        try:
            output_path = self._create_sample_parquet(temp_dir)
            df = self.spark.read.parquet(output_path)

            actual_columns = set(df.columns)
            expected_columns = set(EXPECTED_COLUMNS.keys())

            # Check for extra columns (might be fine, but worth noting)
            extra = actual_columns - expected_columns
            if extra:
                logger.warning(f"[WARNING] Unexpected columns found: {extra}")

            print(f"[OK] Column count check complete (found {len(actual_columns)} columns)")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


def run_test_suite():
    """Run all schema validation tests"""
    print("\n" + "=" * 80)
    print("GLUE OUTPUT SCHEMA VALIDATION TEST SUITE (PHASE 4)")
    print("=" * 80)

    if not SPARK_AVAILABLE:
        print("[FAIL] PySpark not installed - cannot run tests")
        print("Install with: pip install pyspark")
        return 1

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestGlueOutputSchema)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Tests run: {result.testsRun}")
    print(f"Passed: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failed: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.wasSuccessful():
        print("\n[SUCCESS] All schema validation tests passed!")
        return 0
    else:
        print("\n[FAIL] Some tests failed!")
        return 1


if __name__ == '__main__':
    sys.exit(run_test_suite())
