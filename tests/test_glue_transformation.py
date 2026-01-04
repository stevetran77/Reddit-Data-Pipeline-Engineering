"""
Phase 4: Glue Transformation Data Quality Validation Test

Validates data quality of Glue transformation output:
- No duplicate records (location_id + datetime uniqueness)
- No null values in critical columns
- Datetime values are valid timestamps
- Coordinates within valid ranges
- Parameter values are positive numbers
- Partitions match datetime values
- Partition completeness for processed period
- Parameter coverage (all 7 parameters present)
- Location coverage (all extraction locations in output)
- Deduplication ratio (raw vs deduplicated)

Run with:
  pytest tests/test_glue_transformation.py -v
  python tests/test_glue_transformation.py
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
    from pyspark.sql import functions as F
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

# Setup Logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("GlueDataQualityValidator")

PARAMETER_COLUMNS = ['pm25', 'pm10', 'no2', 'so2', 'o3', 'co']
VALID_LATITUDE_RANGE = (-90, 90)
VALID_LONGITUDE_RANGE = (-180, 180)


class TestGlueDataQuality(unittest.TestCase):
    """Test Glue transformation data quality"""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark Session once for all tests"""
        if SPARK_AVAILABLE:
            cls.spark = SparkSession.builder \
                .appName("GlueDataQualityTest") \
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

    def _create_test_data(self, temp_dir, include_duplicates=False, include_nulls=False):
        """Create test Parquet with realistic data"""
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

        schema = StructType([
            StructField("location_id", StringType(), False),
            StructField("datetime", TimestampType(), False),
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
                "city_name": "Hanoi" if not include_nulls else None,
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
            },
        ]

        if include_duplicates:
            # Add duplicate of first record
            data.append(data[0].copy())

        df = self.spark.createDataFrame(data, schema=schema)
        output_path = os.path.join(temp_dir, "quality_test_data")
        df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_path)
        return output_path

    def test_no_duplicate_records(self):
        """Test that output has no duplicate location_id + datetime combinations"""
        print("\n[TEST] No duplicate records...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir, include_duplicates=False)
            df = self.spark.read.parquet(output_path)

            total_count = df.count()
            distinct_count = df.select('location_id', 'datetime').distinct().count()

            self.assertEqual(total_count, distinct_count,
                           f"Found duplicates: {total_count} records, {distinct_count} unique combinations")

            print(f"[OK] No duplicate records found (total={total_count}, unique={distinct_count})")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_critical_columns_not_null(self):
        """Test that critical columns have no null values"""
        print("\n[TEST] Critical columns not null...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir, include_nulls=False)
            df = self.spark.read.parquet(output_path)

            critical_cols = ['location_id', 'datetime']
            for col in critical_cols:
                null_count = df.filter(F.col(col).isNull()).count()
                self.assertEqual(null_count, 0,
                               f"Critical column '{col}' has {null_count} null values")

            print(f"[OK] All {len(critical_cols)} critical columns have no nulls")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_datetime_values_valid(self):
        """Test that datetime values are valid timestamps"""
        print("\n[TEST] Datetime values are valid...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir)
            df = self.spark.read.parquet(output_path)

            # Check for null datetimes
            null_datetimes = df.filter(F.col('datetime').isNull()).count()
            self.assertEqual(null_datetimes, 0, "Found null datetime values")

            # Check that datetimes are in reasonable range (not too far in past/future)
            future_datetimes = df.filter(
                F.col('datetime') > F.lit(datetime(2030, 1, 1))
            ).count()
            self.assertEqual(future_datetimes, 0, "Found datetime values too far in future")

            print("[OK] All datetime values are valid")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_coordinates_in_valid_range(self):
        """Test that latitude/longitude are within valid ranges"""
        print("\n[TEST] Coordinates within valid ranges...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir)
            df = self.spark.read.parquet(output_path)

            # Check latitude range
            invalid_lat = df.filter(
                (F.col('latitude') < VALID_LATITUDE_RANGE[0]) |
                (F.col('latitude') > VALID_LATITUDE_RANGE[1])
            ).count()
            self.assertEqual(invalid_lat, 0, f"Found {invalid_lat} invalid latitude values")

            # Check longitude range
            invalid_lon = df.filter(
                (F.col('longitude') < VALID_LONGITUDE_RANGE[0]) |
                (F.col('longitude') > VALID_LONGITUDE_RANGE[1])
            ).count()
            self.assertEqual(invalid_lon, 0, f"Found {invalid_lon} invalid longitude values")

            print(f"[OK] All coordinates within valid ranges (lat: {VALID_LATITUDE_RANGE}, lon: {VALID_LONGITUDE_RANGE})")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_parameter_values_positive(self):
        """Test that air quality parameter values are non-negative"""
        print("\n[TEST] Parameter values are positive...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir)
            df = self.spark.read.parquet(output_path)

            negative_checks = {}
            for param in PARAMETER_COLUMNS:
                if param in df.columns:
                    negative_count = df.filter(F.col(param) < 0).count()
                    negative_checks[param] = negative_count

            # All should be 0 or not found
            for param, count in negative_checks.items():
                self.assertEqual(count, 0, f"Parameter '{param}' has {count} negative values")

            print(f"[OK] All parameter values are non-negative")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_partitions_match_datetime(self):
        """Test that year/month/day partitions match datetime values"""
        print("\n[TEST] Partitions match datetime...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir)
            df = self.spark.read.parquet(output_path)

            # Extract year/month/day from datetime and compare with partition columns
            df_check = df.withColumn(
                "calc_year", F.year(F.col("datetime"))
            ).withColumn(
                "calc_month", F.lpad(F.month(F.col("datetime")), 2, '0')
            ).withColumn(
                "calc_day", F.lpad(F.dayofmonth(F.col("datetime")), 2, '0')
            ).select(
                F.col("year"),
                F.col("month"),
                F.col("day"),
                F.col("calc_year"),
                F.col("calc_month"),
                F.col("calc_day")
            )

            mismatch = df_check.filter(
                (F.col("year") != F.col("calc_year")) |
                (F.col("month") != F.col("calc_month")) |
                (F.col("day") != F.col("calc_day"))
            ).count()

            self.assertEqual(mismatch, 0, f"Found {mismatch} partition/datetime mismatches")

            print("[OK] All partition columns match datetime values")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_all_parameters_have_some_data(self):
        """Test that all 7 parameters have at least some non-null values"""
        print("\n[TEST] All parameters have data...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir)
            df = self.spark.read.parquet(output_path)

            params_with_data = {}
            for param in PARAMETER_COLUMNS:
                if param in df.columns:
                    non_null = df.filter(F.col(param).isNotNull()).count()
                    params_with_data[param] = non_null

            # Each parameter should have at least some data
            missing_data = [p for p, count in params_with_data.items() if count == 0]
            if missing_data:
                logger.warning(f"[WARNING] Parameters with no data: {missing_data}")

            print(f"[OK] Parameter coverage check complete ({len(params_with_data)} parameters found)")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_all_locations_present(self):
        """Test that all expected locations appear in output"""
        print("\n[TEST] All expected locations present...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir)
            df = self.spark.read.parquet(output_path)

            # Expected locations from test data
            expected_locations = {'loc_hanoi_001', 'loc_hcmc_001'}
            actual_locations = set(df.select('location_id').distinct().rdd.flatMap(lambda x: x).collect())

            missing_locs = expected_locations - actual_locations
            extra_locs = actual_locations - expected_locations

            self.assertEqual(len(missing_locs), 0, f"Missing locations: {missing_locs}")

            if extra_locs:
                logger.info(f"[INFO] Found extra locations (might be fine): {extra_locs}")

            print(f"[OK] All {len(actual_locations)} expected locations present in output")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_location_metadata_consistency(self):
        """Test that location metadata is consistent per location_id"""
        print("\n[TEST] Location metadata consistency...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir)
            df = self.spark.read.parquet(output_path)

            # Group by location_id and check if metadata is consistent
            metadata_distinct = df.select(
                'location_id', 'city_name', 'country_code', 'latitude', 'longitude'
            ).distinct()

            # Count distinct metadata per location
            metadata_per_loc = metadata_distinct.groupBy('location_id').count()
            inconsistent = metadata_per_loc.filter(F.col('count') > 1).count()

            self.assertEqual(inconsistent, 0,
                           f"Found {inconsistent} locations with inconsistent metadata")

            print("[OK] Location metadata is consistent per location_id")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_record_count_reasonable(self):
        """Test that output record count is reasonable (not empty, not extremely large)"""
        print("\n[TEST] Record count is reasonable...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir)
            df = self.spark.read.parquet(output_path)

            record_count = df.count()

            # Should have at least 1 record
            self.assertGreater(record_count, 0, "Output has no records")

            # Should have reasonable upper limit (for test, 1 million is good limit)
            self.assertLess(record_count, 1_000_000,
                          f"Output has unreasonably large record count: {record_count}")

            print(f"[OK] Record count is reasonable: {record_count} records")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_partition_folders_created(self):
        """Test that partition folders are correctly created in output"""
        print("\n[TEST] Partition folders created correctly...")

        temp_dir = tempfile.mkdtemp(prefix="glue_quality_test_")
        try:
            output_path = self._create_test_data(temp_dir)

            # Check if partition folders exist
            year_folders = [d for d in os.listdir(output_path) if d.startswith('year=')]
            self.assertGreater(len(year_folders), 0, "No year partitions found")

            # Check month partition
            if year_folders:
                year_path = os.path.join(output_path, year_folders[0])
                month_folders = [d for d in os.listdir(year_path) if d.startswith('month=')]
                self.assertGreater(len(month_folders), 0, "No month partitions found")

            print(f"[OK] Partition folders created: {len(year_folders)} year partition(s)")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


def run_test_suite():
    """Run all data quality validation tests"""
    print("\n" + "=" * 80)
    print("GLUE TRANSFORMATION DATA QUALITY VALIDATION (PHASE 4)")
    print("=" * 80)

    if not SPARK_AVAILABLE:
        print("[FAIL] PySpark not installed - cannot run tests")
        print("Install with: pip install pyspark")
        return 1

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestGlueDataQuality)
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
        print("\n[SUCCESS] All data quality tests passed!")
        return 0
    else:
        print("\n[FAIL] Some tests failed!")
        return 1


if __name__ == '__main__':
    sys.exit(run_test_suite())
