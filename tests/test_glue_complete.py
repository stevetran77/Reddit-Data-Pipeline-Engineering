"""
Comprehensive Glue Job Test Suite
Combines all Glue-related tests into a single file:
- AWS Glue utilities (client, crawler, job management)
- PySpark transformation logic
- Full job execution simulation
- Edge cases (VoidType, null handling, deduplication)

Run with: pytest tests/test_glue_complete.py -v
Or: python tests/test_glue_complete.py
"""

import unittest
import logging
import sys
import os
import json
import tempfile
import shutil
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import project modules
try:
    from utils.glue_utils import (
        get_glue_client,
        start_crawler,
        get_crawler_status,
        wait_for_crawler,
        start_glue_job,
        get_job_run_status,
        wait_for_job
    )
    GLUE_UTILS_AVAILABLE = True
except ImportError:
    GLUE_UTILS_AVAILABLE = False

# Import PySpark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import StringType, DoubleType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

# Setup Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GlueCompleteTest")


# =============================================================================
# PART 1: AWS Glue Utilities Tests
# =============================================================================

class TestGlueUtilities(unittest.TestCase):
    """Test AWS Glue client and utility functions (mocked)"""

    def test_get_glue_client(self):
        """Test Glue client creation"""
        if not GLUE_UTILS_AVAILABLE:
            self.skipTest("Glue utils not available")

        with patch('boto3.client') as mock_boto:
            mock_boto.return_value = Mock()
            client = get_glue_client()
            self.assertIsNotNone(client)
            # Check that boto3.client was called with 'glue' as first argument
            self.assertEqual(mock_boto.call_args[0][0], 'glue')

    @patch('utils.glue_utils.get_glue_client')
    def test_start_crawler_success(self, mock_get_client):
        """Test starting crawler successfully"""
        if not GLUE_UTILS_AVAILABLE:
            self.skipTest("Glue utils not available")

        mock_client = Mock()
        mock_client.start_crawler.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        mock_get_client.return_value = mock_client

        result = start_crawler("test_crawler")
        self.assertIsNotNone(result)
        self.assertIn('ResponseMetadata', result)
        mock_client.start_crawler.assert_called_once_with(Name="test_crawler")

    @patch('utils.glue_utils.get_glue_client')
    def test_get_crawler_status(self, mock_get_client):
        """Test getting crawler status"""
        if not GLUE_UTILS_AVAILABLE:
            self.skipTest("Glue utils not available")

        mock_client = Mock()
        mock_client.get_crawler.return_value = {
            'Crawler': {'State': 'READY'}
        }
        mock_get_client.return_value = mock_client

        status = get_crawler_status("test_crawler")
        self.assertEqual(status, 'READY')

    @patch('utils.glue_utils.get_glue_client')
    def test_start_glue_job_success(self, mock_get_client):
        """Test starting Glue job"""
        if not GLUE_UTILS_AVAILABLE:
            self.skipTest("Glue utils not available")

        mock_client = Mock()
        mock_client.start_job_run.return_value = {'JobRunId': 'jr_123'}
        mock_get_client.return_value = mock_client

        job_run_id = start_glue_job("test_job", {"--input": "s3://test/"})
        self.assertEqual(job_run_id, 'jr_123')


# =============================================================================
# PART 2: PySpark Transformation Logic Tests
# =============================================================================

class TestGlueTransformLogic(unittest.TestCase):
    """Test PySpark transformation logic with real Spark session"""

    @classmethod
    def setUpClass(cls):
        """Initialize Spark Session once for all tests"""
        if SPARK_AVAILABLE:
            cls.spark = SparkSession.builder \
                .appName("GlueCompleteTest") \
                .master("local[1]") \
                .config("spark.sql.shuffle.partitions", "1") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .getOrCreate()
            cls.spark.sparkContext.setLogLevel("ERROR")
        else:
            print("[WARNING] PySpark not installed")

    @classmethod
    def tearDownClass(cls):
        if SPARK_AVAILABLE:
            cls.spark.stop()

    def setUp(self):
        if not SPARK_AVAILABLE:
            self.skipTest("PySpark not installed")

    def test_datetime_parsing(self):
        """Test ISO 8601 datetime parsing with timezone"""
        print("\n[TEST] Datetime parsing...")

        data = [
            {"datetime": "2025-12-20T10:00:00+07:00", "value": 1},
            {"datetime": "2025-12-20T11:00:00+08:00", "value": 2}
        ]
        df = self.spark.createDataFrame(data)
        df_parsed = df.withColumn("datetime", F.col("datetime").cast("timestamp"))

        # Should not have null values
        null_count = df_parsed.filter(F.col("datetime").isNull()).count()
        self.assertEqual(null_count, 0, "Datetime parsing should not produce nulls")
        print("[OK] Datetime parsing successful")

    def test_partition_column_extraction(self):
        """Test year/month/day extraction from timestamp"""
        print("\n[TEST] Partition column extraction...")

        data = [{"datetime": "2025-12-20T10:00:00+07:00"}]
        df = self.spark.createDataFrame(data)
        df = df.withColumn("datetime", F.col("datetime").cast("timestamp"))
        df = df \
            .withColumn("year", F.year(F.col("datetime"))) \
            .withColumn("month", F.lpad(F.month(F.col("datetime")), 2, '0')) \
            .withColumn("day", F.lpad(F.dayofmonth(F.col("datetime")), 2, '0'))

        row = df.first()
        self.assertEqual(row['year'], 2025)
        self.assertEqual(row['month'], "12")
        self.assertEqual(row['day'], "20")
        print("[OK] Partition columns extracted correctly")

    def test_deduplication(self):
        """Test deduplication logic"""
        print("\n[TEST] Deduplication...")

        data = [
            {"location_id": "loc_1", "datetime": "2025-12-20T10:00:00+07:00", "value": 1},
            {"location_id": "loc_1", "datetime": "2025-12-20T10:00:00+07:00", "value": 2},  # Duplicate
            {"location_id": "loc_2", "datetime": "2025-12-20T10:00:00+07:00", "value": 3}
        ]
        df = self.spark.createDataFrame(data)
        df = df.withColumn("datetime", F.col("datetime").cast("timestamp"))

        window_spec = Window.partitionBy("location_id", "datetime").orderBy(F.col("datetime"))
        df_dedup = df.withColumn("row_num", F.row_number().over(window_spec)) \
            .filter(F.col("row_num") == 1).drop("row_num")

        self.assertEqual(df_dedup.count(), 2, "Should deduplicate to 2 records")
        print("[OK] Deduplication working")

    def test_pivot_operation(self):
        """Test parameter pivot to columns"""
        print("\n[TEST] Pivot operation...")

        data = [
            {"location_id": "loc_1", "datetime": "2025-12-20T10:00:00+07:00", "parameter": "PM2.5", "value": 35.0},
            {"location_id": "loc_1", "datetime": "2025-12-20T10:00:00+07:00", "parameter": "PM10", "value": 50.0},
            {"location_id": "loc_2", "datetime": "2025-12-20T11:00:00+07:00", "parameter": "CO", "value": 1.2}
        ]
        df = self.spark.createDataFrame(data)
        df = df.withColumn("datetime", F.col("datetime").cast("timestamp"))

        df_pivoted = df.groupBy("location_id", "datetime").pivot("parameter").agg(F.mean("value"))

        self.assertIn("PM2.5", df_pivoted.columns)
        self.assertIn("PM10", df_pivoted.columns)
        self.assertIn("CO", df_pivoted.columns)
        self.assertEqual(df_pivoted.count(), 2)
        print("[OK] Pivot operation successful")

    def test_voidtype_fix(self):
        """Test VoidType fix with all-null metadata"""
        print("\n[TEST] VoidType fix with all-null metadata...")

        # Create test data from JSON (to simulate real S3 input with null values)
        import tempfile
        import json
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        try:
            # Write JSON with null values
            test_record = {
                "location_id": "loc_1",
                "parameter": "PM2.5",
                "value": 45.0,
                "datetime": "2025-12-20T10:00:00+07:00",
                "city": None,
                "country": None,
                "latitude": None,
                "longitude": None
            }
            temp_file.write(json.dumps(test_record))
            temp_file.close()

            # Read from JSON (this is how Glue reads from S3)
            df_raw = self.spark.read.json(temp_file.name)

            # Extract metadata WITH explicit casting (the fix)
            metadata_df = df_raw.select(
                F.col("location_id").cast("string"),
                F.col("city").cast("string").alias("city_name"),
                F.col("country").cast("string").alias("country_code"),
                F.col("latitude").cast("double"),
                F.col("longitude").cast("double")
            ).dropDuplicates(["location_id"])

            # Verify no VoidType in schema
            for field in metadata_df.schema.fields:
                self.assertNotEqual(str(field.dataType), 'VoidType',
                                  f"Column '{field.name}' should not be VoidType")

            print("[OK] No VoidType detected")
        finally:
            import os
            os.unlink(temp_file.name)

    def test_full_transformation_flow(self):
        """Test complete transformation pipeline"""
        print("\n[TEST] Full transformation flow...")

        raw_data = [
            {"location_id": "loc_1", "parameter": "PM2.5", "value": 35.5, "datetime": "2025-12-20T10:00:00+07:00", "city": "Hanoi", "country": "VN", "latitude": 21.0, "longitude": 105.0},
            {"location_id": "loc_1", "parameter": "PM2.5", "value": 35.5, "datetime": "2025-12-20T10:00:00+07:00", "city": "Hanoi", "country": "VN", "latitude": 21.0, "longitude": 105.0},  # Duplicate
            {"location_id": "loc_2", "parameter": "CO", "value": 1.0, "datetime": "2025-12-20T12:00:00+07:00", "city": None, "country": None, "latitude": None, "longitude": None}
        ]
        df_raw = self.spark.createDataFrame(raw_data)

        # 1. Transform Datetime
        df_trans = df_raw.withColumn("datetime", F.col("datetime").cast("timestamp"))
        df_trans = df_trans \
            .withColumn("year", F.year(F.col("datetime"))) \
            .withColumn("month", F.lpad(F.month(F.col("datetime")), 2, '0')) \
            .withColumn("day", F.lpad(F.dayofmonth(F.col("datetime")), 2, '0'))

        # 2. Deduplicate
        window_spec = Window.partitionBy("location_id", "datetime", "parameter").orderBy(F.col("datetime"))
        df_dedup = df_trans.withColumn("row_num", F.row_number().over(window_spec)) \
            .filter(F.col("row_num") == 1).drop("row_num")

        # 3. Pivot
        df_pivoted = df_dedup.groupBy("location_id", "datetime", "year", "month", "day") \
            .pivot("parameter").agg(F.mean("value"))

        # 4. Enrich Metadata
        metadata_df = df_raw.select(
            F.col("location_id").cast("string"),
            F.col("city").cast("string").alias("city_name"),
            F.col("country").cast("string").alias("country_code"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double")
        ).dropDuplicates(["location_id"])

        df_final = df_pivoted.join(metadata_df, on="location_id", how="left")

        # 5. Fill Nulls
        df_final = df_final \
            .fillna("Unknown", subset=["city_name"]) \
            .fillna(0.0, subset=["latitude", "longitude"])

        # Assertions
        self.assertEqual(df_final.count(), 2, "Should have 2 records after dedup and pivot")
        self.assertIn("PM2.5", df_final.columns, "Should have PM2.5 column")

        # Check null handling
        row_loc2 = df_final.filter(F.col("location_id") == "loc_2").first()
        self.assertEqual(row_loc2['city_name'], "Unknown")
        self.assertEqual(row_loc2['latitude'], 0.0)

        print("[OK] Full transformation flow successful")


# =============================================================================
# PART 3: Full Job Execution Tests
# =============================================================================

class TestGlueJobExecution(unittest.TestCase):
    """Test full Glue job execution simulation"""

    def setUp(self):
        if not SPARK_AVAILABLE:
            self.skipTest("PySpark not installed")

    def test_full_job_execution_with_parquet_write(self):
        """Test complete job with Parquet write"""
        print("\n[TEST] Full job execution with Parquet write...")

        spark = SparkSession.builder \
            .appName("TestFullExecution") \
            .master("local[1]") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        temp_dir = tempfile.mkdtemp(prefix="glue_full_test_")
        input_dir = os.path.join(temp_dir, "input")
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(input_dir, exist_ok=True)

        try:
            # Create test data
            test_data = [
                {"location_id": "loc_1", "parameter": "PM2.5", "value": 45.0, "datetime": "2025-12-20T10:00:00+07:00", "city": "Hanoi", "country": "VN", "latitude": 21.0, "longitude": 105.0},
                {"location_id": "loc_1", "parameter": "PM10", "value": 80.0, "datetime": "2025-12-20T10:00:00+07:00", "city": "Hanoi", "country": "VN", "latitude": 21.0, "longitude": 105.0},
                {"location_id": "loc_2", "parameter": "CO", "value": 1.5, "datetime": "2025-12-20T11:00:00+07:00", "city": None, "country": None, "latitude": None, "longitude": None}
            ]

            input_file = os.path.join(input_dir, "test.json")
            with open(input_file, 'w') as f:
                for record in test_data:
                    f.write(json.dumps(record) + '\n')

            # Read and transform
            df_raw = spark.read.json(input_dir)
            df_trans = df_raw.withColumn("datetime", F.col("datetime").cast("timestamp"))
            df_trans = df_trans \
                .withColumn("year", F.year(F.col("datetime"))) \
                .withColumn("month", F.lpad(F.month(F.col("datetime")), 2, '0')) \
                .withColumn("day", F.lpad(F.dayofmonth(F.col("datetime")), 2, '0'))

            window_spec = Window.partitionBy("location_id", "datetime").orderBy(F.col("datetime"))
            df_dedup = df_trans.withColumn("row_num", F.row_number().over(window_spec)) \
                .filter(F.col("row_num") == 1).drop("row_num")

            df_pivoted = df_dedup.groupBy("location_id", "datetime", "year", "month", "day") \
                .pivot("parameter").agg(F.mean("value"))

            metadata_df = df_raw.select(
                F.col("location_id").cast("string"),
                F.col("city").cast("string").alias("city_name"),
                F.col("country").cast("string").alias("country_code"),
                F.col("latitude").cast("double"),
                F.col("longitude").cast("double")
            ).dropDuplicates(["location_id"])

            df_final = df_pivoted.join(metadata_df, on="location_id", how="left")
            df_final = df_final \
                .fillna("Unknown", subset=["city_name"]) \
                .fillna("VN", subset=["country_code"]) \
                .fillna(0.0, subset=["latitude", "longitude"])

            # Write to Parquet
            df_final.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_dir)

            # Verify
            df_verify = spark.read.parquet(output_dir)
            self.assertEqual(df_verify.count(), 2, "Should have 2 records in output")

            # Check partitions created
            self.assertTrue(os.path.exists(os.path.join(output_dir, "year=2025")))

            print("[OK] Full job execution successful")

        finally:
            spark.stop()
            shutil.rmtree(temp_dir, ignore_errors=True)


# =============================================================================
# Test Runner
# =============================================================================

def run_test_suite():
    """Run all test suites"""
    print("\n" + "=" * 80)
    print("COMPREHENSIVE GLUE JOB TEST SUITE")
    print("=" * 80)

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestGlueUtilities))
    suite.addTests(loader.loadTestsFromTestCase(TestGlueTransformLogic))
    suite.addTests(loader.loadTestsFromTestCase(TestGlueJobExecution))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")

    if result.wasSuccessful():
        print("\n[SUCCESS] All tests passed!")
        return 0
    else:
        print("\n[FAIL] Some tests failed!")
        return 1


if __name__ == '__main__':
    sys.exit(run_test_suite())
