"""
AWS Glue PySpark Job: Transform OpenAQ Raw Measurements to Marts
Processes raw JSON measurements from S3, applies Spark transformations
(datetime parsing, deduplication, pivot, enrichment), and outputs
partitioned Parquet files ready for Athena querying.

Input:  s3://bucket/aq_raw/year/month/day/hour/*.json
Output: s3://bucket/aq_dev/marts/location_*/year/month/day/*.parquet

Usage (Glue Console or CLI):
    glue startJobRun \\
        --job-name openaq_transform_measurements \\
        --arguments='{
            "--input_path": "s3://bucket/aq_raw/",
            "--output_path": "s3://bucket/aq_dev/marts/",
            "--env": "dev",
            "--partition_cols": "year,month,day"
        }'
"""

import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# ============================================================================
# Setup Logging
# ============================================================================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_info(msg: str):
    print(f"[INFO] {msg}")
    logger.info(msg)

def log_ok(msg: str):
    print(f"[OK] {msg}")
    logger.info(msg)

def log_success(msg: str):
    print(f"[SUCCESS] {msg}")
    logger.info(msg)

def log_fail(msg: str):
    print(f"[FAIL] {msg}")
    logger.error(msg)

def log_warning(msg: str):
    print(f"[WARNING] {msg}")
    logger.warning(msg)

# ============================================================================
# STEP 1: Khởi tạo Glue Context
# ============================================================================
log_info("Initializing Glue Context...")

# Gộp tất cả tham số vào một list duy nhất
# AWS Glue sẽ tự động bỏ qua các tham số thừa (như --TempDir) không có trong list này
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'input_path', 'output_path', 'env', 'partition_cols']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
JOB_NAME = args['JOB_NAME']
INPUT_PATH = args['input_path']
OUTPUT_PATH = args['output_path']

# Lấy giá trị từ args (Lưu ý: getResolvedOptions bắt buộc các tham số trên phải tồn tại)
# Vì Airflow pipeline của bạn LUÔN truyền env và partition_cols, nên không sợ lỗi thiếu tham số.
ENV = args.get('env', 'dev')
PARTITION_COLS = args.get('partition_cols', 'year,month,day').split(',')

log_ok(f"Glue job initialized: {JOB_NAME}")

# ============================================================================
# STEP 2: Đọc dữ liệu Raw (JSON) từ S3
# ============================================================================
log_info("\n=== STEP 1: Reading raw JSON data ===")

try:
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [INPUT_PATH], "recurse": True},
        format="json"
    )

    df_raw = datasource.toDF()
    record_count = df_raw.count()

    log_ok(f"Read {record_count} raw records from {INPUT_PATH}")
    log_info(f"Raw data schema: {df_raw.schema}")

except Exception as e:
    log_fail(f"Failed to read raw JSON: {str(e)}")
    raise

# ============================================================================
# STEP 3: Transform - Datetime parsing, deduplication, partition columns
# ============================================================================
log_info("\n=== STEP 2: Transform measurements ===")

try:
    # Parse datetime string to timestamp
    # Sử dụng cast("timestamp") để tự động xử lý định dạng ISO 8601 có timezone (+07:00)
    df_transformed = df_raw.withColumn(
        "datetime",
        F.col("datetime").cast("timestamp")
    )

    # Extract partition columns
    df_transformed = df_transformed \
        .withColumn("year", F.year(F.col("datetime"))) \
        .withColumn("month", F.lpad(F.month(F.col("datetime")), 2, '0')) \
        .withColumn("day", F.lpad(F.dayofmonth(F.col("datetime")), 2, '0'))

    # Deduplicate by location_id + datetime (keep first occurrence)
    # Window function to mark duplicates
    window_spec = Window.partitionBy("location_id", "datetime").orderBy(F.col("datetime"))
    df_transformed = df_transformed.withColumn(
        "row_num",
        F.row_number().over(window_spec)
    ).filter(F.col("row_num") == 1).drop("row_num")

    transformed_count = df_transformed.count()
    log_ok(f"Transformed and deduplicated {transformed_count} records")

except Exception as e:
    log_fail(f"Transformation failed: {str(e)}")
    raise

# ============================================================================
# STEP 4: Pivot parameters into columns
# ============================================================================
log_info("\n=== STEP 3: Pivot parameters ===")

try:
    # Group by location_id + datetime and pivot parameter into columns
    df_pivoted = df_transformed.groupBy(
        F.col("location_id"),
        F.col("datetime"),
        F.col("year"),
        F.col("month"),
        F.col("day")
    ).pivot("parameter").agg(
        F.mean("value")  # Use mean if multiple values for same parameter/datetime
    )

    pivoted_count = df_pivoted.count()
    pivot_columns = [col for col in df_pivoted.columns if col not in ["location_id", "datetime", "year", "month", "day"]]

    log_ok(f"Pivoted {pivoted_count} records")
    log_info(f"Parameter columns (metrics): {pivot_columns}")

except Exception as e:
    log_fail(f"Pivot operation failed: {str(e)}")
    raise

# ============================================================================
# STEP 5: Enrich with metadata
# ============================================================================
log_info("\n=== STEP 4: Enrich with metadata ===")

try:
    # Extract unique location metadata from raw data
    # CRITICAL: Cast to explicit types to prevent VoidType errors when all values are null
    metadata_df = df_raw.select(
        F.col("location_id").cast("string"),
        F.col("city").cast("string").alias("city_name"),
        F.col("country").cast("string").alias("country_code"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double")
    ).dropDuplicates(["location_id"])

    # Join with pivoted data
    df_enriched = df_pivoted.join(
        metadata_df,
        on="location_id",
        how="left"
    )

    # Fill nulls with defaults for metadata
    df_enriched = df_enriched \
        .fillna("Unknown", subset=["city_name"]) \
        .fillna("VN", subset=["country_code"]) \
        .fillna(0.0, subset=["latitude", "longitude"])

    enriched_count = df_enriched.count()
    log_ok(f"Enriched {enriched_count} records with location metadata")

except Exception as e:
    log_fail(f"Enrichment failed: {str(e)}")
    raise

# ============================================================================
# STEP 6: Validate output
# ============================================================================
log_info("\n=== STEP 5: Validate output ===")

try:
    # Check record count
    if enriched_count == 0:
        log_warning("Output DataFrame is empty!")

    # Check critical columns exist
    critical_cols = ["location_id", "datetime", "year", "month", "day"]
    missing_cols = [col for col in critical_cols if col not in df_enriched.columns]

    if missing_cols:
        log_fail(f"Missing critical columns: {missing_cols}")
        raise Exception(f"Missing columns: {missing_cols}")

    # Check null values in critical columns
    null_counts = df_enriched.select([
        F.count(F.when(F.col(col).isNull(), 1)).alias(col)
        for col in critical_cols
    ]).collect()[0].asDict()

    log_ok(f"Output records: {enriched_count}")
    log_info(f"Null values in critical columns: {null_counts}")
    log_info(f"Output columns: {df_enriched.columns}")

except Exception as e:
    log_fail(f"Validation failed: {str(e)}")
    raise

# ============================================================================
# STEP 7: Write partitioned Parquet to S3
# ============================================================================
log_info("\n=== STEP 6: Write to S3 ===")

try:
    # Repartition by location_id for better distribution
    # This ensures each location gets its own folder structure
    df_repartitioned = df_enriched.repartition("location_id")

    # Write partitioned parquet
    # Mode "append" adds new partitions, "overwrite" replaces all
    df_repartitioned.write \
        .mode("append") \
        .partitionBy(PARTITION_COLS) \
        .parquet(OUTPUT_PATH)

    log_success(f"Written partitioned Parquet to {OUTPUT_PATH}")
    log_info(f"Partitioning structure: {PARTITION_COLS}")

except Exception as e:
    log_fail(f"Failed to write Parquet: {str(e)}")
    raise

# ============================================================================
# Commit Glue Job
# ============================================================================
log_success(f"Glue job {JOB_NAME} completed successfully")
job.commit()