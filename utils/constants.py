import configparser
import os
from pathlib import Path

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_FILE = PROJECT_ROOT / "config" / "config.conf"

# Initialize ConfigParser
config = configparser.ConfigParser()
BASE_URL = "https://api.openaq.org/v3"

# Load configuration file (with Lambda fallback)
if CONFIG_FILE.exists():
    config.read(CONFIG_FILE)
else:
    # Lambda mode: use environment variables instead
    if not os.getenv('PIPELINE_ENV'):
        raise FileNotFoundError(f"Configuration file not found: {CONFIG_FILE}")
    # If env var exists, skip config file (Lambda will provide env vars)


# ============================================================================
# ENVIRONMENT CONFIGURATION (UPDATED)
# ============================================================================
# Mặc định là 'dev'
ENV = os.getenv('PIPELINE_ENV', 'dev').lower()

# Chỉ map 2 môi trường chính tương ứng với folder bạn có
ENV_FOLDER_MAP = {
    'dev': 'aq_dev',
    'prod': 'aq_prod'
}

# Lấy folder hiện tại. 
# Nếu ENV là 'staging' hay gì khác lạ, nó sẽ fallback về 'aq_dev' để an toàn.
CURRENT_ENV_FOLDER = ENV_FOLDER_MAP.get(ENV, 'aq_dev')

# Raw folders - separated by environment for test/prod isolation
RAW_FOLDER_MAP = {
    'dev': 'aq_raw_test',
    'prod': 'aq_raw_prod'
}

# Current raw folder based on environment
RAW_FOLDER = RAW_FOLDER_MAP.get(ENV, 'aq_raw_test')

# Legacy raw folder (deprecated - for backward compatibility)
RAW_FOLDER_LEGACY = 'aq_raw'

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================
DATABASE_HOST = config.get("database", "database_host", fallback="localhost")
DATABASE_PORT = config.getint("database", "database_port", fallback=5432)
DATABASE_NAME = config.get("database", "database_name", fallback="airflow_reddit")
DATABASE_USERNAME = config.get("database", "database_username", fallback="postgres")
DATABASE_PASSWORD = config.get("database", "database_password", fallback="postgres")

DATABASE_URL = f"postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"


# ============================================================================
# FILE PATHS
# ============================================================================
INPUT_PATH = config.get("file_paths", "input_path", fallback="/opt/airflow/data/input")
OUTPUT_PATH = config.get("file_paths", "output_path", fallback="/opt/airflow/data/output")

# In AWS Lambda, /opt is read-only. Use /tmp for writable storage.
if os.getenv("AWS_EXECUTION_ENV") or os.getenv("LAMBDA_TASK_ROOT"):
    INPUT_PATH = "/tmp/input"
    OUTPUT_PATH = "/tmp/output"

try:
    os.makedirs(INPUT_PATH, exist_ok=True)
    os.makedirs(OUTPUT_PATH, exist_ok=True)
except OSError:
    # Ignore directory creation errors in read-only environments
    pass


# ============================================================================
# AWS CONFIGURATION
# ============================================================================
AWS_ACCESS_KEY_ID = config.get("aws", "aws_access_key_id", fallback="")
AWS_SECRET_ACCESS_KEY = config.get("aws", "aws_secret_access_key", fallback="")
AWS_SESSION_TOKEN = config.get("aws", "aws_session_token", fallback="")
AWS_REGION = config.get("aws", "aws_region", fallback="us-east-1")
AWS_BUCKET_NAME = config.get("aws", "aws_bucket_name", fallback="")


# ============================================================================
# AWS GLUE CONFIGURATION
# ============================================================================
_glue_db_base = config.get("aws_glue", "glue_database_name", fallback="openaq_database")
_glue_crawler_base = config.get("aws_glue", "glue_crawler_name", fallback="openaq_s3_crawler")

# Tên Database: openaq_dev hoặc openaq_prod
GLUE_DATABASE_NAME = f"openaq_{ENV}" if ENV in ['dev', 'prod'] else "openaq_dev"

# Tên Crawler: openaq_s3_crawler_dev hoặc openaq_s3_crawler_prod
GLUE_CRAWLER_NAME = f"openaq_s3_crawler_{ENV}" if ENV in ['dev', 'prod'] else "openaq_s3_crawler_dev"

# Tên Transform Job: openaq_transform_measurements_dev hoặc openaq_transform_measurements_prod
GLUE_TRANSFORM_JOB_NAME = config.get(
    "aws_glue",
    "glue_transform_job_name",
    fallback=f"openaq_transform_measurements_{ENV}"
)

# Cấu hình PySpark ETL Job
GLUE_ETL_JOB_NAME = config.get("aws_glue", "glue_etl_job_name", fallback="openaq_to_redshift")
GLUE_IAM_ROLE = config.get("aws_glue", "glue_iam_role", fallback="")
GLUE_JOB_TIMEOUT = config.getint("aws_glue", "glue_job_timeout", fallback=2880)  # 48 hours
GLUE_WORKER_TYPE = config.get("aws_glue", "glue_worker_type", fallback="G.1X")
GLUE_NUM_WORKERS = config.getint("aws_glue", "glue_num_workers", fallback=2)


# ============================================================================
# AWS ATHENA CONFIGURATION
# ============================================================================
ATHENA_DATABASE = GLUE_DATABASE_NAME
_bucket = config.get("aws", "aws_bucket_name", fallback="")
# Folder kết quả query: aq_dev/athena-results hoặc aq_prod/athena-results
ATHENA_OUTPUT_LOCATION = f"s3://{_bucket}/{CURRENT_ENV_FOLDER}/athena-results/"

# ============================================================================
# ETL SETTINGS
# ============================================================================
BATCH_SIZE = config.getint("etl_settings", "batch_size", fallback=100)
ERROR_HANDLING = config.get("etl_settings", "error_handling", fallback="abort")
LOG_LEVEL = config.get("etl_settings", "log_level", fallback="info").upper()


# ============================================================================
# OPENAQ API CONFIGURATION
# ============================================================================
OPENAQ_API_KEY = config.get("api_keys", "openaq_api_key", fallback="")
OPENAQ_TARGET_CITY = config.get("openaq_settings", "target_city", fallback="Hanoi")
OPENAQ_TARGET_COUNTRY = config.get("openaq_settings", "target_country", fallback="VN")
OPENAQ_DATA_GRANULARITY = config.get("openaq_settings", "data_granularity", fallback="hourly")
OPENAQ_LOOKBACK_HOURS = config.getint("openaq_settings", "lookback_hours", fallback=24)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def get_config(section: str, key: str, fallback=None):
    return config.get(section, key, fallback=fallback)

def get_config_int(section: str, key: str, fallback=None):
    return config.getint(section, key, fallback=fallback)

def print_config_summary():
    """Print a summary of loaded configuration."""
    print("=" * 60)
    print("CONFIGURATION SUMMARY")
    print("=" * 60)
    print(f"Environment: {ENV}")
    print(f"Raw Folder: {RAW_FOLDER} (test: aq_raw_test, prod: aq_raw_prod)")
    print(f"Marts Folder: {CURRENT_ENV_FOLDER}")
    print(f"Database: {DATABASE_USERNAME}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}")
    print("=" * 60)