import configparser
import os
from pathlib import Path

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_FILE = PROJECT_ROOT / "config" / "config.conf"

# Initialize ConfigParser
config = configparser.ConfigParser()

# Load configuration file
if CONFIG_FILE.exists():
    config.read(CONFIG_FILE)
else:
    raise FileNotFoundError(f"Configuration file not found: {CONFIG_FILE}")


# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================
DATABASE_HOST = config.get("database", "database_host", fallback="localhost")
DATABASE_PORT = config.getint("database", "database_port", fallback=5432)
DATABASE_NAME = config.get("database", "database_name", fallback="airflow_reddit")
DATABASE_USERNAME = config.get("database", "database_username", fallback="postgres")
DATABASE_PASSWORD = config.get("database", "database_password", fallback="postgres")

# Database connection string for SQLAlchemy
DATABASE_URL = f"postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"


# ============================================================================
# FILE PATHS
# ============================================================================
INPUT_PATH = config.get("file_paths", "input_path", fallback="/opt/airflow/data/input")
OUTPUT_PATH = config.get("file_paths", "output_path", fallback="/opt/airflow/data/output")

# Create directories if they don't exist
os.makedirs(INPUT_PATH, exist_ok=True)
os.makedirs(OUTPUT_PATH, exist_ok=True)


# ============================================================================
# AWS CONFIGURATION
# ============================================================================
AWS_ACCESS_KEY_ID = config.get("aws", "aws_access_key_id", fallback="")
AWS_SECRET_ACCESS_KEY = config.get("aws", "aws_secret_access_key", fallback="")
AWS_SESSION_TOKEN = config.get("aws", "aws_session_token", fallback="")
AWS_REGION = config.get("aws", "aws_region", fallback="us-east-1")
AWS_BUCKET_NAME = config.get("aws", "aws_bucket_name", fallback="")


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

# OpenAQ Settings
OPENAQ_TARGET_CITY = config.get("openaq_settings", "target_city", fallback="Hanoi")
OPENAQ_TARGET_COUNTRY = config.get("openaq_settings", "target_country", fallback="VN")
OPENAQ_DATA_GRANULARITY = config.get("openaq_settings", "data_granularity", fallback="hourly")
OPENAQ_LOOKBACK_HOURS = config.getint("openaq_settings", "lookback_hours", fallback=24)


# ============================================================================
# HELPER FUNCTION
# ============================================================================
def get_config(section: str, key: str, fallback=None):
    """
    Get a configuration value from the config file.

    Args:
        section (str): The section name in the config file
        key (str): The key name within the section
        fallback: Default value if key is not found

    Returns:
        The configuration value or fallback
    """
    return config.get(section, key, fallback=fallback)


def get_config_int(section: str, key: str, fallback=None):
    """
    Get an integer configuration value from the config file.

    Args:
        section (str): The section name in the config file
        key (str): The key name within the section
        fallback: Default value if key is not found

    Returns:
        The configuration value as integer or fallback
    """
    return config.getint(section, key, fallback=fallback)


def print_config_summary():
    """Print a summary of loaded configuration (excluding sensitive data)."""
    print("=" * 60)
    print("CONFIGURATION SUMMARY")
    print("=" * 60)
    print(f"Database: {DATABASE_USERNAME}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}")
    print(f"Input Path: {INPUT_PATH}")
    print(f"Output Path: {OUTPUT_PATH}")
    print(f"Batch Size: {BATCH_SIZE}")
    print(f"Error Handling: {ERROR_HANDLING}")
    print(f"Log Level: {LOG_LEVEL}")
    print("=" * 60)
