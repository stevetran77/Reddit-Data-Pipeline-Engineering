import sys
sys.path.insert(0, '../')

from utils.constants import print_config_summary
from utils.aws_utils import check_s3_connection
from etls.openaq_etl import connect_openaq
from utils.constants import OPENAQ_API_KEY


def test_configuration():
    """Test configuration loading."""
    print("\n=== Testing Configuration ===")
    print_config_summary()


def test_openaq_connection():
    """Test OpenAQ API connection."""
    print("\n=== Testing OpenAQ Connection ===")
    try:
        client = connect_openaq(OPENAQ_API_KEY)
        print("[OK] OpenAQ connection successful")
        client.close()
        return True
    except Exception as e:
        print(f"[FAIL] OpenAQ connection failed: {e}")
        return False


def test_s3_connection():
    """Test AWS S3 connection."""
    print("\n=== Testing S3 Connection ===")
    return check_s3_connection()


def test_mini_pipeline():
    """Run mini pipeline with 2 hours lookback."""
    print("\n=== Testing Mini Pipeline ===")
    from pipelines.openaq_pipeline import openaq_pipeline

    try:
        openaq_pipeline(
            file_name='test_run',
            city='Hanoi',
            country='VN',
            lookback_hours=2
        )
        print("[OK] Mini pipeline test passed")
        return True
    except Exception as e:
        print(f"[FAIL] Mini pipeline failed: {e}")
        return False


if __name__ == '__main__':
    test_configuration()
    test_openaq_connection()
    test_s3_connection()
    test_mini_pipeline()
