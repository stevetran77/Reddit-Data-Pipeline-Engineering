import sys
sys.path.insert(0, '../')

from utils.constants import print_config_summary
from utils.aws_utils import check_s3_connection
from etls.reddit_etl import connect_reddit
from utils.constants import REDDIT_CLIENT_ID, REDDIT_SECRET_KEY


def test_configuration():
    """Test configuration loading."""
    print("\n=== Testing Configuration ===")
    print_config_summary()


def test_reddit_connection():
    """Test Reddit API connection."""
    print("\n=== Testing Reddit Connection ===")
    try:
        reddit = connect_reddit(
            REDDIT_CLIENT_ID,
            REDDIT_SECRET_KEY,
            'Test Agent'
        )
        print("[OK] Reddit connection successful")
        return True
    except Exception as e:
        print(f"[FAIL] Reddit connection failed: {e}")
        return False


def test_s3_connection():
    """Test AWS S3 connection."""
    print("\n=== Testing S3 Connection ===")
    return check_s3_connection()


def test_mini_pipeline():
    """Run mini pipeline with 5 posts."""
    print("\n=== Testing Mini Pipeline ===")
    from pipelines.reddit_pipelines import reddit_pipeline

    try:
        reddit_pipeline(
            file_name='test_run',
            subreddit='python',
            time_filter='day',
            limit=5
        )
        print("[OK] Mini pipeline test passed")
        return True
    except Exception as e:
        print(f"[FAIL] Mini pipeline failed: {e}")
        return False


if __name__ == '__main__':
    test_configuration()
    test_reddit_connection()
    test_s3_connection()
    test_mini_pipeline()
