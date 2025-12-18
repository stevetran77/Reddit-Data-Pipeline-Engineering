#!/usr/bin/env python3
"""
Test script to validate all connections in config.conf
Tests: PostgreSQL, AWS S3, AWS Glue, Redshift
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.constants import *


def test_config_loaded():
    """Test if config file is loaded successfully"""
    print("[START] Testing configuration loading...")
    try:
        print(f"[OK] Config file loaded from: {PROJECT_ROOT / 'config' / 'config.conf'}")
        print(f"[OK] AWS Region: {AWS_REGION}")
        print(f"[OK] S3 Bucket: {AWS_BUCKET_NAME}")
        return True
    except Exception as e:
        print(f"[FAIL] Config loading failed: {e}")
        return False


def test_postgresql():
    """Test PostgreSQL connection"""
    print("\n[START] Testing PostgreSQL connection...")
    try:
        from sqlalchemy import create_engine
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute("SELECT version();")
            version = result.fetchone()[0]
            print(f"[OK] PostgreSQL connected")
            print(f"[OK] Version: {version[:50]}...")
            return True
    except Exception as e:
        print(f"[FAIL] PostgreSQL connection failed: {e}")
        return False


def test_s3_connection():
    """Test AWS S3 connection"""
    print("\n[START] Testing AWS S3 connection...")
    try:
        import boto3
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN if AWS_SESSION_TOKEN else None
        )
        # Try to list buckets
        response = s3_client.list_buckets()
        buckets = [b['Name'] for b in response['Buckets']]
        print(f"[OK] AWS S3 connected (found {len(buckets)} buckets)")

        # Check if our bucket exists
        if AWS_BUCKET_NAME in buckets:
            print(f"[OK] Bucket '{AWS_BUCKET_NAME}' exists")
            return True
        else:
            print(f"[FAIL] Bucket '{AWS_BUCKET_NAME}' not found in account")
            print(f"Available buckets: {buckets}")
            return False
    except Exception as e:
        print(f"[FAIL] AWS S3 connection failed: {e}")
        return False


def test_glue_connection():
    """Test AWS Glue connection"""
    print("\n[START] Testing AWS Glue connection...")
    try:
        import boto3
        glue_client = boto3.client(
            'glue',
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN if AWS_SESSION_TOKEN else None
        )
        # Try to list databases
        response = glue_client.get_databases()
        databases = [d['Name'] for d in response['DatabaseList']]
        print(f"[OK] AWS Glue connected (found {len(databases)} databases)")

        # Check if our database exists
        if GLUE_DATABASE_NAME in databases:
            print(f"[OK] Glue database '{GLUE_DATABASE_NAME}' exists")
            return True
        else:
            print(f"[FAIL] Glue database '{GLUE_DATABASE_NAME}' not found")
            print(f"Available databases: {databases}")
            return False
    except Exception as e:
        print(f"[FAIL] AWS Glue connection failed: {e}")
        return False


def test_redshift_connection():
    """Test Redshift connection"""
    print("\n[START] Testing Redshift connection...")

    if not REDSHIFT_HOST or REDSHIFT_HOST == "your-cluster.region.redshift.amazonaws.com":
        print(f"[WARNING] Redshift host not configured: {REDSHIFT_HOST}")
        print("[WARNING] Skipping Redshift test (configure redshift_host in config.conf)")
        return None

    try:
        import redshift_connector
        conn = redshift_connector.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            database=REDSHIFT_DATABASE,
            user=REDSHIFT_USERNAME,
            password=REDSHIFT_PASSWORD,
            timeout=5
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"[OK] Redshift connected")
        print(f"[OK] Version: {version[:50]}...")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"[FAIL] Redshift connection failed: {e}")
        return False


def test_openaq_api():
    """Test OpenAQ API connection"""
    print("\n[START] Testing OpenAQ API connection...")
    try:
        import requests
        # Simple test - just check API is accessible
        url = "https://api.openaq.org/v2/locations"
        params = {"city": OPENAQ_TARGET_CITY, "limit": 1}
        response = requests.get(url, params=params, timeout=5)

        if response.status_code == 200:
            print(f"[OK] OpenAQ API connected")
            data = response.json()
            if data.get('results'):
                print(f"[OK] Found {data['meta']['found']} locations in {OPENAQ_TARGET_CITY}")
            return True
        else:
            print(f"[FAIL] OpenAQ API returned status {response.status_code}")
            return False
    except Exception as e:
        print(f"[FAIL] OpenAQ API connection failed: {e}")
        return False


def main():
    """Run all connection tests"""
    print("\n" + "=" * 70)
    print("CONNECTION TEST SUITE")
    print("=" * 70)

    results = {
        "Config Loading": test_config_loaded(),
        "PostgreSQL": test_postgresql(),
        "AWS S3": test_s3_connection(),
        "AWS Glue": test_glue_connection(),
        "Redshift": test_redshift_connection(),
        "OpenAQ API": test_openaq_api(),
    }

    print("\n" + "=" * 70)
    print("TEST RESULTS SUMMARY")
    print("=" * 70)

    passed = sum(1 for v in results.values() if v is True)
    failed = sum(1 for v in results.values() if v is False)
    skipped = sum(1 for v in results.values() if v is None)

    for test_name, result in results.items():
        status = "[OK]" if result is True else "[FAIL]" if result is False else "[SKIP]"
        print(f"{status} {test_name}")

    print("=" * 70)
    print(f"Passed: {passed} | Failed: {failed} | Skipped: {skipped}")
    print("=" * 70)

    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
