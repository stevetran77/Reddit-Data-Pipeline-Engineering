#!/usr/bin/env python3
"""
Test script to validate Athena connection and configuration.
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.athena_utils import (
    validate_athena_connection,
    list_tables,
    get_table_count,
    execute_query,
    wait_for_query_completion,
    get_query_results,
)
from utils.constants import ATHENA_DATABASE, ATHENA_OUTPUT_LOCATION


def test_athena_connection():
    """Test basic Athena connection"""
    print("[START] Testing Athena connection...")
    try:
        if validate_athena_connection():
            print(f"[OK] Athena connected to database: {ATHENA_DATABASE}")
            print(f"[OK] Query output location: {ATHENA_OUTPUT_LOCATION}")
            return True
        else:
            print(f"[FAIL] Athena database not found: {ATHENA_DATABASE}")
            return False
    except Exception as e:
        print(f"[FAIL] Athena connection error: {e}")
        return False


def test_list_tables():
    """Test listing tables from Athena"""
    print("\n[START] Listing tables in Athena database...")
    try:
        tables = list_tables()
        if tables:
            print(f"[OK] Found {len(tables)} tables:")
            for table in tables:
                print(f"     - {table}")
            return True
        else:
            print(f"[WARNING] No tables found in database '{ATHENA_DATABASE}'")
            print("[WARNING] This is normal if Glue crawler hasn't run yet")
            return True  # Not a failure - just needs crawler
    except Exception as e:
        print(f"[FAIL] Failed to list tables: {e}")
        return False


def test_sample_query():
    """Test a sample query"""
    print("\n[START] Testing sample query...")
    try:
        query = f"""
        SELECT
            table_name,
            table_schema
        FROM information_schema.tables
        WHERE table_schema = '{ATHENA_DATABASE}'
        LIMIT 5
        """

        print("[INFO] Executing query...")
        query_id = execute_query(query)

        print("[INFO] Waiting for query to complete...")
        wait_for_query_completion(query_id)

        print("[INFO] Retrieving results...")
        results = get_query_results(query_id)

        print(f"[OK] Query executed successfully")
        print(f"[OK] Retrieved {len(results)} rows")

        if results:
            print("\nResults:")
            for row in results[:5]:
                print(f"     {row}")

        return True
    except Exception as e:
        print(f"[FAIL] Query test failed: {e}")
        return False


def main():
    """Run all Athena tests"""
    print("\n" + "=" * 70)
    print("ATHENA CONNECTION TEST SUITE")
    print("=" * 70)

    results = {
        "Athena Connection": test_athena_connection(),
        "List Tables": test_list_tables(),
        "Sample Query": test_sample_query(),
    }

    print("\n" + "=" * 70)
    print("TEST RESULTS SUMMARY")
    print("=" * 70)

    passed = sum(1 for v in results.values() if v is True)
    failed = sum(1 for v in results.values() if v is False)

    for test_name, result in results.items():
        status = "[OK]" if result else "[FAIL]"
        print(f"{status} {test_name}")

    print("=" * 70)
    print(f"Passed: {passed} | Failed: {failed}")
    print("=" * 70)

    if failed == 0:
        print("\n[SUCCESS] All Athena tests passed!")
        print("\nNext steps:")
        print("1. Run Glue Crawler to catalog S3 data")
        print("2. Check Athena Query Editor for your tables")
        print("3. Connect Looker to Athena for visualization")
        return True
    else:
        print("\n[FAIL] Some tests failed. Check errors above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
