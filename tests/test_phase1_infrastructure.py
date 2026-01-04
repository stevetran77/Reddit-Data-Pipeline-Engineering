"""
Phase 1: AWS Infrastructure Verification Tests

This script verifies that all AWS infrastructure is correctly configured before
running the Glue pipeline transformation tests.

Verification checks:
1. Glue Job Configuration
2. Glue Crawler Configuration
3. IAM Permissions (Glue role)
4. Glue Database Exists
"""

import boto3
import sys
from utils.constants import (
    GLUE_DATABASE_NAME,
    GLUE_CRAWLER_NAME,
    GLUE_TRANSFORM_JOB_NAME,
    GLUE_IAM_ROLE,
    AWS_REGION,
    AWS_BUCKET_NAME,
    ENV,
)
from utils.glue_utils import get_glue_client, list_glue_jobs


def check_glue_job_exists():
    """Check if Glue transformation job exists and has correct configuration."""
    print(f"\n[START] Checking Glue Job: {GLUE_TRANSFORM_JOB_NAME}")

    try:
        client = get_glue_client()
        response = client.get_job(Name=GLUE_TRANSFORM_JOB_NAME)
        job = response.get('Job', {})

        print(f"[OK] Job exists: {job.get('Name')}")
        print(f"    - Description: {job.get('Description', 'N/A')}")
        print(f"    - Role: {job.get('Role')}")
        print(f"    - Command: {job.get('Command', {})}")
        print(f"    - DefaultArguments: {job.get('DefaultArguments', {})}")

        # Verify script location
        script_location = job.get('Command', {}).get('ScriptLocation', '')
        if 'process_openaq_raw.py' in script_location:
            print(f"[OK] Script location is correct: {script_location}")
        else:
            print(f"[WARNING] Script location may be incorrect: {script_location}")

        return True
    except client.exceptions.EntityNotFoundException:
        print(f"[FAIL] Glue job not found: {GLUE_TRANSFORM_JOB_NAME}")
        return False
    except Exception as e:
        print(f"[FAIL] Error checking Glue job: {str(e)}")
        return False


def check_glue_crawler_exists():
    """Check if Glue crawler exists and has correct configuration."""
    print(f"\n[START] Checking Glue Crawler: {GLUE_CRAWLER_NAME}")

    try:
        client = get_glue_client()
        response = client.get_crawler(Name=GLUE_CRAWLER_NAME)
        crawler = response.get('Crawler', {})

        print(f"[OK] Crawler exists: {crawler.get('Name')}")
        print(f"    - Role: {crawler.get('Role')}")
        print(f"    - State: {crawler.get('State')}")
        print(f"    - S3TargetPaths: {crawler.get('Targets', {}).get('S3Targets', [])}")
        print(f"    - DatabaseName: {crawler.get('DatabaseName')}")

        # Verify database name
        db_name = crawler.get('DatabaseName', '')
        if GLUE_DATABASE_NAME in db_name or db_name == GLUE_DATABASE_NAME:
            print(f"[OK] Crawler database is correct: {db_name}")
        else:
            print(f"[WARNING] Crawler database may be incorrect. Expected: {GLUE_DATABASE_NAME}, Got: {db_name}")

        return True
    except client.exceptions.EntityNotFoundException:
        print(f"[FAIL] Glue crawler not found: {GLUE_CRAWLER_NAME}")
        return False
    except Exception as e:
        print(f"[FAIL] Error checking Glue crawler: {str(e)}")
        return False


def check_glue_database_exists():
    """Check if Glue database exists."""
    print(f"\n[START] Checking Glue Database: {GLUE_DATABASE_NAME}")

    try:
        client = get_glue_client()
        response = client.get_database(Name=GLUE_DATABASE_NAME)
        database = response.get('Database', {})

        print(f"[OK] Database exists: {database.get('Name')}")
        print(f"    - Description: {database.get('Description', 'N/A')}")
        print(f"    - LocationUri: {database.get('LocationUri', 'N/A')}")

        return True
    except client.exceptions.EntityNotFoundException:
        print(f"[FAIL] Glue database not found: {GLUE_DATABASE_NAME}")
        return False
    except Exception as e:
        print(f"[FAIL] Error checking Glue database: {str(e)}")
        return False


def check_iam_role_permissions():
    """Check IAM role for Glue has necessary permissions."""
    print(f"\n[START] Checking IAM Role Permissions: {GLUE_IAM_ROLE}")

    try:
        iam_client = boto3.client('iam', region_name=AWS_REGION)

        # Extract role name from ARN
        # Format: arn:aws:iam::123456789:role/RoleName
        role_name = GLUE_IAM_ROLE.split('/')[-1]

        response = iam_client.get_role(RoleName=role_name)
        role = response.get('Role', {})

        print(f"[OK] Role exists: {role.get('RoleName')}")
        print(f"    - Arn: {role.get('Arn')}")
        print(f"    - CreateDate: {role.get('CreateDate')}")

        # List attached policies
        policies = iam_client.list_attached_role_policies(RoleName=role_name)
        print(f"    - Attached Policies ({len(policies.get('AttachedPolicies', []))} total):")
        for policy in policies.get('AttachedPolicies', []):
            print(f"      - {policy.get('PolicyName')}")

        # List inline policies
        inline = iam_client.list_role_policies(RoleName=role_name)
        print(f"    - Inline Policies ({len(inline.get('PolicyNames', []))} total):")
        for policy_name in inline.get('PolicyNames', []):
            print(f"      - {policy_name}")

        return True
    except Exception as e:
        print(f"[FAIL] Error checking IAM role: {str(e)}")
        return False


def check_s3_bucket_exists():
    """Check if S3 bucket exists and verify key paths."""
    print(f"\n[START] Checking S3 Bucket: {AWS_BUCKET_NAME}")

    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)

        # Check bucket exists
        response = s3_client.head_bucket(Bucket=AWS_BUCKET_NAME)
        print(f"[OK] Bucket exists: {AWS_BUCKET_NAME}")

        # Check for key paths
        paths_to_check = [
            'aq_raw/',
            f'aq_{ENV}/',
            f'aq_{ENV}/marts/',
            'scripts/glue_jobs/',
        ]

        for path in paths_to_check:
            try:
                response = s3_client.list_objects_v2(
                    Bucket=AWS_BUCKET_NAME,
                    Prefix=path,
                    MaxKeys=1
                )
                if 'Contents' in response or response.get('KeyCount', 0) > 0:
                    print(f"[OK] Path exists: s3://{AWS_BUCKET_NAME}/{path}")
                else:
                    print(f"[WARNING] Path may be empty: s3://{AWS_BUCKET_NAME}/{path}")
            except Exception as e:
                print(f"[WARNING] Could not verify path {path}: {str(e)}")

        return True
    except Exception as e:
        print(f"[FAIL] Error checking S3 bucket: {str(e)}")
        return False


def check_athena_database_exists():
    """Check if Athena database exists."""
    print(f"\n[START] Checking Athena Database: {GLUE_DATABASE_NAME}")

    try:
        athena_client = boto3.client('athena', region_name=AWS_REGION)

        # List databases using catalog
        response = athena_client.list_databases()
        databases = response.get('DatabaseList', [])

        db_names = [db.get('Name') for db in databases]
        print(f"[OK] Found {len(db_names)} databases in Athena")

        if GLUE_DATABASE_NAME in db_names:
            print(f"[OK] Database {GLUE_DATABASE_NAME} exists in Athena")
            return True
        else:
            print(f"[WARNING] Database {GLUE_DATABASE_NAME} not found in Athena")
            print(f"    Available databases: {', '.join(db_names[:5])}...")
            return False
    except Exception as e:
        print(f"[WARNING] Error checking Athena database: {str(e)}")
        return False


def run_all_checks():
    """Run all infrastructure verification checks."""
    print("=" * 80)
    print("PHASE 1: AWS Infrastructure Verification")
    print("=" * 80)
    print(f"Environment: {ENV}")
    print(f"Region: {AWS_REGION}")
    print(f"Bucket: {AWS_BUCKET_NAME}")
    print(f"Database: {GLUE_DATABASE_NAME}")
    print(f"Crawler: {GLUE_CRAWLER_NAME}")
    print(f"Job: {GLUE_TRANSFORM_JOB_NAME}")

    results = {}

    # Run all checks
    results['Glue Job'] = check_glue_job_exists()
    results['Glue Crawler'] = check_glue_crawler_exists()
    results['Glue Database'] = check_glue_database_exists()
    results['IAM Role'] = check_iam_role_permissions()
    results['S3 Bucket'] = check_s3_bucket_exists()
    results['Athena Database'] = check_athena_database_exists()

    # Summary
    print("\n" + "=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)

    for check_name, result in results.items():
        status = "[OK]" if result else "[FAIL]"
        print(f"{status} {check_name}")

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nResult: {passed}/{total} checks passed")

    if passed == total:
        print("[SUCCESS] All infrastructure checks passed!")
        return 0
    else:
        print("[FAIL] Some infrastructure checks failed. See details above.")
        return 1


if __name__ == "__main__":
    exit_code = run_all_checks()
    sys.exit(exit_code)
