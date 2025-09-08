#!/usr/bin/env python3
"""
Test file for CommonUtils functions
Run this from the project root directory
"""
 
import sys
import os
 
# Add src to path so we can import our utils
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils.s3_utils import(
    get_s3_client,
    
) 
 
from utils.CommonUtils import (
    get_config,
    client_auth,
    read_secret,
    get_boto3_client,
    get_current_environment,
    get_current_region,
    get_aws_region
)
 
def test_config():
    """Test configuration loading"""
    print("=== Testing Configuration ===")
    try:
        config = get_config()
        print("✅ Config loaded successfully")
        print(f"Environment: {config.get('environment', 'Not found')}")
        print(f"Region: {config.get('region', 'Not found')}")
        return True
    except Exception as e:
        print(f"❌ Config loading failed: {e}")
        return False
 
def test_vault_auth():
    """Test Vault authentication"""
    print("\n=== Testing Vault Authentication ===")
    try:
        client = client_auth()
        if client:
            print("✅ Vault authentication successful")
            print(f"Vault URL: {client.url}")
            return True
        else:
            print("❌ Vault authentication failed")
            return False
    except Exception as e:
        print(f"❌ Vault authentication error: {e}")
        return False
   
def test_read_secret():
    """Test Vault secret reading"""
    print("\n=== Testing Read Secret ===")
    try:
        # Example values - adjust to match your Vault setup
        secret_path = "/1020_edb-genai/idmc"       # path under your KV
        environment = "dev"                  # environment to test
 
        secret_data = read_secret(secret_path, environment)
        if secret_data:
            print("✅ Secret fetched successfully")
            print(f"Secret Data: {secret_data}")
            return True
        else:
            print("❌ Failed to fetch secret")
            return False
    except Exception as e:
        print(f"❌ Error while reading secret: {e}")
        return False
 
 
# def test_environment_functions():
#     """Test environment and region functions"""
#     print("\n=== Testing Environment Functions ===")
#     try:
#         env = get_current_environment()
#         region = get_current_region()
#         aws_region = get_aws_region(region)
       
#         print(f"✅ Current Environment: {env}")
#         print(f"✅ Current Region: {region}")
#         print(f"✅ AWS Region: {aws_region}")
#         return True
#     except Exception as e:
#         print(f"❌ Environment functions error: {e}")
#         return False
 
### connection with aws dev
# def test_boto3_s3():
#     """Test boto3 client for S3"""
#     print("\n=== Testing Boto3 S3 Client ===")
#     try:
#         # Create boto3 client for S3
#         s3_client = get_boto3_client("s3", "dev", "us")
        
#         # Try a simple operation: list buckets
#         response = s3_client.list_buckets()
#         bucket_names = [bucket["Name"] for bucket in response["Buckets"]]
        
#         print("✅ S3 client created successfully")
#         print(f"Buckets found: {bucket_names}")
#         return True
#     except Exception as e:
#         print(f"❌ Error using boto3 S3 client: {e}")
#         return False

def test_s3_required_buckets():
    """Check if required S3 buckets exist in dev/us"""
    print("\n=== Checking Required S3 Buckets ===")
    try:
        # Get S3 client for dev + us
        s3_client = get_boto3_client("s3", "dev", "us")

        # Define required buckets
        required_buckets = [
            "tpc-aws-ted-qa-edpp-iics-bdm-mount-ap-northeast-1",
            "tpc-aws-ted-tst-iics-edpp-mount-usvga",
            "tpc-aws-ted-tst-iics-edpp-mount-defra",
            "airflow-ci-cd-dev",
        ]

        # Fetch all buckets from AWS
        response = s3_client.list_buckets()
        existing_buckets = [b["Name"] for b in response["Buckets"]]

        # Check each required bucket
        for bucket in required_buckets:
            if bucket in existing_buckets:
                print(f"✅ Bucket exists: {bucket}")
            else:
                print(f"❌ Bucket missing: {bucket}")

    except Exception as e:
        print(f"ERROR:: Unable to verify S3 buckets: {e}")


 
 
def main():
    """Run all tests"""
    print("🚀 Starting CommonUtils Testing...\n")
   
    tests = [
        test_config,
        # test_environment_functions,
        # test_vault_auth,
        # test_mwaa_configurations,  # New MWAA test
        # test_read_secret,
        # test_boto3_s3
        # test_s3_required_buckets
    ]
   
    passed = 0
    total = len(tests)
   
    for test in tests:
        if test():
            passed += 1
        print("-" * 50)
   
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
   
    if passed == total:
        print("🎉 All tests passed!")
    else:
        print("⚠️  Some tests failed. Check your configuration and credentials.")
 
if __name__ == "__main__":
    main()