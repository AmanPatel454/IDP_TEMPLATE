__author__ = "ZS Associates"

"""
s3utils.py - Clean S3 migration utilities with helper functions and orchestrator class.
"""

from typing import List, Dict, Any, Optional, Tuple
import boto3
from botocore.exceptions import ClientError

# Import from parent utils directory  
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from utils import CommonUtils




def get_s3_client(environment: str, region: str):
    """
    Get an authenticated boto3 S3 client for the given environment and region.
    """
    try:
        s3_client = CommonUtils.get_boto3_client(
            "s3",
            environment,
            region
        )
        return s3_client
    except Exception as e:
        raise Exception(f"ERROR::Failed to create S3 client: {e}")
    
def check_file_exists(environment: str, region: str, bucket_name: str, file_key: str) -> bool:
    """
    Check if a specific file exists in an S3 bucket.
    """
    try:
        s3_client = get_s3_client(environment, region)
        s3_client.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise Exception(f"ERROR::Failed to check file existence: {e}")
        

import boto3
from typing import List, Dict, Any


def prefix_exists(bucket: str, prefix: str, s3_client) -> bool:
    """
    Check if a prefix (folder path) exists in the given bucket.
    """
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return "Contents" in response


def ensure_subfolders(bucket: str, prefix: str, s3_client):
    """
    Ensure the destination subfolders exist by creating a zero-byte object with trailing '/'.
    """
    if not prefix_exists(bucket, prefix, s3_client):
        print(f"📂 Creating folder path: {prefix} in bucket {bucket}")
        s3_client.put_object(Bucket=bucket, Key=prefix)
    return prefix


def migrate_selected_files(
    source_bucket: str,
    dest_bucket: str,
    file_list: List[str],
    source_s3_client,
    dest_s3_client
) -> Dict[str, Any]:
    """
    Migrate a list of specific files using provided clients.
    Ensures that required destination subfolders exist.
    """
    try:
        print(f"📄 Migrating {len(file_list)} selected files")
        
        migrated_count = 0
        errors = []
        
        for file_key in file_list:
            try:
                print(f"  Migrating: {file_key}")
                
                # ---- Ensure destination subfolders exist ----
                prefix = "/".join(file_key.split("/")[:-1]) + "/"
                if prefix != "/":  # only ensure if not root
                    ensure_subfolders(dest_bucket, prefix, dest_s3_client)

                # Get file from source
                source_object = source_s3_client.get_object(Bucket=source_bucket, Key=file_key)

                # Upload to destination
                dest_s3_client.put_object(
                    Bucket=dest_bucket,
                    Key=file_key,
                    Body=source_object['Body'].read()
                )
                migrated_count += 1
                
            except Exception as e:
                error_msg = f"Failed to migrate '{file_key}': {e}"
                print(f"❌ {error_msg}")
                errors.append(error_msg)
        
        print(f"✅ Selected files: {migrated_count}/{len(file_list)} migrated")
        
        return {
            "status": "SUCCESS" if not errors else "PARTIAL_SUCCESS",
            "migrated_count": migrated_count,
            "total_requested": len(file_list),
            "errors": errors
        }
        
    except Exception as e:
        raise Exception(f"ERROR::Critical error in selected files migration: {e}")


def migrate_all_files(
    source_bucket: str,
    dest_bucket: str,
    folder_prefix: str,
    source_s3_client,
    dest_s3_client
) -> Dict[str, Any]:
    """
    Migrate all files under a given folder prefix.
    If folder does not exist in destination, it will be created.
    The folder structure will be preserved.
    """
    try:
        print(f"📂 Starting migration for all files under prefix: {folder_prefix}")

        # Ensure prefix ends with "/"
        if not folder_prefix.endswith("/"):
            folder_prefix = folder_prefix + "/"

        # List all objects in the given prefix
        paginator = source_s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=source_bucket, Prefix=folder_prefix)

        file_keys = []
        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    file_keys.append(obj["Key"])

        if not file_keys:
            print(f"⚠️ No files found under prefix: {folder_prefix}")
            return {
                "status": "NO_FILES",
                "migrated_count": 0,
                "total_requested": 0,
                "errors": []
            }

        print(f"📄 Found {len(file_keys)} files under {folder_prefix}")

        # Call migrate_selected_files to handle migration
        result = migrate_selected_files(
            source_bucket,
            dest_bucket,
            file_keys,
            source_s3_client,
            dest_s3_client
        )

        print(f"✅ Migration completed for prefix: {folder_prefix}")
        return result

    except Exception as e:
        raise Exception(f"ERROR::Critical error in migrate_all_files: {e}")


# def main():
#     # Example source & destination clients (adjust profiles/regions accordingly)
#     source_s3_client = get_s3_client("dev", "us")
#     dest_s3_client = get_s3_client("tst", "us")

#     source_bucket = "tpc-aws-ted-dev-edpp-bdm-mount-us-east-1"
#     dest_bucket = "tpc-aws-ted-qa-edpp-iics-bdm-mount-us-east-1"
    
#     # Example file keys to migrate
#     file_list = [
#         "IICS/cicd_sdsdtesting/ParamFiles/Recordings.docx",
#         "IICS/cicd_tedssdsting/Scripts/IDMC CICD Architecture Diagram.png"
#         ]
        
#         result = migrate_selected_files(
#             source_bucket,
#             dest_bucket,
#             file_list,
#             source_s3_client,
#             dest_s3_client
#         )
        
#         print("\n📊 Migration Summary:")
#         print(result)

def main():
    # Read Harness pipeline variables
    migration_type = os.getenv("MIGRATION_TYPE", "").lower()   # "all" or "selective"
    source_bucket = os.getenv("SOURCE_BUCKET")
    dest_bucket = os.getenv("DEST_BUCKET")
    source_env = os.getenv("SOURCE_ENV", "dev")
    target_env = os.getenv("TARGET_ENV", "tst")
    region = os.getenv("REGION", "us")

    source_s3_client = get_s3_client(source_env, region)
    dest_s3_client = get_s3_client(target_env, region)

    if migration_type == "all":
        # Expect a pipeline variable MIGRATION_FOLDER
        folder_prefix = os.getenv("MIGRATION_FOLDER")
        if not folder_prefix:
            print("❌ Error: MIGRATION_FOLDER variable is required for 'all' migration")
            sys.exit(1)

        print(f"🚀 Running FULL migration for folder: {folder_prefix}")
        result = migrate_all_files(
            source_bucket,
            dest_bucket,
            folder_prefix,
            source_s3_client,
            dest_s3_client
        )

    elif migration_type == "selective":
        # Expect MIGRATION_FILES as comma-separated list
        files_input = os.getenv("MIGRATION_FILES", "")
        if not files_input:
            print("❌ Error: MIGRATION_FILES variable is required for 'selective' migration")
            sys.exit(1)

        file_list = [f.strip() for f in files_input.split(",") if f.strip()]
        print(f"🚀 Running SELECTIVE migration for {len(file_list)} files")
        result = migrate_selected_files(
            source_bucket,
            dest_bucket,
            file_list,
            source_s3_client,
            dest_s3_client
        )

    else:
        print(f"❌ Error: Unknown MIGRATION_TYPE '{migration_type}'. Must be 'all' or 'selective'.")
        sys.exit(1)

    print("\n📊 Migration Summary:")
    print(result)


if __name__ == "__main__":
    main()





