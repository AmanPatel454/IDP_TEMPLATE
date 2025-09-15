#!/usr/bin/python3
# -*- coding: utf-8 -*-
__author__ = "ZS Associates"

"""
CommonUtils.py - Essential Common Utilities for Vault and AWS
Tech Description: Core utilities for Vault authentication and AWS operations
Pre_requisites: Requires CommonUtilsConstants.py and config.json
"""

import boto3
import hvac
import re
from typing import Dict, Any, Tuple, Union

# Import constants
from . import CommonUtilsConstants


def get_config() -> Dict[str, Any]:
    """Load and return configuration from JSON file"""
    try:
        with open(CommonUtilsConstants.CONFIG_FILE_PATH) as config_file:
            return CommonUtilsConstants.json.load(config_file)
    except Exception as e:
        raise Exception(f"ERROR::Unable to fetch configs: {str(e)}")

def get_mapping_config() -> Dict[str, Any]:
    """Load and return configuration from JSON file"""
    try:
        with open(CommonUtilsConstants.MAPPING_CONFIG_FILE_PATH) as config_file:
            return CommonUtilsConstants.json.load(config_file)
    except Exception as e:
        raise Exception(f"ERROR::Unable to fetch configs: {str(e)}")

def client_auth():
    """Authenticate vault client using AppRole"""
    try:
        config = get_config()
        
        client = hvac.Client(url=config["URL_KEY"],
                             namespace=config["NAMESPACE_KEY"])
        
        auth_response = client.auth.approle.login(
            role_id=config["ROLE_ID_KEY"],
            secret_id=config["SECRET_ID_KEY"],
        )
        
        token = auth_response[CommonUtilsConstants.AUTH_KEY][CommonUtilsConstants.CLIENT_TOKEN_KEY]
        client.token = token
        
        print("Authentication successful")
        return client
    except Exception as e:
        print("Error occurred while authenticating the client:", e)
        return False


def get_secret_engine(environment: str) -> str:
    """Get secret engine based on environment"""
    try:
        config = get_config()
        env_lower = environment.lower()
        
        if env_lower in CommonUtilsConstants.DEV_ENV_CHK:
            return config["DEV_SECRET_ENGINE"]
        elif env_lower in CommonUtilsConstants.TST_ENV_CHK:
            return config["TST_SECRET_ENGINE"]
        elif env_lower in CommonUtilsConstants.PRD_ENV_CHK:
            return config["PRD_SECRET_ENGINE"]
        else:
            raise Exception(f"Invalid environment selection for vault secret engine: {environment}")
    except Exception as ex:
        raise Exception(f"Unable to get secret engine: {str(ex)}")


def read_secret(path: str, environment: str) -> Union[Dict[str, Any], bool]:
    """Read secret from vault"""
    try:
        client = client_auth()
        if not client:
            return False
            
        mount_point = get_secret_engine(environment)
        secret_response = client.secrets.kv.v2.read_secret_version(
            path=path,
            mount_point=mount_point
        )
        
        return secret_response[CommonUtilsConstants.DATA_KEY][CommonUtilsConstants.DATA_KEY]
        
    except Exception as e:
        print(f"ERROR::Error fetching secrets from vault: {e}")
        return False


def get_aws_region(region: str) -> str:
    """Get AWS region from region code"""
    try:
        region = region.strip().upper()
        region_map = {
            CommonUtilsConstants.US_KEY: CommonUtilsConstants.US_REGION,
            CommonUtilsConstants.EU_KEY: CommonUtilsConstants.EU_REGION,
            CommonUtilsConstants.JP_KEY: CommonUtilsConstants.JP_REGION
        }
        
        if region not in region_map:
            raise Exception(f"Invalid region: {region}")
            
        return region_map[region]
        
    except Exception as ex:
        raise Exception(f"ERROR::Unable to get aws region: {str(ex)}")


def assume_cross_account_role(environment: str) -> Tuple[str, str, str]:
    """Assume cross-account role and return credentials"""
    try:
        config = get_config()
        
        # Get role ARN based on environment using your constants
        if environment == CommonUtilsConstants.DEV_ENV_KEY:
            role_arn = config[CommonUtilsConstants.DEV_ENV_ARN]
        elif environment == CommonUtilsConstants.TST_ENV_KEY:
            role_arn = config[CommonUtilsConstants.TST_ENV_ARN]
        elif environment == CommonUtilsConstants.PRD_ENV_KEY:
            role_arn = config[CommonUtilsConstants.PRD_ENV_ARN]
        else:
            raise Exception(f"Invalid environment selection while assume role: {environment}")
                
        # Get credentials from Vault
        client = client_auth()

        response = client.secrets.aws.generate_credentials(
            name=CommonUtilsConstants.KEY_ASSUME_ROLE,
            role_arn=role_arn
        )
        aws_access_key = response[CommonUtilsConstants.DATA_KEY][CommonUtilsConstants.ACCESS_KEY]
        aws_secret_key = response[CommonUtilsConstants.DATA_KEY][CommonUtilsConstants.SECRET_KEY]
        aws_session_token = response[CommonUtilsConstants.DATA_KEY][CommonUtilsConstants.SECURITY_TOKEN_KEY]
        
        return aws_access_key, aws_secret_key, aws_session_token
        
    except Exception as ex:
        raise Exception(f"ERROR::Unable to assume cross account role: {str(ex)}")


def get_boto3_client(resource: str, environment: str, region: str):
    """Get boto3 client with assumed role credentials"""
    try:
        region = get_aws_region(region)
        access_key, secret_key, session_token = assume_cross_account_role(environment)
        
        aws_client = boto3.client(
            resource,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name=region
        )
        
        return aws_client
        
    except Exception as ex:
        raise Exception(f"ERROR::Unable to create boto3 client: {str(ex)}")


def get_buckets(agent_name, env):
    """
    Returns (source_bucket, dest_bucket) based on environment.
    """
    
    mapping = CommonUtils.get_mapping_config()
    run_env = re.search(r"\{\s*'([^']+)'", agent_name)

    agent_info = mapping["agent_mapping"].get(agent_name)
    if not agent_info:
        raise ValueError(f"No agent found for {agent_name}")

    if env.lower() == "dev":
        source_bucket = agent_info.get("Dev_Bucket")
        dest_bucket = agent_info.get("Test_Bucket")
    elif env.lower() == "test":
        source_bucket = agent_info.get("Test_Bucket")
        dest_bucket = agent_info.get("Prod_Bucket")
    else:
        raise ValueError("env must be 'dev' or 'test'")

    return source_bucket, dest_bucket