import json
import os

CONFIG_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "configs", "config.json")

AUTH_KEY = "auth"
CLIENT_TOKEN_KEY = "client_token"
DATA_KEY = "data"

ENVIRONMENT_KEY = "environment"
REGION_KEY = "region"
TST_ENV_KEY = "tst"
PRD_ENV_KEY = "prd"
DEV_ENV_KEY = "dev"
TST_ENV_ARN = "tst_environment_role_arn"
PRD_ENV_ARN = "prd_environment_role_arn"
DEV_ENV_ARN = "dev_environment_role_arn"
DEV_AWS_ACCOUNTID = "dev_account_id"
TST_AWS_ACCOUNTID = "tst_account_id"
PRD_AWS_ACCOUNTID = "prd_account_id"
TEC_DEV_AWS_ACCOUNTID = "tec_dev_account_id"
TEC_TST_AWS_ACCOUNTID = "tec_tst_account_id"
TEC_PRD_AWS_ACCOUNTID = "tec_prd_account_id"
DEV_LOGS_S3_BUCKET = "dev_logs_s3_bucket"
TST_LOGS_S3_BUCKET = "tst_logs_s3_bucket"
PRD_LOGS_S3_BUCKET = "prd_logs_s3_bucket"
STS_KEY = "sts"
SESSION_NAME = "cross_account_session"
CREDENTIALS_KEY = "Credentials"
ACCESS_ID_KEY = "AccessKeyId"
SECRET_ACCESS_KEY_KEY = "SecretAccessKey"
SESSION_TOKEN_KEY = "SessionToken"

US_KEY = "US"
JP_KEY = "JP"
EU_KEY = "EU"

US_REGION = "us-east-1"
EU_REGION = "eu-central-1"
JP_REGION = "ap-northeast-1"
KEY_KEY = "Key"
VALUE_KEY = "Value"
KEY_KEY_L = "key"
VALUE_KEY_L = "value"
APMS_ID_KEY = "apms-id"
APPLICATION_NAME_KEY = "application-name"
APPLICATION_OWNER_KEY = "application-owner"
BUSINESS_CRITICALITY_KEY = "business-criticality"
BUSINESS_CRITICALITY_VALUE = "high"
BUSINESS_UNIT_KEY = "business-unit-n1"
DATA_CLASSIFICATION_KEY = "data-classification"
EDB_ID_KEY = "edb-id"
ENVIRONMENT_ID_KEY = "environment-id"
REGION_ID_KEY = "region-id"
IT_TECHNICAL_OWNER_KEY = "it-technical-owner"
IT_BUSINESS_OWNER_KEY = "it-business-owner"
VERSION_KEY = "version"
VERSION_VALUE = "2022-03-30"
S3_VERSIONING_ENABLED = "s3_versioning_enabled"
INTELLIGENT_TIER_ENABLED = "s3_intelligent_tier_enabled"
Y_KEY = "Y"
N_KEY = "N"

DEV_MY_ACCESS_BASE_URL = "dev_my_access_api_base_url"
TST_MY_ACCESS_BASE_URL = "tst_my_access_api_base_url"
PRD_MY_ACCESS_BASE_URL = "prd_my_access_api_base_url"
CLIENT_ID = "client_id"
CLIENT_SECRET = "client_secret"

APMS_ID_VALIDATION = "APMS-"
BSN_ID_VALIDATION = "BSN"
VALID_ENVIRONMENTS = ["dev", "tst", "prd"]
VALID_REGIONS = ["us", "eu", "jp"]
EMAIL_VALIDATION = "@takeda.com"
SNS_KEY = "sns"
S3_KEY = "s3"
KMS_KEY = "kms"
TAG_KEY_KEY = "TagKey"
TAG_VALUE_KEY = "TagValue"
ACCOUNT_ID_KEY = "account_id"
KMS_ALIAS_NAME = "alias/automation-project-name-kms"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
ALIASES_KEY = "Aliases"
ALIAS_NAME_KEY = "AliasName"
TARGET_KEY_ID_KEY = "TargetKeyId"
KMS_DESCRIPTION = ("This key is generated through automation for encrypting the bucket for the project - project-"
                   "name")
ENCRYPT_DECRYPT_KEY = "ENCRYPT_DECRYPT"
AWS_KMS_KEY = "AWS_KMS"
KEY_METADATA_KEY = "KeyMetadata"
KEY_ID_KEY = "KeyId"
KEY_ARN = "Arn"
PROJECT_NAME_LENGTH = 12
PROJECT_DESCRIPTION_LENGTH = 500
VALID_BU_LIST = "USBU, OBU, PDT, RnD, GMS, GQ, JP, GPD, CORP, GDDT, GIA, GMA, Canada, Finance, GEM, GPLS, OBU, GOBU, HR, JPBU, EUCAN"
VALID_DATA_CLASSIFICATION_LIST = "confidential, protected, public"
EDB_ID_COL_KEY = "edb_id"
APMS_ID_COL_KEY = "apms_id"
IDMC_CONNECTION_COUNT_COL_KEY = "idmc_connection_count"
COUNTER_NUM_COL_KEY = "counter_num"
COUNTER_TYPE_COL_KEY = "counter_type"
REQUEST_INSERT_DT_KEY = "request_insert_dt"
REQUEST_CREATION_DT_COL_KEY = "request_creation_dt"
INFRA_REQUESTED_COL_KEY = "infra_requested"
OKTA_GROUP_COL_KEY = "okta_group"
OKTA_GROUP_NAME_COL_KEY = "group_name"
EXISTING_SCHEMA_COL_KEY = "existing_schema"
CLUSTER_COL_KEY = "cluster"
WAREHOUSE_COL_KEY = "warehouse"
WORKSPACE_FOLDER_COL_KEY = "workspace_folder"
SERVICE_PRINCIPAL_COL_KEY = "service_principal"
DATABRICKS_OKTA_GROUPS_COL_KEY = "databricks_okta_groups"
PROJECT_NAME_COL_KEY = "project_name"
DATABRICKS_KEY = "databricks"
BACKEND_IDMC_METADATA_TABLE_NAME_KEY = "backend_idmc_metadata_table_name"
DATABRICKS_BACKEND_TABLE_AUTH_KEY = "databricks_backend_table_auth"
DATABRICKS_URL_KEY = "databricks_url"
DATABRICKS_SQLWH_HTTP_PATH_KEY = "databricks_sqlwh_http_path"
# BACKEND_METADATA_TABLE_NAME_OLD = "hive_metastore.com_us_lake.edb_data_platform_launchpad_metadata"
# BACKEND_METADATA_TABLE_NAME = "dbx_uc_usdev.ent_us_raw_dlp.edb_data_launchpad_metadata_ext"
# BACKEND_COUNTER_METADATA_TABLE_NAME = "dbx_uc_usdev.ent_us_raw_dlp.edb_data_launchpad_counter_metadata_ext"
PROJECT_ID_KEY = "project-id"
PROJECT_NAME_KEY = "project-name"
ALL_ENVIRONMENT_KEY = "all_environment"
ALL_REGION_KEY = "all_regions"
DEV_ENV_ID_VALUE = "developement"
TST_ENV_ID_VALUE = "test"
PRD_ENV_ID_VALUE = "production"
US_REGION_ID_VALUE = "usa"
EU_REGION_ID_VALUE = "eu"
JP_REGION_ID_VALUE = "japan"
US_REGION_VALUE = "us"
JP_REGION_VALUE = "jp"
EU_REGION_VALUE = "eu"
DATABRICKS_MAX_RETRY_ATTEMPTS = 3
DATABRICKS_UPDATE_MAX_RETRY_ATTEMPTS = 5
DATABRICKS_SLEEP_TIME = 20
DATABRICKS_SLEEP_TIME_SECONDS = 120
SERVICE_PRINCIPAL_TOKEN_LIFETIME_SECONDS_VALUE = -1
STATUS_KEY = "status"
SUCCESS_KEY = "success"
FAILED_KEY = "failed"
RESULT_KEY = "result"
DATABRICKS_APMS_ID_TAG_VALUE = "APMS-91706"
EXCLUDE_CATALOGS_LIST = ["system", "__databricks_internal", "databricks_internal"]
EXCLUDE_SCHEMA_LIST = ["system", "information_schema", "default"]
BACKEND_INFO_SCHEMATA_TABLE_NAME = "system.information_schema.schemata"
AIRFLOW_SERVICE_CREDENTIAL = "airflow-service-credential-<environment>"

BACKEND_METADATA_TABLE_COLUMNS = [
    "request_insert_dt",
    "apms_id",
    "edb_id",
    "project_name",
    "project_description",
    "project_justification",
    "requestor_email_id",
    "environment",
    "region",
    "sns_distribution_list",
    "data_classification",
    "business_unit",
    "technical_owner_email_id",
    "business_owner_email_id",
    "technical_owner_approval_status",
    "business_owner_approval_status",
    "technical_owner_comments",
    "business_owner_comments",
    "infra_provision_status",
    "request_creation_dt",
    "technical_owner_approval_dt",
    "business_owner_approval_dt",
    "request_end_dt",
    "kms_key_id",
    "request_type",
    "pipeline_id",
    "build_id",
    "bsn_id",
    "infra_requested"
]


BACKEND_IDMC_METADATA_TABLE_COLUMNS = [
    "id",
    "edb_id",
    "apms_id",
    "requestor_email_id",
    "connection_name",
    "connection_type",
    "connection_description",
    "environment",
    "region",
    "technical_owner_email",
    "business_owner_email",
    "request_type",
    "build_id",
    "pipeline_id",
    "status",
    "provision_start_dt"
]

APPLICATION = "application"
IDMC_KEY = "idmc"
IDMC_ADMIN_PREVELIGES = "admin_privileges"
IDMC_DETAILS = "idmc_details"
ORGANIZATION_ID = "organization_id"

IDMC_DATA_ENGINEER_ROLE = "data_engineer_role"
DATA_ENGINEER_PERMISSION = "data_engineer_permission"
IDMC_OPERATIONS_ROLE = "operations_role"
OPERATIONS_PERMISSION = "operations_permission"
IDMC_ADMIN_ROLE = "admin_role"
ADMIN_PERMISSION = "admin_permission"

IDMC_BASEURL_KEY = "idmc_base_url"
IDMC_LOGIN_URL_KEY = "idmc_login_url"
IDMC_CONN_URL_KEY = "idmc_conn_url"

SAML_ROLENAME = "idmc_saml_role_name"

DATA_ENGINEER = "data-engineer"
OPERATIONS = "ops-support"
ADMIN = "app-admins"

S3_SUBFOLDERS = "s3_sub_folders"
S3_BUCKET = "s3_bucket"

IDMC_HEADER = "idmc_headers"

DEV_ENV_CHK = ["dev", "development"]
TST_ENV_CHK = ["test", "qa", "tst"]
PRD_ENV_CHK = ["prod", "prd"]

KEY_ASSUME_ROLE = "assumerole"
ACCESS_KEY = "access_key"
SECRET_KEY = "secret_key"
SECURITY_TOKEN_KEY = "security_token"

DATABRICKS_VAULT_PATH = "ODPE/databricks/db_token"
DATABRICKS_VAULT_PATH_ENV = "ODPE/databricks/workspace_token_<region>"
TOKEN_KEY = "token"
NULL_VALUE = "null"
TRUE_VALUE = "true"
FALSE_VALUE = "false"

METADATA_STATUS_STARTED = "STARTED"
METADATA_STATUS_NOTSTARTED = "NOT STARTED"
METADATA_STATUS_PENDING = "PENDING"
METADATA_STATUS_FAILED = "FAILED"
METADATA_STATUS_SUCCESS = "COMPLETED"
METADATA_STATUS_REJECT = "REJECTED"
REQUEST_TYPE_KEY = "request_type"
REQUEST_TYPE_CREATE = "CREATE"
REQUEST_TYPE_MODIFY = "MODIFY"
REQUEST_APPROVAL_KEY = "APPROVE"
REQUEST_REQUEST_KEY = "REJECT"
WHERE_QUERY_KEY = "environment = '{environment}' and edb_id = '{edb_id}' and request_type = '{request_type}' and infra_provision_status = '{infra_provision_status}'"
SELECT_QUERY_KEY = "count(*)"
NONE_KEY = "None"
MODIFY_WHERE_QUERY_KEY = "environment = '{environment}' and edb_id = '{edb_id}' and request_type = 'MODIFY' and (infra_provision_status = 'PENDING' or infra_provision_status = 'STARTED')"
# LOGS
HARNESS_FOLDER_PATH = "/harness/logs/"
LOGS_ZIP_FILENAME = "logs.zip"
LOG_SERVICE_URL = "https://app.harness.io/gateway/log-service/blob/download"
HARNESS_SECRET_PATH = "ODPE/harness"
HARNESS_X_API_KEY = "x-api-key"

SECRET_MANAGER_KEY = "secretsmanager"
HARNESS_API_SECRET_NAME = "automation-eda-datalaunchpad-harness"

HARNESS_DEV_CREATE_PIPELINE_ID = "harness_dev_create_pipeline_id"
HARNESS_DEV_MODIFY_PIPELINE_ID = "harness_dev_modify_pipeline_id"
HARNESS_DEV_IDMC_CONNECTION_PIPELINE_ID = "harness_dev_idmc_connection_pipeline_id"
HARNESS_TST_CREATE_PIPELINE_ID = "harness_tst_create_pipeline_id"
HARNESS_TST_MODIFY_PIPELINE_ID = "harness_tst_modify_pipeline_id"
HARNESS_TST_IDMC_CONNECTION_PIPELINE_ID = "harness_tst_idmc_connection_pipeline_id"
HARNESS_PRD_CREATE_PIPELINE_ID = "harness_prd_create_pipeline_id"
HARNESS_PRD_MODIFY_PIPELINE_ID = "harness_prd_modify_pipeline_id"
HARNESS_PRD_IDMC_CONNECTION_PIPELINE_ID = "harness_prd_idmc_connection_pipeline_id"
HARNESS_DEV_PIPELINE_API_BASEURL = "harness_dev_pipeline_api_base_url"
HARNESS_TST_PIPELINE_API_BASEURL = "harness_tst_pipeline_api_base_url"
HARNESS_PRD_PIPELINE_API_BASEURL = "harness_prd_pipeline_api_base_url"

AWS_DBX_DATALAUNCHPAD_BUCKET_NAME = "tpc-aws-ted-<environment>-edpp-dbx-datalaunchpad-ent-us-east-1"
SCHEMA_LIST_FILE_KEY = "infra_dlp_schema_list/<environment>_<region>.json"
REQUEST_CONNECTION_TYPE = "connection_type"
REQUEST_CONNECTION_NAME = "connection_name"

OKTA_DOMAIN = "https://takeda.okta.com"
OKTA_AUDIENCE = "api://default"
OKTA_ISSUER = f"{OKTA_DOMAIN}/oauth2/default"
OKTA_JWKS_URL = f"{OKTA_ISSUER}/v1/keys"

TEC_ACCOUNT_KEY = "tec_account_id"