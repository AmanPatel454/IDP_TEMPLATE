"""
Project-wide constants for the Airflow CI/CD metadata manager.
"""
from typing import List

# ============================================================================
# Database
# ============================================================================
TABLE_NAME: str = "usdev_eda.ent_us_raw_dlp.idmc_cicd_deploy_metadata"

TABLE_COLUMNS: List[str] = [
    "apms_id", "request_insert_dt", "edb_id", "project_name",
    "requestor_email_id", "migrate_type", "deployment_requested", "region",
    "technical_owner_email_id", "business_owner_email_id",
    "technical_owner_approval_status", "business_owner_approval_status",
    "approval_date", "pipeline_execution_results", "build_id",
]

# ============================================================================
# SQL Column Sets for Different Operations
# ============================================================================
INITIAL_RECORD_COLUMNS: str = (
    "apms_id, edb_id, project_name, requestor_email_id, migrate_type, "
    "deployment_requested, region, technical_owner_email_id, "
    "business_owner_email_id, technical_owner_approval_status, "
    "business_owner_approval_status, pipeline_execution_results, "
    "build_id, request_insert_dt"
)

PIPELINE_RESULTS_SELECT_COLUMNS: str = "pipeline_execution_results"

APPROVAL_UPDATE_COLUMNS: List[str] = [
    "technical_owner_approval_status",
    "business_owner_approval_status", 
    "approval_date"
]

# ============================================================================
# Approval Status Constants
# ============================================================================
APPROVAL_NOT_REQUIRED = "not_required"
APPROVAL_PENDING      = "pending"
APPROVAL_APPROVED     = "approved"
APPROVAL_REJECTED     = "rejected"

# ============================================================================
# Migration Type Constants
# ============================================================================
MIGRATE_DEV_TO_TST    = "dev_to_tst"
MIGRATE_TST_TO_PROD   = "tst_to_prod"

# ============================================================================
# Execution State Constants
# ============================================================================
EXEC_COMPLETED        = "completed"
EXEC_FAILED           = "failed"
EXEC_RUNNING          = "running"

# ============================================================================
# Step Names
# ============================================================================
STEP_INITIAL_VALIDATION = "Validation_and_metadata_extraction"