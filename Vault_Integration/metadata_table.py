from __future__ import annotations

import json
import sys
import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from databricks import sql

from utils import Constants
from utils.CommonUtils import get_config, read_secret, get_current_environment
from utils import CommonUtilsConstants
from utils import DLPLogSetup

_LOG = DLPLogSetup.get_logger()


def _make_sql_connection() -> sql.Connection:
    """Return a live Databricks SQL warehouse connection or raise RuntimeError."""
    try:
        env = get_current_environment()
        cfg = get_config()[CommonUtilsConstants.DATABRICKS_KEY][CommonUtilsConstants.ALL_ENVIRONMENT_KEY][CommonUtilsConstants.DATABRICKS_BACKEND_TABLE_AUTH_KEY]
        token = read_secret(CommonUtilsConstants.DATABRICKS_VAULT_PATH, env)[CommonUtilsConstants.TOKEN_KEY]

        connection = sql.connect(
            server_hostname=cfg[CommonUtilsConstants.DATABRICKS_URL_KEY],
            http_path=cfg[CommonUtilsConstants.DATABRICKS_SQLWH_HTTP_PATH_KEY],
            access_token=token,
        )
        _LOG.info("Databricks SQL connection established")
        return connection
    except Exception as exc:
        raise RuntimeError("Could not connect to Databricks SQL") from exc


class AirflowDeploymentMetadataManager:
    """Insert and update Airflow deployment metadata."""

    def _execute_query(
        self,
        query: str,
        params: list[Any] | tuple[Any, ...],
    ) -> None:
        """Execute insert/update query with parameters."""
        with _make_sql_connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()

    def _select_one(
        self,
        query: str,
        params: list[Any] | tuple[Any, ...],
    ) -> Optional[tuple[Any, ...]]:
        """Execute select query and return one row."""
        with _make_sql_connection() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def insert_initial_record(
        self,
        *,
        apms_id: str,
        edb_id: int,
        project_name: str,
        requestor_email: str,
        migrate_type: str,
        deployment_requested: str,
        region: str,
        tech_owner_email: str,
        biz_owner_email: str,
        build_id: str,
        validation: Dict[str, Any],
    ) -> None:
        """Create a fresh metadata row with validation results."""
        pipeline_blob = {
            "Validation_and_metadata_extraction": {
                "validation": validation,
                "status": Constants.EXEC_COMPLETED,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        }

        tech_status = Constants.APPROVAL_NOT_REQUIRED if migrate_type == Constants.MIGRATE_DEV_TO_TST else Constants.APPROVAL_PENDING
        biz_status = tech_status

        placeholders = ",".join(["?"] * 13) + ", CURRENT_TIMESTAMP()"
        sql_stmt = f"INSERT INTO {Constants.TABLE_NAME} ({Constants.INITIAL_RECORD_COLUMNS}) VALUES ({placeholders})"

        self._execute_query(
            sql_stmt,
            [
                apms_id,
                edb_id,
                project_name,
                requestor_email,
                migrate_type,
                deployment_requested,
                region,
                tech_owner_email,
                biz_owner_email,
                tech_status,
                biz_status,
                json.dumps(pipeline_blob),
                build_id,
            ],
        )
        _LOG.info("Initial record inserted for EDB %s build %s", edb_id, build_id)

    def append_execution_step(
        self,
        *,
        edb_id: int,
        build_id: str,
        step_key: str,
        step_payload: Dict[str, Any],
    ) -> None:
        """Add or update a JSON execution step."""
        select_sql = (
            f"SELECT {Constants.PIPELINE_RESULTS_SELECT_COLUMNS} FROM {Constants.TABLE_NAME} "
            "WHERE edb_id = ? AND build_id = ? "
            "ORDER BY request_insert_dt DESC LIMIT 1"
        )
        row = self._select_one(select_sql, [edb_id, build_id])
        if not row:
            raise ValueError(f"No metadata for EDB {edb_id} build {build_id}")

        blob = json.loads(row[0]) if row[0] else {}
        blob[step_key] = {**step_payload, "timestamp": datetime.now(timezone.utc).isoformat()}

        update_sql = f"UPDATE {Constants.TABLE_NAME} SET pipeline_execution_results = ? WHERE edb_id = ? AND build_id = ?"
        self._execute_query(update_sql, [json.dumps(blob), edb_id, build_id])
        _LOG.info("Execution step '%s' added to EDB %s build %s", step_key, edb_id, build_id)

    def update_approvals(
        self,
        *,
        edb_id: int,
        build_id: str,
        tech_status: Optional[str] = None,
        biz_status: Optional[str] = None,
    ) -> None:
        """Update approval columns and stamp approval_date."""
        if not tech_status and not biz_status:
            _LOG.warning("No approval status supplied; nothing to update")
            return

        sets, params = [], []
        if tech_status:
            sets.append(f"{Constants.APPROVAL_UPDATE_COLUMNS[0]} = ?")
            params.append(tech_status)
        if biz_status:
            sets.append(f"{Constants.APPROVAL_UPDATE_COLUMNS[1]} = ?")
            params.append(biz_status)

        sets.append(f"{Constants.APPROVAL_UPDATE_COLUMNS[2]} = CURRENT_TIMESTAMP()")
        params.extend([edb_id, build_id])

        upd_sql = f"UPDATE {Constants.TABLE_NAME} SET {', '.join(sets)} WHERE edb_id = ? AND build_id = ?"
        self._execute_query(upd_sql, params)
        _LOG.info("Approval status updated for EDB %s build %s", edb_id, build_id)