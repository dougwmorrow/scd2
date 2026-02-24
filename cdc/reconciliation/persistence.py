"""OBS-6: Reconciliation result persistence to General.ops.ReconciliationLog."""

from __future__ import annotations

import logging

import connections

logger = logging.getLogger(__name__)


def ensure_reconciliation_log_table() -> None:
    """OBS-6: Create General.ops.ReconciliationLog if it doesn't exist.

    Call once at pipeline startup or before the first reconciliation run.
    Idempotent — safe to call multiple times.
    """
    ddl = """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'ops' AND TABLE_NAME = 'ReconciliationLog'
    )
    CREATE TABLE ops.ReconciliationLog (
        Id          INT IDENTITY(1,1) PRIMARY KEY,
        TableName   NVARCHAR(256)  NOT NULL,
        SourceName  NVARCHAR(128)  NOT NULL,
        CheckType   NVARCHAR(64)   NOT NULL,
        RunDate     DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
        Status      NVARCHAR(16)   NOT NULL,
        SourceRows  INT            NULL,
        TargetRows  INT            NULL,
        MismatchedRows INT         NULL,
        SourceOnlyRows INT         NULL,
        TargetOnlyRows INT         NULL,
        Metadata    NVARCHAR(MAX)  NULL,
        Errors      NVARCHAR(MAX)  NULL
    )
    """
    try:
        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(ddl)
            cursor.close()
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.debug("OBS-6: Could not ensure ReconciliationLog table — non-fatal", exc_info=True)


def _persist_reconciliation_result(
    check_type: str,
    table_name: str,
    source_name: str,
    is_clean: bool,
    source_rows: int = 0,
    target_rows: int = 0,
    mismatched_rows: int = 0,
    source_only_rows: int = 0,
    target_only_rows: int = 0,
    metadata: dict | None = None,
    errors: list[str] | None = None,
) -> None:
    """OBS-6: Write a reconciliation result to General.ops.ReconciliationLog.

    Called by each public reconciliation function to enable historical trending
    and automated alerting on reconciliation drift.
    """
    import json
    from datetime import datetime, timezone

    status = "PASSED" if is_clean else "FAILED"
    if errors:
        status = "ERROR" if not is_clean else "PASSED"
    meta_json = json.dumps(metadata) if metadata else None
    err_json = json.dumps(errors) if errors else None

    try:
        conn = connections.get_general_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO ops.ReconciliationLog "
                "(TableName, SourceName, CheckType, RunDate, Status, "
                "SourceRows, TargetRows, MismatchedRows, SourceOnlyRows, "
                "TargetOnlyRows, Metadata, Errors) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                table_name, source_name, check_type,
                datetime.now(timezone.utc), status,
                source_rows, target_rows, mismatched_rows,
                source_only_rows, target_only_rows,
                meta_json, err_json,
            )
            cursor.close()
            conn.commit()
        finally:
            conn.close()
    except Exception:
        logger.debug(
            "OBS-6: Failed to persist %s result for %s.%s — non-fatal",
            check_type, source_name, table_name, exc_info=True,
        )
