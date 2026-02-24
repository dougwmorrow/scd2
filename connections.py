"""SQL Server target database connections (UDM_Stage, UDM_Bronze, General).

Provides both pyodbc connections (for DDL/DML) and ConnectorX URIs (for reads).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from urllib.parse import quote_plus

import pyodbc

import config

logger = logging.getLogger(__name__)


def _pyodbc_connection_string(database: str) -> str:
    return (
        f"DRIVER={{{config.ODBC_DRIVER}}};"
        f"SERVER={config.SQL_SERVER_HOST},{config.SQL_SERVER_PORT};"
        f"DATABASE={database};"
        f"UID={config.SQL_SERVER_USER};"
        f"PWD={config.SQL_SERVER_PASSWORD};"
        "TrustServerCertificate=yes;"
    )


def _connectorx_uri(database: str) -> str:
    usr = quote_plus(config.SQL_SERVER_USER)
    pwd = quote_plus(config.SQL_SERVER_PASSWORD)
    return (
        f"mssql://{usr}:{pwd}@{config.SQL_SERVER_HOST}:{config.SQL_SERVER_PORT}"
        f"/{database}?TrustServerCertificate=true"
    )


# --- pyodbc Connections ---

def get_stage_connection() -> pyodbc.Connection:
    return pyodbc.connect(_pyodbc_connection_string(config.STAGE_DB), autocommit=True)


def get_bronze_connection() -> pyodbc.Connection:
    return pyodbc.connect(_pyodbc_connection_string(config.BRONZE_DB), autocommit=True)


def get_general_connection() -> pyodbc.Connection:
    return pyodbc.connect(_pyodbc_connection_string(config.GENERAL_DB), autocommit=True)


def get_connection(database: str) -> pyodbc.Connection:
    return pyodbc.connect(_pyodbc_connection_string(database), autocommit=True)


# --- ConnectorX URIs ---

def stage_connectorx_uri() -> str:
    return _connectorx_uri(config.STAGE_DB)


def bronze_connectorx_uri() -> str:
    return _connectorx_uri(config.BRONZE_DB)


def general_connectorx_uri() -> str:
    return _connectorx_uri(config.GENERAL_DB)


# --- Context Managers ---

@contextmanager
def cursor_for(database: str):
    """X-1: Context manager yielding a cursor for the given database.

    Replaces the verbose ``conn = get_connection(); try: ... finally: conn.close()``
    pattern with a concise ``with cursor_for(db) as cur:`` block.

    Usage::

        with cursor_for("General") as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
    """
    conn = get_connection(database)
    try:
        cursor = conn.cursor()
        yield cursor
        cursor.close()
    finally:
        conn.close()


# --- Startup Validation ---

def verify_rcsi_enabled() -> None:
    """E-9: Verify READ_COMMITTED_SNAPSHOT is enabled on Bronze database.

    Without RCSI, the SCD2 batched UPDATE TOP(N) triggers lock escalation
    to a table-level exclusive lock once N exceeds ~5,000 rows, blocking all
    readers for the duration of each batch. Current batch size is controlled by
    config.SCD2_UPDATE_BATCH_SIZE (default 4,000 — deliberately below the
    escalation threshold). With RCSI, readers see pre-update snapshots and
    are not blocked.

    Logs WARNING if RCSI is not enabled — the pipeline continues but SCD2
    UPDATEs will block concurrent readers.
    """
    try:
        with cursor_for(config.BRONZE_DB) as cur:
            cur.execute(
                "SELECT is_read_committed_snapshot_on "
                "FROM sys.databases WHERE name = ?",
                config.BRONZE_DB,
            )
            row = cur.fetchone()

            if row is None:
                logger.warning(
                    "E-9: Could not find database %s in sys.databases — "
                    "unable to verify RCSI status",
                    config.BRONZE_DB,
                )
            elif not row[0]:
                logger.warning(
                    "E-9: READ_COMMITTED_SNAPSHOT is NOT enabled on %s. "
                    "SCD2 batch UPDATEs (TOP %d) will escalate to table-level "
                    "exclusive locks, blocking all readers. Enable with: "
                    "ALTER DATABASE [%s] SET READ_COMMITTED_SNAPSHOT ON",
                    config.BRONZE_DB, config.SCD2_UPDATE_BATCH_SIZE,
                    config.BRONZE_DB,
                )
            else:
                logger.info("E-9: RCSI verified enabled on %s", config.BRONZE_DB)
    except Exception:
        logger.warning(
            "E-9: Could not verify RCSI on %s — continuing without verification",
            config.BRONZE_DB, exc_info=True,
        )
